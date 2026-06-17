import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from logging import getLogger
from typing import AsyncGenerator, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import TypeHistogram
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
from nodestream.pipeline import Extractor
from nodestream.schema import Schema

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

LAST_INGESTED_AT_PROPERTY = "last_ingested_at"

logger = getLogger(__name__)

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """\
MATCH (n:{type})
{where}RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}RETURN a, r, b SKIP $offset LIMIT $limit
"""

FETCH_NODES_SHARD_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY n.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN n
"""

FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY r.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b
"""


FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY elementId(r) SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b
"""


COUNT_NODES_BY_TYPE_QUERY_FORMAT = """\
MATCH (n:{type})
{where}RETURN count(n) AS count
"""

COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """\
MATCH ()-[r:{relationship_type}]->()
{where}RETURN count(r) AS count
"""


# ---------------------------------------------------------------------------
# Mapping helpers (module-level, no retriever state needed)
# ---------------------------------------------------------------------------


def map_neo4j_node_to_nodestream_node(
    node: Neo4jNode, nodeType: str, schema: Schema
) -> Node | None:
    if not schema.has_node_of_type(nodeType):
        logger.warning("Node type %r not found in schema — skipping record", nodeType)
        return None
    nodeSchema = schema.get_node_type_by_name(nodeType)
    properties = PropertySet(node)
    keyValues = PropertySet.empty()
    for keyName in nodeSchema.keys:
        if keyName in properties:
            keyValues[keyName] = properties.pop(keyName)
    additionalTypes: Tuple[str, ...] = tuple(
        label for label in node.labels if label != nodeType
    )
    return Node(
        type=nodeType,
        properties=properties,
        key_values=keyValues,
        additional_types=additionalTypes,
    )


def map_neo4j_relationship_to_nodestream_relationship(
    relationship: Neo4jRelationship, relationshipType: str
) -> Relationship:
    return Relationship(
        type=relationshipType,
        properties=PropertySet(relationship),
    )


# ---------------------------------------------------------------------------
# Typed extractor subclasses — shard-first design
# ---------------------------------------------------------------------------


class Neo4jNodeExtractor(Extractor):
    """Executes a single pre-bounded node shard query and maps each record.

    The caller is responsible for supplying the Cypher statement and a params
    dict that includes ``shard_offset`` and ``shard_limit`` (plus any filter
    parameters such as ``cutoff``).  Records whose node type is absent from the
    schema are silently skipped with a warning.
    """

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        nodeType: str,
        schema: Schema,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.nodeType = nodeType
        self.schema = schema

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            node = map_neo4j_node_to_nodestream_node(
                record["n"], nodeType=self.nodeType, schema=self.schema
            )
            if node is not None:
                yield node.into_ingest()


class Neo4jRelationshipExtractor(Extractor):
    """Executes a single pre-bounded relationship shard query and maps each record.

    The caller is responsible for supplying the Cypher statement and a params
    dict that includes ``shard_offset`` and ``shard_limit`` (plus any filter
    parameters such as ``cutoff``).  Records where either endpoint node type is
    absent from the schema are silently skipped with a warning.
    """

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        schema: Schema,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.fromNodeType = fromNodeType
        self.toNodeType = toNodeType
        self.relationshipType = relationshipType
        self.schema = schema

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            fromNode = map_neo4j_node_to_nodestream_node(
                record["a"], nodeType=self.fromNodeType, schema=self.schema
            )
            toNode = map_neo4j_node_to_nodestream_node(
                record["b"], nodeType=self.toNodeType, schema=self.schema
            )
            if fromNode is not None and toNode is not None:
                yield RelationshipWithNodes(
                    from_node=fromNode,
                    to_node=toNode,
                    relationship=map_neo4j_relationship_to_nodestream_relationship(
                        record["r"], relationshipType=self.relationshipType
                    ),
                ).into_ingest()


# ---------------------------------------------------------------------------
# Distribution strategies
# ---------------------------------------------------------------------------


class DistributionStrategy(ABC):
    """Controls the order in which per-type extractor lists are interleaved."""

    @abstractmethod
    def distribute(
        self, extractorsByType: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        ...  # pragma: no cover


class SequentialDistribution(DistributionStrategy):
    """Drain all shards of one type before moving to the next."""

    async def distribute(
        self, extractorsByType: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        for extractors in extractorsByType:
            for extractor in extractors:
                yield extractor


class RoundRobinDistribution(DistributionStrategy):
    """Yield one shard per type in rotation, so all types make progress together."""

    async def distribute(
        self, extractorsByType: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        sentinel = object()
        for column in zip_longest(*extractorsByType, fillvalue=sentinel):
            for extractor in column:
                if extractor is not sentinel:
                    yield extractor


DISTRIBUTION_STRATEGIES = {
    "sequential": SequentialDistribution,
    "round_robin": RoundRobinDistribution,
}
DEFAULT_DISTRIBUTION = "sequential"


# ---------------------------------------------------------------------------
# Retriever
# ---------------------------------------------------------------------------


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        schema: Schema,
        shard_size: int,
        *,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
        preload_nodes: bool = False,
        distribution: str = DEFAULT_DISTRIBUTION,
    ) -> None:
        super().__init__(schema=schema)
        self.database_connection = database_connection
        self.shard_size = shard_size
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.preload_nodes = preload_nodes
        self.histogram: TypeHistogram = TypeHistogram()
        self.cutoff: datetime = datetime.now(timezone.utc)
        self.distributionStrategy: DistributionStrategy = DISTRIBUTION_STRATEGIES.get(
            distribution, SequentialDistribution
        )()

    # -- fetch_extractors --------------------------------------------------------

    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        if self.preload_nodes:
            async for extractor in self.fetchNodeExtractors():
                yield extractor
        async for extractor in self.fetchRelationshipExtractors():
            yield extractor

    async def fetchNodeExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.cutoff
        nodeCounts = self.histogram.node_counts
        extractorsByType = [
            [
                self.buildNodeShardExtractor(
                    nodeType,
                    self.key_field_for_node_type(nodeType, schema),
                    shardOffset,
                    shardLimit,
                    schema=schema,
                    cutoff=cutoff,
                )
                for shardOffset, shardLimit in self.computeShards(
                    count, self.shard_size
                )
            ]
            for nodeType, count in nodeCounts.items()
            if count > 0
        ]
        async for extractor in self.distributionStrategy.distribute(extractorsByType):
            yield extractor

    async def fetchRelationshipExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.cutoff
        relationshipCounts = self.histogram.relationship_counts

        # Build (relationshipType, adjacencies) pairs, skipping types with no adjacencies.
        relationshipTypeAdjacencyPairs = [
            (
                relationshipType,
                list(schema.get_adjacencies_by_relationship_type(relationshipType)),
            )
            for relationshipType in relationshipCounts
        ]
        relationshipTypeAdjacencyPairs = [
            (relationshipType, adjacencies)
            for relationshipType, adjacencies in relationshipTypeAdjacencyPairs
            if adjacencies
        ]

        extractorsByType = [
            [
                self.buildRelationshipShardExtractor(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    relationshipType,
                    self.key_field_for_relationship_type(relationshipType, schema),
                    shardOffset,
                    shardLimit,
                    schema=schema,
                    cutoff=cutoff,
                )
                for shardOffset, shardLimit in self.computeShards(
                    relationshipCounts[relationshipType], self.shard_size
                )
                for adjacency in adjacencies
            ]
            for relationshipType, adjacencies in relationshipTypeAdjacencyPairs
            if relationshipCounts[relationshipType] > 0
        ]
        async for extractor in self.distributionStrategy.distribute(extractorsByType):
            yield extractor

    # -- Shard params helper ----------------------------------------------------

    def buildShardParameters(
        self, cutoff: datetime, shardOffset: int, shardLimit: int
    ) -> dict:
        return dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shardOffset,
            shard_limit=shardLimit,
        )

    # -- Extractor builders -----------------------------------------------------

    def buildNodeShardExtractor(
        self,
        nodeType: str,
        keyField: str,
        shardOffset: int,
        shardLimit: int,
        schema: Schema,
        cutoff: datetime,
    ) -> Neo4jNodeExtractor:
        whereClause = self.build_where_clause("n")
        statement = FETCH_NODES_SHARD_QUERY_FORMAT.format(
            type=nodeType, where=whereClause, key_field=keyField
        )
        params = self.buildShardParameters(cutoff, shardOffset, shardLimit)
        return Neo4jNodeExtractor(
            self.database_connection,
            statement,
            params,
            nodeType=nodeType,
            schema=schema,
        )

    def buildRelationshipShardExtractor(
        self,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        keyField: Optional[str],
        shardOffset: int,
        shardLimit: int,
        schema: Schema,
        cutoff: datetime,
    ) -> Neo4jRelationshipExtractor:
        whereClause = self.build_where_clause("r", sample=True)
        if keyField:
            statement = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=whereClause,
                key_field=keyField,
            )
        else:
            statement = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=whereClause,
            )
        params = self.buildShardParameters(cutoff, shardOffset, shardLimit)
        return Neo4jRelationshipExtractor(
            self.database_connection,
            statement,
            params,
            fromNodeType=fromNodeType,
            toNodeType=toNodeType,
            relationshipType=relationshipType,
            schema=schema,
        )

    # -- Query builders ---------------------------------------------------------

    def build_where_clause(self, cypherVariable: str, *, sample: bool = False) -> str:
        clauses: list[str] = []
        if sample and self.sample_ratio:
            clauses.append(
                f"toInteger(split(elementId({cypherVariable}), ':')[-1]) % {self.sample_ratio} = 0"
            )
        if self.latest_hours is not None:
            clauses.append(f"{cypherVariable}.`{LAST_INGESTED_AT_PROPERTY}` >= $cutoff")
        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses) + "\n"

    def build_filter_parameters(self, cutoff: datetime) -> dict[str, object]:
        return {"cutoff": cutoff}

    # -- Count helpers ----------------------------------------------------------

    async def preview_node_count(self, node_type: str, cutoff: datetime) -> int:
        whereClause = self.build_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(
            type=node_type, where=whereClause
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        firstResult = next(iter(results), None)
        return int(firstResult["count"]) if firstResult is not None else 0

    async def preview_relationship_count(
        self, relationship_type: str, cutoff: datetime
    ) -> int:
        whereClause = self.build_where_clause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=whereClause
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        firstResult = next(iter(results), None)
        return int(firstResult["count"]) if firstResult is not None else 0

    async def gatherCounts(self, types, countFunction, cutoff: datetime) -> dict:
        counts = await asyncio.gather(
            *(countFunction(typeName, cutoff=cutoff) for typeName in types)
        )
        return dict(zip(types, counts))

    async def build_histogram(self) -> TypeHistogram:
        self.cutoff = self.snapshotCutoff()
        nodeTypes = [nodeType.name for nodeType in self.schema.nodes]
        relationshipTypes = [
            relationshipType.name for relationshipType in self.schema.relationships
        ]
        nodeCounts = await self.gatherCounts(
            nodeTypes, self.preview_node_count, self.cutoff
        )
        relationshipCounts = await self.gatherCounts(
            relationshipTypes, self.preview_relationship_count, self.cutoff
        )
        self.histogram = TypeHistogram(
            node_counts=nodeCounts, relationship_counts=relationshipCounts
        )
        return self.histogram

    def computeShards(self, totalCount: int, shardSize: int) -> List[Tuple[int, int]]:
        if totalCount <= 0 or shardSize <= 0:
            return []
        numberOfShards = math.ceil(totalCount / shardSize)
        return [
            (
                shardIndex * shardSize,
                min(shardSize, totalCount - shardIndex * shardSize),
            )
            for shardIndex in range(numberOfShards)
        ]

    def snapshotCutoff(self) -> datetime:
        """Return the snapshot upper bound for this run.

        When ``latest_hours`` is set the cutoff is pushed back by that many
        hours so only recently-ingested records are included.  When it is not
        set the cutoff is simply *now*, acting as a guaranteed upper-bound
        anchor so no records ingested after the run started can slip in.
        """
        now = datetime.now(timezone.utc)
        if self.latest_hours is not None:
            return now - timedelta(hours=self.latest_hours)
        return now

    def key_field_for_node_type(self, node_type: str, schema: Schema) -> str:
        """Return the field to ORDER BY when paginating nodes.

        Priority:
        1. ``last_ingested_at`` when recency filtering is active — keeps ordering
           consistent with the WHERE clause filter.
        2. The first declared schema key for the node type — stable, meaningful
           order derived from the data model.
        3. ``last_ingested_at`` as a universal fallback — every ingested node
           carries this property, so it is always safe to order by.
        """
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        nodeSchema = schema.get_node_type_by_name(node_type)
        if nodeSchema and nodeSchema.keys:
            return next(iter(nodeSchema.keys))
        return LAST_INGESTED_AT_PROPERTY

    def key_field_for_relationship_type(
        self, relationship_type: str, schema: Schema
    ) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None
