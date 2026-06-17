import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from functools import partial
from itertools import zip_longest
from typing import AsyncGenerator, Callable, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import TypeHistogram
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
from nodestream.pipeline import Extractor
from nodestream.schema import Schema

from .extractor import Neo4jExtractor
from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

LAST_INGESTED_AT_PROPERTY = "last_ingested_at"

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

FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY elementId(n) SKIP $shard_offset LIMIT $shard_limit
RETURN n
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
# Extractor base classes
# ---------------------------------------------------------------------------


class Neo4jMappingExtractor(Extractor):
    """Paginates a Neo4j query and maps each record."""

    def __init__(self, inner: Neo4jExtractor, mapRecord: Callable) -> None:
        self.inner = inner
        self.mapRecord = mapRecord

    async def extract_records(self) -> AsyncGenerator:
        async for record in self.inner.extract_records():
            yield self.mapRecord(record)


class Neo4jShardExtractor(Extractor):
    """Executes a single pre-bounded shard query and maps each record."""

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        mapRecord: Callable,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.mapRecord = mapRecord

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            yield self.mapRecord(record)


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
        limit: int = 1000,
        *,
        node_types: List[str] | None = None,
        relationship_types: List[str] | None = None,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
        node_only: bool = False,
        shard_size: int | None = None,
        distribution: str = DEFAULT_DISTRIBUTION,
        max_shards_per_type: int = 10000,
    ) -> None:
        super().__init__(schema=schema)
        self.database_connection = database_connection
        self.limit = limit
        self.node_types = (
            node_types if node_types is not None else [n.name for n in schema.nodes]
        )
        self.relationship_types = (
            relationship_types
            if relationship_types is not None
            else [r.name for r in schema.relationships]
        )
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.node_only = node_only
        self.shard_size = shard_size
        self.max_shards_per_type = max_shards_per_type
        self.distributionStrategy: DistributionStrategy = DISTRIBUTION_STRATEGIES.get(
            distribution, SequentialDistribution
        )()

    # -- fetch_extractors --------------------------------------------------------

    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        if self.node_only:
            async for extractor in self.fetchNodeExtractors():
                yield extractor
        else:
            async for extractor in self.fetchRelationshipExtractors():
                yield extractor

    async def fetchNodeExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.snapshotCutoff()
        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(
                    self.preview_node_count(nodeType, cutoff=cutoff)
                    for nodeType in self.node_types
                )
            )
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
                for nodeType, count in zip(self.node_types, counts)
                if count > 0
            ]
        else:
            extractorsByType = [
                [self.buildNodeExtractor(nodeType, schema=schema, cutoff=cutoff)]
                for nodeType in self.node_types
            ]
        async for extractor in self.distributionStrategy.distribute(extractorsByType):
            yield extractor

    async def fetchRelationshipExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.snapshotCutoff()

        # Build (relationshipType, adjacencies) pairs, skipping types with no adjacencies.
        relationshipTypeAdjacencyPairs = [
            (
                relationshipType,
                list(schema.get_adjacencies_by_relationship_type(relationshipType)),
            )
            for relationshipType in self.relationship_types
        ]
        relationshipTypeAdjacencyPairs = [
            (relationshipType, adjacencies)
            for relationshipType, adjacencies in relationshipTypeAdjacencyPairs
            if adjacencies
        ]

        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(
                    self.preview_relationship_count(relationshipType, cutoff=cutoff)
                    for relationshipType, _ in relationshipTypeAdjacencyPairs
                )
            )
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
                        count, self.shard_size
                    )
                    for adjacency in adjacencies
                ]
                for (relationshipType, adjacencies), count in zip(
                    relationshipTypeAdjacencyPairs, counts
                )
                if count > 0
            ]
        else:
            extractorsByType = [
                [
                    self.buildRelationshipExtractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relationshipType,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for adjacency in adjacencies
                ]
                for relationshipType, adjacencies in relationshipTypeAdjacencyPairs
            ]
        async for extractor in self.distributionStrategy.distribute(extractorsByType):
            yield extractor

    # -- Mapping helpers --------------------------------------------------------

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, node_type: str, schema: Schema | None = None
    ) -> Node:
        properties = PropertySet(node)
        key_values = PropertySet.empty()
        if schema is not None:
            nodeSchema = schema.get_node_type_by_name(node_type)
            if nodeSchema is not None:
                for keyName in nodeSchema.keys:
                    if keyName in properties:
                        key_values[keyName] = properties.pop(keyName)
        additionalTypes: Tuple[str, ...] = tuple(
            label for label in node.labels if label != node_type
        )
        return Node(
            type=node_type,
            properties=properties,
            key_values=key_values,
            additional_types=additionalTypes,
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship, relationship_type: str
    ) -> Relationship:
        return Relationship(
            type=relationship_type,
            properties=PropertySet(relationship),
        )

    # -- Record mapper helpers --------------------------------------------------

    def mapNodeRecord(self, record, nodeType: str, schema, fromShard: bool = False):
        rawNode = record["n"] if fromShard else record.original["n"]
        return self.map_neo4j_node_to_nodestream_node(
            rawNode, node_type=nodeType, schema=schema
        ).into_ingest()

    def mapRelationshipRecord(
        self,
        record,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        schema,
        fromShard: bool = False,
    ):
        rawRecord = record if fromShard else record.original
        return RelationshipWithNodes(
            from_node=self.map_neo4j_node_to_nodestream_node(
                rawRecord["a"], node_type=fromNodeType, schema=schema
            ),
            to_node=self.map_neo4j_node_to_nodestream_node(
                rawRecord["b"], node_type=toNodeType, schema=schema
            ),
            relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                rawRecord["r"], relationship_type=relationshipType
            ),
        ).into_ingest()

    # -- Shard params helper ----------------------------------------------------

    def buildShardParameters(self, cutoff, shardOffset: int, shardLimit: int) -> dict:
        return dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shardOffset,
            shard_limit=shardLimit,
        )

    # -- Extractor builders -----------------------------------------------------

    def buildNodeExtractor(
        self,
        nodeType: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        whereClause = self.build_where_clause("n")
        inner = Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(
                type=nodeType, where=whereClause
            ),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            partial(self.mapNodeRecord, nodeType=nodeType, schema=schema),
        )

    def buildNodeShardExtractor(
        self,
        nodeType: str,
        keyField: Optional[str],
        shardOffset: int,
        shardLimit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        whereClause = self.build_where_clause("n")
        if keyField:
            statement = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=nodeType, where=whereClause, key_field=keyField
            )
        else:
            statement = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=nodeType, where=whereClause
            )
        params = self.buildShardParameters(cutoff, shardOffset, shardLimit)
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            partial(
                self.mapNodeRecord, nodeType=nodeType, schema=schema, fromShard=True
            ),
        )

    def buildRelationshipExtractor(
        self,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        whereClause = self.build_where_clause("r", sample=True)
        inner = Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=whereClause,
            ),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            partial(
                self.mapRelationshipRecord,
                fromNodeType=fromNodeType,
                toNodeType=toNodeType,
                relationshipType=relationshipType,
                schema=schema,
            ),
        )

    def buildRelationshipShardExtractor(
        self,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        keyField: Optional[str],
        shardOffset: int,
        shardLimit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
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
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            partial(
                self.mapRelationshipRecord,
                fromNodeType=fromNodeType,
                toNodeType=toNodeType,
                relationshipType=relationshipType,
                schema=schema,
                fromShard=True,
            ),
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

    def build_filter_parameters(
        self, cutoff: datetime | None = None
    ) -> dict[str, object]:
        if self.latest_hours is None:
            return {}
        if cutoff is None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
        return {"cutoff": cutoff}

    # -- Count helpers ----------------------------------------------------------

    async def preview_node_count(
        self, node_type: str, cutoff: datetime | None = None
    ) -> int:
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
        self, relationship_type: str, cutoff: datetime | None = None
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

    async def gatherCounts(self, types, countFunction, cutoff) -> dict:
        counts = await asyncio.gather(
            *(countFunction(typeName, cutoff=cutoff) for typeName in types)
        )
        return dict(zip(types, counts))

    async def build_histogram(self) -> TypeHistogram:
        cutoff = self.snapshotCutoff()
        nodeCounts = await self.gatherCounts(
            self.node_types, self.preview_node_count, cutoff
        )
        relationshipCounts = {}
        if not self.node_only:
            relationshipCounts = await self.gatherCounts(
                self.relationship_types, self.preview_relationship_count, cutoff
            )
        return TypeHistogram(
            node_counts=nodeCounts, relationship_counts=relationshipCounts
        )

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

    def snapshotCutoff(self) -> datetime | None:
        if self.latest_hours is None:
            return None
        return datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)

    def key_field_for_node_type(self, node_type: str, schema: Schema) -> Optional[str]:
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        nodeSchema = schema.get_node_type_by_name(node_type)
        if nodeSchema and nodeSchema.keys:
            return next(iter(nodeSchema.keys))
        return None

    def key_field_for_relationship_type(
        self, relationship_type: str, schema: Schema
    ) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None
