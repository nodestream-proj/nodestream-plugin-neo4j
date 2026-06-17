import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
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

    # -- fetchExtractors --------------------------------------------------------

    async def fetchExtractors(self) -> AsyncGenerator[Extractor, None]:
        if self.node_only:
            async for extractor in self._fetchNodeExtractors():
                yield extractor
        else:
            async for extractor in self._fetchRelExtractors():
                yield extractor

    async def _fetchNodeExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.snapshotCutoff()
        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(self.preview_node_count(t, cutoff=cutoff) for t in self.node_types)
            )
            extractorsByType = [
                [
                    self.buildNodeShardExtractor(
                        nodeType,
                        self.key_field_for_node_type(nodeType, schema),
                        offset,
                        limit,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for offset, limit in self.compute_shards(count, self.shard_size)
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

    async def _fetchRelExtractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self.snapshotCutoff()

        # Build (relType, adjacencies) pairs, skipping types with no adjacencies.
        relTypeAdj = [
            (relType, list(schema.get_adjacencies_by_relationship_type(relType)))
            for relType in self.relationship_types
        ]
        relTypeAdj = [(rt, adjs) for rt, adjs in relTypeAdj if adjs]

        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(
                    self.preview_relationship_count(rt, cutoff=cutoff)
                    for rt, _ in relTypeAdj
                )
            )
            extractorsByType = [
                [
                    self.buildRelShardExtractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relType,
                        self.key_field_for_relationship_type(relType, schema),
                        offset,
                        limit,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for offset, limit in self.compute_shards(count, self.shard_size)
                    for adjacency in adjacencies
                ]
                for (relType, adjacencies), count in zip(relTypeAdj, counts)
                if count > 0
            ]
        else:
            extractorsByType = [
                [
                    self.buildRelExtractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relType,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for adjacency in adjacencies
                ]
                for relType, adjacencies in relTypeAdj
            ]
        async for extractor in self.distributionStrategy.distribute(extractorsByType):
            yield extractor

    # -- Mapping helpers --------------------------------------------------------

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, node_type: str, schema: Schema | None = None
    ) -> Node:
        properties = PropertySet(node)
        keyValues = PropertySet.empty()
        if schema is not None:
            nodeSchema = schema.get_node_type_by_name(node_type)
            if nodeSchema is not None:
                for key in nodeSchema.keys:
                    if key in properties:
                        keyValues[key] = properties.pop(key)
        additionalTypes: Tuple[str, ...] = tuple(
            label for label in node.labels if label != node_type
        )
        return Node(
            type=node_type,
            properties=properties,
            key_values=keyValues,
            additional_types=additionalTypes,
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship, relationship_type: str
    ) -> Relationship:
        return Relationship(
            type=relationship_type,
            properties=PropertySet(relationship),
        )

    # -- Extractor builders -----------------------------------------------------

    def buildNodeExtractor(
        self,
        node_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("n")
        inner = Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            lambda record, nt=node_type: self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=nt, schema=schema
            ).into_ingest(),
        )

    def buildNodeShardExtractor(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("n")
        if key_field:
            statement = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=node_type, where=where, key_field=key_field
            )
        else:
            statement = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=node_type, where=where
            )
        params = dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            lambda record, nt=node_type: self.map_neo4j_node_to_nodestream_node(
                record["n"], node_type=nt, schema=schema
            ).into_ingest(),
        )

    def buildRelExtractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("r", sample=True)
        inner = Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
            ),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            lambda record, fnt=from_node_type, tnt=to_node_type, rt=relationship_type: RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=fnt, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=tnt, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record.original["r"], relationship_type=rt
                ),
            ).into_ingest(),
        )

    def buildRelShardExtractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("r", sample=True)
        if key_field:
            statement = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
                key_field=key_field,
            )
        else:
            statement = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
            )
        params = dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            lambda record, fnt=from_node_type, tnt=to_node_type, rt=relationship_type: RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record["a"], node_type=fnt, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record["b"], node_type=tnt, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record["r"], relationship_type=rt
                ),
            ).into_ingest(),
        )

    # -- Query builders ---------------------------------------------------------

    def build_where_clause(self, var: str, *, sample: bool = False) -> str:
        clauses: list[str] = []
        if sample and self.sample_ratio:
            clauses.append(
                f"toInteger(split(elementId({var}), ':')[-1]) % {self.sample_ratio} = 0"
            )
        if self.latest_hours is not None:
            clauses.append(f"{var}.`{LAST_INGESTED_AT_PROPERTY}` >= $cutoff")
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
        where = self.build_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where)
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def preview_relationship_count(
        self, relationship_type: str, cutoff: datetime | None = None
    ) -> int:
        where = self.build_where_clause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def build_histogram(self) -> TypeHistogram:
        cutoff = self.snapshotCutoff()
        nodeCounts = dict(
            zip(
                self.node_types,
                await asyncio.gather(
                    *(
                        self.preview_node_count(t, cutoff=cutoff)
                        for t in self.node_types
                    )
                ),
            )
        )
        relCounts = {}
        if not self.node_only:
            relCounts = dict(
                zip(
                    self.relationship_types,
                    await asyncio.gather(
                        *(
                            self.preview_relationship_count(t, cutoff=cutoff)
                            for t in self.relationship_types
                        )
                    ),
                )
            )
        return TypeHistogram(node_counts=nodeCounts, relationship_counts=relCounts)

    def compute_shards(
        self, total_count: int, shard_size: int
    ) -> List[Tuple[int, int]]:
        if total_count <= 0 or shard_size <= 0:
            return []
        numShards = math.ceil(total_count / shard_size)
        return [
            (i * shard_size, min(shard_size, total_count - i * shard_size))
            for i in range(numShards)
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
