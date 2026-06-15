import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Callable, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import ACTIVE_QUERIES, TypeHistogram
from nodestream.metrics import Metrics
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


class MappingExtractor(Extractor):
    """Wraps a Neo4jExtractor and applies a record-mapping function."""

    def __init__(self, inner: Neo4jExtractor, mapRecord: Callable) -> None:
        self.inner = inner
        self.mapRecord = mapRecord

    async def extract_records(self) -> AsyncGenerator:
        async for record in self.inner.extract_records():
            yield self.mapRecord(record)


class ShardExtractor(Extractor):
    """Runs a single pre-fetched shard query and maps records."""

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
# Node extractor strategies
# ---------------------------------------------------------------------------


class NodeExtractorStrategy(ABC):
    def __init__(self, retriever: "Neo4jTypeRetriever") -> None:
        self.retriever = retriever

    @abstractmethod
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        ...


class SequentialNodeExtractors(NodeExtractorStrategy):
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.retriever.schema
        for nodeType in self.retriever.node_types:
            cutoff = self.retriever.snapshotCutoff()
            yield self.retriever.buildNodeExtractor(nodeType, schema=schema, cutoff=cutoff)


class ShardedNodeExtractors(NodeExtractorStrategy):
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.retriever.schema
        for nodeType in self.retriever.node_types:
            cutoff = self.retriever.snapshotCutoff()
            count = await self.retriever.preview_node_count(nodeType, cutoff=cutoff)
            keyField = self.retriever.key_field_for_node_type(nodeType, schema)
            for shardOffset, shardLimit in self.retriever.compute_shards(count, self.retriever.shard_size):
                yield self.retriever.buildNodeShardExtractor(
                    nodeType, keyField, shardOffset, shardLimit, schema=schema, cutoff=cutoff
                )


# ---------------------------------------------------------------------------
# Relationship extractor strategies
# ---------------------------------------------------------------------------


class RelExtractorStrategy(ABC):
    def __init__(self, retriever: "Neo4jTypeRetriever") -> None:
        self.retriever = retriever

    @abstractmethod
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        ...


class SimpleRelExtractors(RelExtractorStrategy):
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.retriever.schema
        for relationshipType in self.retriever.relationship_types:
            adjacencies = list(schema.get_adjacencies_by_relationship_type(relationshipType))
            if not adjacencies:
                continue
            cutoff = self.retriever.snapshotCutoff()
            for adjacency in adjacencies:
                yield self.retriever.buildRelExtractor(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    relationshipType,
                    schema=schema,
                    cutoff=cutoff,
                )


class ShardedRelExtractors(RelExtractorStrategy):
    async def extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.retriever.schema
        relationshipTypeAdjacencies = []
        for relationshipType in self.retriever.relationship_types:
            adjacencies = list(schema.get_adjacencies_by_relationship_type(relationshipType))
            if not adjacencies:
                continue
            cutoff = self.retriever.snapshotCutoff()
            relationshipTypeAdjacencies.append((relationshipType, adjacencies, cutoff))

        countResults = await asyncio.gather(
            *(
                self.retriever.count_relationship_type(relationshipType, cutoff)
                for relationshipType, _, cutoff in relationshipTypeAdjacencies
            )
        )
        counts = {
            relationshipType: (count, cutoff)
            for relationshipType, count, cutoff in countResults
        }

        for relationshipType, adjacencies, _ in relationshipTypeAdjacencies:
            count, cutoff = counts[relationshipType]
            keyField = self.retriever.key_field_for_relationship_type(relationshipType, schema)
            for shardOffset, shardLimit in self.retriever.compute_shards(count, self.retriever.shard_size):
                for adjacency in adjacencies:
                    yield self.retriever.buildRelShardExtractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relationshipType,
                        keyField,
                        shardOffset,
                        shardLimit,
                        schema=schema,
                        cutoff=cutoff,
                    )


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
        concurrency_limit: int = 1,
        orchestrator_queue_size: int = 0,
        shard_size: int | None = None,
        max_shards_per_type: int = 10000,
    ) -> None:
        super().__init__(
            schema=schema,
            concurrency_limit=concurrency_limit,
            orchestrator_queue_size=orchestrator_queue_size,
        )
        self.database_connection = database_connection
        self.limit = limit
        self.node_types = node_types if node_types is not None else [n.name for n in schema.nodes]
        self.relationship_types = relationship_types if relationship_types is not None else [r.name for r in schema.relationships]
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.node_only = node_only
        self.shard_size = shard_size
        self.max_shards_per_type = max_shards_per_type

        if shard_size is not None:
            self.nodeExtractorStrategy: NodeExtractorStrategy = ShardedNodeExtractors(self)
            self.relExtractorStrategy: RelExtractorStrategy = ShardedRelExtractors(self)
        else:
            self.nodeExtractorStrategy = SequentialNodeExtractors(self)
            self.relExtractorStrategy = SimpleRelExtractors(self)

    async def fetchNodeExtractors(self) -> AsyncGenerator[Extractor, None]:
        async for extractor in self.nodeExtractorStrategy.extractors():
            yield extractor

    async def fetchRelationshipExtractors(self) -> AsyncGenerator[Extractor, None]:
        if self.node_only:
            return
        async for extractor in self.relExtractorStrategy.extractors():
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
        self, node_type: str, schema: Schema | None = None, cutoff: datetime | None = None
    ) -> Extractor:
        where = self.build_where_clause("n")
        inner = Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return MappingExtractor(
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
        return ShardExtractor(
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
        return MappingExtractor(
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
        return ShardExtractor(
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

    async def count_relationship_type(
        self, relationship_type: str, cutoff: datetime | None
    ) -> tuple[str, int, datetime | None]:
        count = await self.preview_relationship_count(relationship_type, cutoff=cutoff)
        return relationship_type, count, cutoff

    async def build_histogram(self) -> TypeHistogram:
        nodeCounts = {}
        for nodeType in self.node_types:
            nodeCounts[nodeType] = await self.preview_node_count(nodeType)
        relCounts = {}
        if not self.node_only:
            for relType in self.relationship_types:
                relCounts[relType] = await self.preview_relationship_count(relType)
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
