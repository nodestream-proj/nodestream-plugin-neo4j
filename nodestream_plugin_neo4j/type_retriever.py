import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import ACTIVE_QUERIES
from nodestream.metrics import Metrics
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
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


class QueueDone:
    """Sentinel that signals queue consumers all producers have finished."""


async def awaitAll(tasks: list, queue: asyncio.Queue) -> None:
    try:
        await asyncio.gather(*tasks)
    finally:
        await queue.put(QueueDone)


async def drainUntilDone(queue: asyncio.Queue) -> AsyncGenerator:
    while True:
        item = await queue.get()
        if item is QueueDone:
            break
        yield item


# ---------------------------------------------------------------------------
# Node fetch strategies
# ---------------------------------------------------------------------------


class NodeFetchStrategy(ABC):
    def __init__(self, retriever: "Neo4jTypeRetriever") -> None:
        self.retriever = retriever

    @abstractmethod
    async def fetch(self) -> AsyncGenerator[Node, None]:
        ...


class SequentialNodeFetch(NodeFetchStrategy):
    async def fetch(self) -> AsyncGenerator[Node, None]:
        schema = self.retriever.schema
        for nodeType in self.retriever.node_types:
            cutoff = self.retriever.snapshotCutoff()
            async for node in self.retriever.get_nodes_of_type(
                nodeType, schema=schema, cutoff=cutoff
            ):
                yield node


class ShardedNodeFetch(NodeFetchStrategy):
    async def fetch(self) -> AsyncGenerator[Node, None]:
        schema = self.retriever.schema
        for nodeType in self.retriever.node_types:
            cutoff = self.retriever.snapshotCutoff()
            count = await self.retriever.preview_node_count(nodeType, cutoff=cutoff)
            keyField = self.retriever.key_field_for_node_type(nodeType, schema)
            for shardOffset, shardLimit in self.retriever.compute_shards(
                count, self.retriever.shard_size
            ):
                async for node in self.retriever.get_nodes_of_type_shard(
                    nodeType,
                    keyField,
                    shardOffset,
                    shardLimit,
                    schema=schema,
                    cutoff=cutoff,
                ):
                    yield node


class ConcurrentNodeFetch(NodeFetchStrategy):
    async def fetch(self) -> AsyncGenerator[Node, None]:
        schema = self.retriever.schema
        queue: asyncio.Queue = asyncio.Queue()
        semaphore = asyncio.Semaphore(self.retriever.concurrency_limit)

        async def fetchType(nodeType: str) -> None:
            async with semaphore:
                cutoff = self.retriever.snapshotCutoff()
                async for node in self.retriever.get_nodes_of_type(
                    nodeType, schema=schema, cutoff=cutoff
                ):
                    await queue.put(node)

        tasks = [
            asyncio.create_task(fetchType(nodeType))
            for nodeType in self.retriever.node_types
        ]
        if not tasks:
            return

        producer = asyncio.create_task(awaitAll(tasks, queue))
        async for node in drainUntilDone(queue):
            yield node
        await producer


# ---------------------------------------------------------------------------
# Relationship fetch strategies
# ---------------------------------------------------------------------------


class RelFetchStrategy(ABC):
    """Template: subclasses implement planSpecs; fetch and fetchSpecIntoQueue are shared."""

    def __init__(self, retriever: "Neo4jTypeRetriever") -> None:
        self.retriever = retriever

    @abstractmethod
    async def planSpecs(self) -> List[Tuple]:
        ...

    async def fetchSpecIntoQueue(
        self,
        spec: Tuple,
        queue: asyncio.Queue,
        semaphore: asyncio.Semaphore,
    ) -> None:
        schema = self.retriever.schema
        relationshipType, adjacency, cutoff, shardOffset, shardLimit, keyField = spec
        async with semaphore:
            Metrics.get().increment(ACTIVE_QUERIES)
            try:
                if shardOffset is not None:
                    generator = self.retriever.get_relationships_of_type_between_shard(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relationshipType,
                        keyField,
                        shardOffset,
                        shardLimit,
                        schema=schema,
                        cutoff=cutoff,
                    )
                else:
                    generator = self.retriever.get_relationships_of_type_between(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relationshipType,
                        schema=schema,
                        cutoff=cutoff,
                    )
                async for item in generator:
                    await queue.put(item)
            finally:
                Metrics.get().decrement(ACTIVE_QUERIES)

    async def fetch(self) -> AsyncGenerator[RelationshipWithNodes, None]:
        fetchSpecs = await self.planSpecs()
        if not fetchSpecs:
            return

        queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.retriever.orchestrator_queue_size or 0
        )
        semaphore = asyncio.Semaphore(self.retriever.concurrency_limit)
        tasks = [
            asyncio.create_task(self.fetchSpecIntoQueue(spec, queue, semaphore))
            for spec in fetchSpecs
        ]
        producer = asyncio.create_task(awaitAll(tasks, queue))
        async for relationship in drainUntilDone(queue):
            yield relationship
        await producer


class SimpleRelFetch(RelFetchStrategy):
    """One spec per (relationshipType, adjacency) — no COUNT queries, no sharding."""

    async def planSpecs(self) -> List[Tuple]:
        schema = self.retriever.schema
        fetchSpecs = []
        for relationshipType in self.retriever.relationship_types:
            adjacencies = list(
                schema.get_adjacencies_by_relationship_type(relationshipType)
            )
            if not adjacencies:
                continue
            cutoff = self.retriever.snapshotCutoff()
            for adjacency in adjacencies:
                fetchSpecs.append(
                    (relationshipType, adjacency, cutoff, None, None, None)
                )
        return fetchSpecs


class ShardedRelFetch(RelFetchStrategy):
    """Fires concurrent COUNTs first, then one spec per (relationshipType, adjacency, shard)."""

    async def planSpecs(self) -> List[Tuple]:
        schema = self.retriever.schema
        relationshipTypeAdjacencies = []
        for relationshipType in self.retriever.relationship_types:
            adjacencies = list(
                schema.get_adjacencies_by_relationship_type(relationshipType)
            )
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

        fetchSpecs = []
        for relationshipType, adjacencies, _ in relationshipTypeAdjacencies:
            count, cutoff = counts[relationshipType]
            keyField = self.retriever.key_field_for_relationship_type(
                relationshipType, schema
            )
            for shardOffset, shardLimit in self.retriever.compute_shards(
                count, self.retriever.shard_size
            ):
                for adjacency in adjacencies:
                    fetchSpecs.append(
                        (
                            relationshipType,
                            adjacency,
                            cutoff,
                            shardOffset,
                            shardLimit,
                            keyField,
                        )
                    )
        return fetchSpecs


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
            node_only=node_only,
        )
        self.database_connection = database_connection
        self.limit = limit
        self.node_types = node_types if node_types is not None else [n.name for n in schema.nodes]
        self.relationship_types = relationship_types if relationship_types is not None else [r.name for r in schema.relationships]
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.shard_size = shard_size
        self.max_shards_per_type = max_shards_per_type

        # Strategy selection resolved once at construction — no runtime conditionals.
        if shard_size is not None:
            self.node_fetch_strategy: NodeFetchStrategy = ShardedNodeFetch(self)
            self.rel_fetch_strategy: RelFetchStrategy = ShardedRelFetch(self)
        elif concurrency_limit > 1:
            self.node_fetch_strategy = ConcurrentNodeFetch(self)
            self.rel_fetch_strategy = SimpleRelFetch(self)
        else:
            self.node_fetch_strategy = SequentialNodeFetch(self)
            self.rel_fetch_strategy = SimpleRelFetch(self)

    async def fetchNodes(self) -> AsyncGenerator[Node, None]:
        async for node in self.node_fetch_strategy.fetch():
            yield node

    async def fetchRelationships(self) -> AsyncGenerator[RelationshipWithNodes, None]:
        async for relationship in self.rel_fetch_strategy.fetch():
            yield relationship

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

    # -- Query builders ---------------------------------------------------------

    def build_where_clause(self, var: str, *, sample: bool = False) -> str:
        clauses: list[str] = []
        if sample and self.sample_ratio:
            # NOTE: parses Neo4j's elementId() string (format "4:<uuid>:<id>") to
            # extract a numeric id for deterministic sampling. Documented as opaque
            # so this could break in a future Neo4j release.
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

    def get_node_type_extractor(
        self, node_type: str, cutoff: datetime | None = None
    ) -> Neo4jExtractor:
        where = self.build_where_clause("n")
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )

    async def execute_node_shard_query(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        cutoff: datetime | None = None,
    ):
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
        return await self.database_connection.execute(
            Query(statement, params), routing_=RoutingControl.READ
        )

    def get_relationships_of_type_between_extractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        where = self.build_where_clause("r", sample=True)
        return Neo4jExtractor(
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

    async def execute_relationship_shard_query(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        cutoff: datetime | None = None,
    ):
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
        return await self.database_connection.execute(
            Query(statement, params), routing_=RoutingControl.READ
        )

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

    # -- Generators -------------------------------------------------------------

    async def get_nodes_of_type(
        self,
        node_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[Node, None]:
        extractor = self.get_node_type_extractor(node_type, cutoff=cutoff)
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=node_type, schema=schema
            )

    async def get_nodes_of_type_shard(
        self,
        node_type: str,
        key_field: str,
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[Node, None]:
        records = await self.execute_node_shard_query(
            node_type, key_field, shard_offset, shard_limit, cutoff=cutoff
        )
        for record in records:
            yield self.map_neo4j_node_to_nodestream_node(
                record["n"], node_type=node_type, schema=schema
            )

    async def get_relationships_of_type_between(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        extractor = self.get_relationships_of_type_between_extractor(
            from_node_type, to_node_type, relationship_type, cutoff=cutoff
        )
        async for record in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=from_node_type, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=to_node_type, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record.original["r"], relationship_type=relationship_type
                ),
            )

    async def get_relationships_of_type_between_shard(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: str,
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        records = await self.execute_relationship_shard_query(
            from_node_type,
            to_node_type,
            relationship_type,
            key_field,
            shard_offset,
            shard_limit,
            cutoff=cutoff,
        )
        for record in records:
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record["a"], node_type=from_node_type, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record["b"], node_type=to_node_type, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record["r"], relationship_type=relationship_type
                ),
            )
