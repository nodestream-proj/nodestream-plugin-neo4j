import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, List, Optional, Set, Tuple, cast

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


# Keyed variants: ORDER BY a stable key field then SKIP/LIMIT within a
# pre-computed shard window.  The shard window is [shard_offset, shard_offset +
# shard_limit) and is applied *after* filtering, so the caller pre-computes
# shard boundaries from the histogram count.  Using WITH … ORDER BY before SKIP
# forces Neo4j to sort the filtered set consistently across parallel shards.
FETCH_NODES_SHARD_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY n.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY r.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b SKIP $offset LIMIT $limit
"""

# Fallback shard variant for nodes/relationships with no schema key — orders by
# elementId which is always available and provides a stable, consistent ordering.
FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY elementId(n) SKIP $shard_offset LIMIT $shard_limit
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY elementId(r) SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b SKIP $offset LIMIT $limit
"""


COUNT_NODES_BY_TYPE_QUERY_FORMAT = """\
MATCH (n:{type})
{where}RETURN count(n) AS count
"""

COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """\
MATCH ()-[r:{relationship_type}]->()
{where}RETURN count(r) AS count
"""

# Sentinel object for internal producer/consumer queues inside fetch_relationships.
class _QueueDone:
    pass


_QUEUE_DONE = _QueueDone()


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        limit: int = 1000,
        *,
        node_types: List[str] | None = None,
        relationship_types: List[str] | None = None,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
        relationships_only: bool = False,
        concurrency_limit: int = 1,
        orchestrator_queue_size: int = 0,
        shard_size: int | None = None,
        max_shards_per_type: int = 10000,
    ) -> None:
        self.database_connection = database_connection
        self.limit = limit
        self.node_types = node_types or []
        self.relationship_types = relationship_types or []
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.relationships_only = relationships_only
        self.concurrency_limit = concurrency_limit
        self.orchestrator_queue_size = orchestrator_queue_size
        self.shard_size = shard_size
        self.max_shards_per_type = max_shards_per_type

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, node_type: str, schema: Schema | None = None
    ) -> Node:
        properties = PropertySet(node)
        key_values = PropertySet.empty()
        if schema is not None:
            node_schema = schema.get_node_type_by_name(node_type)
            if node_schema is not None:
                for key in node_schema.keys:
                    if key in properties:
                        key_values[key] = properties.pop(key)
        # Preserve any additional labels beyond the primary type.
        additional_types: List[str] = [
            label for label in node.labels if label != node_type
        ]
        return Node(
            type=node_type,
            properties=properties,
            key_values=key_values,
            additional_types=additional_types,
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship, relationship_type: str
    ) -> Relationship:
        return Relationship(
            type=relationship_type,
            properties=PropertySet(relationship),
        )

    def whereClause(self, var: str, *, sample: bool = False) -> str:
        """Build an optional WHERE clause (with trailing newline) for *var*.

        Returns an empty string when no filters are active so the query
        template collapses cleanly.

        sample=True adds the elementId modulo filter. This should only be set
        for relationship variables — sampling is always keyed on relationship
        elementId, never on node elementId.
        """
        clauses: list[str] = []

        if sample and self.sample_ratio:
            # NOTE: Known limitation – we parse Neo4j's elementId() string to
            # extract a numeric internal id for deterministic sampling.  The
            # current Neo4j 5+ format is "4:<database-uuid>:<internal-id>" and
            # we grab the last colon-separated segment.  elementId() is
            # documented as opaque so this could break in a future Neo4j
            # release, but there is no stable, APOC-free alternative today.
            clauses.append(
                f"toInteger(split(elementId({var}), ':')[-1]) % {self.sample_ratio} = 0"
            )

        if self.latest_hours is not None:
            clauses.append(
                f"{var}.`{LAST_INGESTED_AT_PROPERTY}` >= $cutoff"
            )

        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses) + "\n"

    def filterParameters(self, cutoff: datetime | None = None) -> dict[str, object]:
        """Parameters required by any active filters."""
        if self.latest_hours is None:
            return {}
        if cutoff is None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
        return {"cutoff": cutoff}

    def getNodeTypeExtractor(self, nodeType: str, cutoff: datetime | None = None) -> Neo4jExtractor:
        where = self.whereClause("n")
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=nodeType, where=where),
            self.database_connection,
            parameters=self.filterParameters(cutoff),
            limit=self.limit,
        )

    def getNodeTypeShardExtractor(
        self,
        nodeType: str,
        keyField: Optional[str],
        shardOffset: int,
        shardLimit: int,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window.

        The shard window [shardOffset, shardOffset + shardLimit) is applied
        over the ORDER BY sorted result set *after* any active filters.  Orders
        by keyField when provided, otherwise falls back to elementId(n) which
        is always available.  Within that window the extractor still paginates
        with self.limit-sized pages so individual queries stay small.
        """
        where = self.whereClause("n")
        if keyField:
            query = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=nodeType, where=where, key_field=keyField
            )
        else:
            query = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=nodeType, where=where
            )
        params = dict(
            self.filterParameters(cutoff),
            shard_offset=shardOffset,
            shard_limit=shardLimit,
        )
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )

    def getRelationshipsOfTypeBetweenExtractor(
        self, fromNodeType: str, toNodeType: str, relationshipType: str,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        where = self.whereClause("r", sample=True)
        return Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=where,
            ),
            self.database_connection,
            parameters=self.filterParameters(cutoff),
            limit=self.limit,
        )

    def getRelationshipsOfTypeBetweenShardExtractor(
        self,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        keyField: Optional[str],
        shardOffset: int,
        shardLimit: int,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window for relationships.

        Orders by keyField when provided, otherwise falls back to elementId(r).
        """
        where = self.whereClause("r", sample=True)
        if keyField:
            query = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=where,
                key_field=keyField,
            )
        else:
            query = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
                from_node_type=fromNodeType,
                relationship_type=relationshipType,
                to_node_type=toNodeType,
                where=where,
            )
        params = dict(
            self.filterParameters(cutoff),
            shard_offset=shardOffset,
            shard_limit=shardLimit,
        )
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )


    async def previewNodeCount(self, nodeType: str, cutoff: datetime | None = None) -> int:
        """Return a quick count of nodes of the given type."""
        where = self.whereClause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=nodeType, where=where)
        query = Query(statement, self.filterParameters(cutoff))
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def previewRelationshipCount(self, relationshipType: str, cutoff: datetime | None = None) -> int:
        """Return a quick count of relationships of the given type."""
        where = self.whereClause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationshipType, where=where
        )
        query = Query(statement, self.filterParameters(cutoff))
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def runBoundedIntoQueue(self, coroutines, queue: asyncio.Queue) -> None:
        """Run coroutines concurrently bounded by self.concurrency_limit, pushing
        results into *queue*. Sends _QUEUE_DONE when all coroutines finish."""
        sem = asyncio.Semaphore(self.concurrency_limit)

        async def bounded(coro):
            async with sem:
                await coro

        await asyncio.gather(*(bounded(c) for c in coroutines))
        await queue.put(_QUEUE_DONE)

    async def fetchRelTypeIntoQueue(
        self,
        relType: str,
        adj,
        schema: Schema,
        queue: asyncio.Queue,
        cutoff: datetime | None = None,
        shardOffset: int | None = None,
        shardLimit: int | None = None,
        keyField: str | None = None,
    ) -> None:
        """Fetch relationships for a single (relType, adjacency[, shard]) unit.

        Without sharding (shardOffset is None): streams the full type.
        With sharding: fetches the single pre-computed shard window.
        Tracks ACTIVE_QUERIES for the duration of the fetch.
        """
        Metrics.get().increment(ACTIVE_QUERIES)
        try:
            if shardOffset is not None:
                gen = self.getRelationshipsOfTypeBetweenShard(
                    adj.from_node_type, adj.to_node_type, relType,
                    keyField, shardOffset, shardLimit, schema=schema, cutoff=cutoff,
                )
            else:
                gen = self.getRelationshipsOfTypeBetween(
                    adj.from_node_type, adj.to_node_type, relType, schema=schema, cutoff=cutoff,
                )
            async for rwn in gen:
                await queue.put(rwn)
        finally:
            Metrics.get().decrement(ACTIVE_QUERIES)

    def computeShards(self, totalCount: int, shardSize: int) -> List[Tuple[int, int]]:
        """Return (shardOffset, shardLimit) pairs covering [0, totalCount)."""
        if totalCount <= 0 or shardSize <= 0:
            return []
        numShards = math.ceil(totalCount / shardSize)
        return [
            (i * shardSize, min(shardSize, totalCount - i * shardSize))
            for i in range(numShards)
        ]

    async def countRelType(
        self, relType: str, cutoff: datetime | None
    ) -> tuple[str, int, datetime | None]:
        """Return (relType, count, cutoff) — cutoff snapshotted before the COUNT fires."""
        count = await self.previewRelationshipCount(relType, cutoff=cutoff)
        return relType, count, cutoff

    async def buildRelCoroutines(
        self, schema: Schema, queue: asyncio.Queue
    ) -> list:
        """Build the flat list of coroutines to run concurrently.

        Without sharding: one coroutine per (relType, adjacency) pair.
        With sharding: COUNTs are fired concurrently for all rel types, then
        one coroutine per (relType, adjacency, shard) triple is built from results.
        """
        # Collect rel types that have adjacencies, snapshot cutoff per type
        relTypeAdjacencies = []
        for relType in self.relationship_types:
            adjacencies = list(schema.get_adjacencies_by_relationship_type(relType))
            if not adjacencies:
                continue
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
                if self.latest_hours is not None else None
            )
            relTypeAdjacencies.append((relType, adjacencies, cutoff))

        # Fan out COUNTs concurrently when sharding is enabled
        if self.shard_size is not None:
            countResults = await asyncio.gather(*(
                self.countRelType(relType, cutoff)
                for relType, _, cutoff in relTypeAdjacencies
            ))
            counts = {relType: (count, cutoff) for relType, count, cutoff in countResults}
        else:
            counts = {}

        coroutines = []
        for relType, adjacencies, cutoff in relTypeAdjacencies:
            if self.shard_size is not None:
                count, cutoff = counts[relType]
                keyField = self.keyFieldForRelationshipType(relType, schema)
                for shardOffset, shardLimit in self.computeShards(count, self.shard_size):
                    for adj in adjacencies:
                        coroutines.append(self.fetchRelTypeIntoQueue(
                            relType, adj, schema, queue, cutoff=cutoff,
                            shardOffset=shardOffset, shardLimit=shardLimit,
                            keyField=keyField,
                        ))
            else:
                for adj in adjacencies:
                    coroutines.append(self.fetchRelTypeIntoQueue(
                        relType, adj, schema, queue, cutoff=cutoff,
                    ))
        return coroutines

    async def fetch_nodes(self, schema: Schema) -> AsyncGenerator[Node, None]:
        """Yield all nodes across self.node_types, sharded when shard_size is set."""
        if self.relationships_only:
            return
        for nodeType in self.node_types:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
                if self.latest_hours is not None else None
            )
            if self.shard_size is not None:
                count = await self.previewNodeCount(nodeType, cutoff=cutoff)
                keyField = self.keyFieldForNodeType(nodeType, schema)
                for shardOffset, shardLimit in self.computeShards(count, self.shard_size):
                    async for node in self.getNodesOfTypeShard(nodeType, keyField, shardOffset, shardLimit, schema=schema, cutoff=cutoff):
                        yield node
            else:
                async for node in self.getNodesOfType(nodeType, schema=schema, cutoff=cutoff):
                    yield node

    async def fetch_relationships(self, schema: Schema) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield all relationships concurrently across all (relType, adjacency[, shard]) units.

        Without sharding: one coroutine per (relType, adjacency) pair.
        With sharding: one coroutine per (relType, adjacency, shard) — every shard
        competes directly for concurrency slots so large types don't starve small ones.
        Up to self.concurrency_limit units run in parallel. ACTIVE_QUERIES reflects
        true concurrent Neo4j reads.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.orchestrator_queue_size or 0)
        coroutines = await self.buildRelCoroutines(schema, queue)

        if not coroutines:
            return

        producer = asyncio.create_task(
            self.runBoundedIntoQueue(coroutines, queue)
        )

        try:
            while True:
                item = await queue.get()
                if isinstance(item, _QueueDone):
                    break
                yield item
        finally:
            await producer

    def keyFieldForNodeType(self, nodeType: str, schema: Schema) -> Optional[str]:
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        nodeSchema = schema.get_node_type_by_name(nodeType)
        if nodeSchema and nodeSchema.keys:
            return next(iter(nodeSchema.keys))
        return None

    def keyFieldForRelationshipType(self, relType: str, schema: Schema) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None

    async def getNodesOfType(self, nodeType: str, schema: Schema | None = None, cutoff: datetime | None = None) -> AsyncGenerator[Node, None]:
        extractor = self.getNodeTypeExtractor(nodeType, cutoff=cutoff)
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=nodeType, schema=schema
            )

    async def getNodesOfTypeShard(
        self,
        nodeType: str,
        keyField: str,
        shardOffset: int,
        shardLimit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[Node, None]:
        """Yield nodes from a single pre-computed shard window."""
        extractor = self.getNodeTypeShardExtractor(
            nodeType, keyField, shardOffset, shardLimit, cutoff=cutoff
        )
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=nodeType, schema=schema
            )

    async def getRelationshipsOfTypeBetween(
        self, fromNodeType: str, toNodeType: str, relationshipType: str,
        schema: Schema | None = None, cutoff: datetime | None = None,
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        extractor = self.getRelationshipsOfTypeBetweenExtractor(
            fromNodeType, toNodeType, relationshipType, cutoff=cutoff
        )
        async for record in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=fromNodeType, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=toNodeType, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record.original["r"], relationship_type=relationshipType
                ),
            )

    async def getRelationshipsOfTypeBetweenShard(
        self,
        fromNodeType: str,
        toNodeType: str,
        relationshipType: str,
        keyField: str,
        shardOffset: int,
        shardLimit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield relationships from a single pre-computed shard window."""
        extractor = self.getRelationshipsOfTypeBetweenShardExtractor(
            fromNodeType,
            toNodeType,
            relationshipType,
            keyField,
            shardOffset,
            shardLimit,
            cutoff=cutoff,
        )
        async for record in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=fromNodeType, schema=schema
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=toNodeType, schema=schema
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record.original["r"], relationship_type=relationshipType
                ),
            )
