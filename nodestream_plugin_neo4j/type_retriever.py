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

    def build_where_clause(self, var: str, *, sample: bool = False) -> str:
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

    def build_filter_parameters(self, cutoff: datetime | None = None) -> dict[str, object]:
        """Parameters required by any active filters."""
        if self.latest_hours is None:
            return {}
        if cutoff is None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
        return {"cutoff": cutoff}

    def get_node_type_extractor(self, node_type: str, cutoff: datetime | None = None) -> Neo4jExtractor:
        where = self.build_where_clause("n")
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )

    def get_node_type_shard_extractor(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window.

        The shard window [shard_offset, shard_offset + shard_limit) is applied
        over the ORDER BY sorted result set *after* any active filters.  Orders
        by key_field when provided, otherwise falls back to elementId(n) which
        is always available.  Within that window the extractor still paginates
        with self.limit-sized pages so individual queries stay small.
        """
        where = self.build_where_clause("n")
        if key_field:
            query = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=node_type, where=where, key_field=key_field
            )
        else:
            query = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=node_type, where=where
            )
        params = dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )

    def get_relationships_of_type_between_extractor(
        self, from_node_type: str, to_node_type: str, relationship_type: str,
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

    def get_relationships_of_type_between_shard_extractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        cutoff: datetime | None = None,
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window for relationships.

        Orders by key_field when provided, otherwise falls back to elementId(r).
        """
        where = self.build_where_clause("r", sample=True)
        if key_field:
            query = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
                key_field=key_field,
            )
        else:
            query = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
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
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )

    async def preview_node_count(self, node_type: str, cutoff: datetime | None = None) -> int:
        """Return a quick count of nodes of the given type."""
        where = self.build_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where)
        query = Query(statement, self.build_filter_parameters(cutoff))
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def preview_relationship_count(self, relationship_type: str, cutoff: datetime | None = None) -> int:
        """Return a quick count of relationships of the given type."""
        where = self.build_where_clause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where
        )
        query = Query(statement, self.build_filter_parameters(cutoff))
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    def compute_shards(self, total_count: int, shard_size: int) -> List[Tuple[int, int]]:
        """Return (shard_offset, shard_limit) pairs covering [0, total_count)."""
        if total_count <= 0 or shard_size <= 0:
            return []
        num_shards = math.ceil(total_count / shard_size)
        return [
            (i * shard_size, min(shard_size, total_count - i * shard_size))
            for i in range(num_shards)
        ]

    async def count_relationship_type(
        self, relationship_type: str, cutoff: datetime | None
    ) -> tuple[str, int, datetime | None]:
        """Return (relationship_type, count, cutoff) — cutoff snapshotted before the COUNT fires."""
        count = await self.preview_relationship_count(relationship_type, cutoff=cutoff)
        return relationship_type, count, cutoff

    async def plan_relationship_fetches(self, schema: Schema) -> List[Tuple]:
        """Return the flat list of fetch specs to run concurrently.

        Each spec is a (rel_type, adj, cutoff, shard_offset, shard_limit, key_field) tuple.
        shard_offset/shard_limit/key_field are None when sharding is disabled.

        Without sharding: one spec per (rel_type, adjacency) pair.
        With sharding: COUNTs are fired concurrently for all rel types first, then
        one spec per (rel_type, adjacency, shard) triple.
        """
        # Collect rel types that have adjacencies, snapshot cutoff per type.
        rel_type_adjacencies = []
        for rel_type in self.relationship_types:
            adjacencies = list(schema.get_adjacencies_by_relationship_type(rel_type))
            if not adjacencies:
                continue
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
                if self.latest_hours is not None else None
            )
            rel_type_adjacencies.append((rel_type, adjacencies, cutoff))

        # Fan out COUNTs concurrently when sharding is enabled.
        if self.shard_size is not None:
            count_results = await asyncio.gather(*(
                self.count_relationship_type(rel_type, cutoff)
                for rel_type, _, cutoff in rel_type_adjacencies
            ))
            counts = {rel_type: (count, cutoff) for rel_type, count, cutoff in count_results}
        else:
            counts = {}

        specs = []
        for rel_type, adjacencies, cutoff in rel_type_adjacencies:
            if self.shard_size is not None:
                count, cutoff = counts[rel_type]
                key_field = self.key_field_for_relationship_type(rel_type, schema)
                for shard_offset, shard_limit in self.compute_shards(count, self.shard_size):
                    for adj in adjacencies:
                        specs.append((rel_type, adj, cutoff, shard_offset, shard_limit, key_field))
            else:
                for adj in adjacencies:
                    specs.append((rel_type, adj, cutoff, None, None, None))
        return specs

    async def fetch_nodes(self, schema: Schema) -> AsyncGenerator[Node, None]:
        """Yield all nodes across self.node_types, sharded when shard_size is set."""
        if self.relationships_only:
            return
        for node_type in self.node_types:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
                if self.latest_hours is not None else None
            )
            if self.shard_size is not None:
                count = await self.preview_node_count(node_type, cutoff=cutoff)
                key_field = self.key_field_for_node_type(node_type, schema)
                for shard_offset, shard_limit in self.compute_shards(count, self.shard_size):
                    async for node in self.get_nodes_of_type_shard(node_type, key_field, shard_offset, shard_limit, schema=schema, cutoff=cutoff):
                        yield node
            else:
                async for node in self.get_nodes_of_type(node_type, schema=schema, cutoff=cutoff):
                    yield node

    async def fetch_relationships(self, schema: Schema) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield all relationships concurrently across all (rel_type, adjacency[, shard]) units.

        Without sharding: one coroutine per (rel_type, adjacency) pair.
        With sharding: one coroutine per (rel_type, adjacency, shard) — every shard
        competes directly for concurrency slots so large types don't starve small ones.
        Up to self.concurrency_limit units run in parallel. ACTIVE_QUERIES reflects
        true concurrent Neo4j reads.
        """
        specs = await self.plan_relationship_fetches(schema)
        if not specs:
            return

        queue: asyncio.Queue = asyncio.Queue(maxsize=self.orchestrator_queue_size or 0)
        sem = asyncio.Semaphore(self.concurrency_limit)

        async def fetch_into_queue(rel_type, adj, cutoff, shard_offset, shard_limit, key_field):
            Metrics.get().increment(ACTIVE_QUERIES)
            try:
                if shard_offset is not None:
                    gen = self.get_relationships_of_type_between_shard(
                        adj.from_node_type, adj.to_node_type, rel_type,
                        key_field, shard_offset, shard_limit, schema=schema, cutoff=cutoff,
                    )
                else:
                    gen = self.get_relationships_of_type_between(
                        adj.from_node_type, adj.to_node_type, rel_type, schema=schema, cutoff=cutoff,
                    )
                async for rwn in gen:
                    await queue.put(rwn)
            finally:
                Metrics.get().decrement(ACTIVE_QUERIES)

        async def run_all_bounded():
            async def run_bounded(spec):
                async with sem:
                    await fetch_into_queue(*spec)
            await asyncio.gather(*(run_bounded(spec) for spec in specs))
            await queue.put(_QUEUE_DONE)

        producer = asyncio.create_task(run_all_bounded())

        try:
            while True:
                item = await queue.get()
                if isinstance(item, _QueueDone):
                    break
                yield item
        finally:
            await producer

    def key_field_for_node_type(self, node_type: str, schema: Schema) -> Optional[str]:
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        node_schema = schema.get_node_type_by_name(node_type)
        if node_schema and node_schema.keys:
            return next(iter(node_schema.keys))
        return None

    def key_field_for_relationship_type(self, relationship_type: str, schema: Schema) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None

    async def get_nodes_of_type(self, node_type: str, schema: Schema | None = None, cutoff: datetime | None = None) -> AsyncGenerator[Node, None]:
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
        """Yield nodes from a single pre-computed shard window."""
        extractor = self.get_node_type_shard_extractor(
            node_type, key_field, shard_offset, shard_limit, cutoff=cutoff
        )
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=node_type, schema=schema
            )

    async def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str,
        schema: Schema | None = None, cutoff: datetime | None = None,
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
        """Yield relationships from a single pre-computed shard window."""
        extractor = self.get_relationships_of_type_between_shard_extractor(
            from_node_type,
            to_node_type,
            relationship_type,
            key_field,
            shard_offset,
            shard_limit,
            cutoff=cutoff,
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
