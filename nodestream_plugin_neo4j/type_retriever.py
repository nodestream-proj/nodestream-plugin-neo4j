import math
from typing import AsyncGenerator, List, Optional, Set, Tuple, cast

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes

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


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        limit: int = 1000,
        *,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
    ) -> None:
        self.database_connection = database_connection
        self.limit = limit
        # Optional sampling: if >1, only take elements where id(x) % sample_ratio = 0.
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        # Optional recency filter, based on a `last_ingested_at` temporal property.
        # When set, only nodes/relationships with last_ingested_at within the last
        # `latest_hours` hours will be returned / counted.
        self.latest_hours = latest_hours

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, node_type: str
    ) -> Node:
        return Node(
            type=node_type,
            properties=PropertySet(node),
            additional_types=cast(
                Tuple[str], tuple(label for label in node.labels if label != node_type)
            ),
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship, relationship_type: str
    ) -> Relationship:
        return Relationship(
            type=relationship_type,
            properties=PropertySet(relationship),
        )

    def _where_clause(self, var: str) -> str:
        """Build an optional WHERE clause (with trailing newline) for *var*.

        Returns an empty string when no filters are active so the query
        template collapses cleanly.
        """
        clauses: list[str] = []

        if self.sample_ratio:
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
                f"{var}.`{LAST_INGESTED_AT_PROPERTY}` >= "
                "datetime() - duration({hours: $latest_hours})"
            )

        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses) + "\n"

    def _filter_parameters(self) -> dict[str, object]:
        """Parameters required by any active filters."""
        if self.latest_hours is None:
            return {}
        return {"latest_hours": self.latest_hours}

    def get_node_type_extractor(self, node_type: str) -> Neo4jExtractor:
        where = self._where_clause("n")
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self._filter_parameters(),
            limit=self.limit,
        )

    def get_node_type_shard_extractor(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window.

        The shard window [shard_offset, shard_offset + shard_limit) is applied
        over the ORDER BY sorted result set *after* any active filters.  Orders
        by key_field when provided, otherwise falls back to elementId(n) which
        is always available.  Within that window the extractor still paginates
        with self.limit-sized pages so individual queries stay small.
        """
        where = self._where_clause("n")
        if key_field:
            query = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=node_type, where=where, key_field=key_field
            )
        else:
            query = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=node_type, where=where
            )
        params = dict(
            self._filter_parameters(),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )

    def get_relationships_of_type_bettween_extractor(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> Neo4jExtractor:
        where = self._where_clause("r")
        return Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
            ),
            self.database_connection,
            parameters=self._filter_parameters(),
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
    ) -> Neo4jExtractor:
        """Return an extractor scoped to a pre-computed shard window for relationships.

        Orders by key_field when provided, otherwise falls back to elementId(r).
        """
        where = self._where_clause("r")
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
            self._filter_parameters(),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )
        return Neo4jExtractor(
            query,
            self.database_connection,
            parameters=params,
            limit=self.limit,
        )

    def compute_shards(
        self, total_count: int, shard_size: int
    ) -> List[Tuple[int, int]]:
        """Return (shard_offset, shard_limit) pairs covering [0, total_count).

        Each shard covers at most shard_size records.  The last shard may be
        smaller.  If total_count is 0 a single zero-width shard is returned so
        the caller always gets at least one entry to process.
        """
        if total_count <= 0 or shard_size <= 0:
            return [(0, shard_size or 1)]
        num_shards = math.ceil(total_count / shard_size)
        return [
            (i * shard_size, min(shard_size, total_count - i * shard_size))
            for i in range(num_shards)
        ]

    async def preview_node_count(self, node_type: str) -> int:
        """Return a quick count of nodes of the given type."""
        where = self._where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where)
        query = Query(statement, self._filter_parameters())
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def preview_relationship_count(self, relationship_type: str) -> int:
        """Return a quick count of relationships of the given type."""
        where = self._where_clause("r")
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where
        )
        query = Query(statement, self._filter_parameters())
        results = await self.database_connection.execute(
            query, routing_=RoutingControl.READ
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        extractor = self.get_node_type_extractor(node_type)
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=node_type
            )

    async def get_nodes_of_type_shard(
        self,
        node_type: str,
        key_field: str,
        shard_offset: int,
        shard_limit: int,
    ) -> AsyncGenerator[Node, None]:
        """Yield nodes from a single pre-computed shard window."""
        extractor = self.get_node_type_shard_extractor(
            node_type, key_field, shard_offset, shard_limit
        )
        async for record in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(
                record.original["n"], node_type=node_type
            )

    async def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        extractor = self.get_relationships_of_type_bettween_extractor(
            from_node_type, to_node_type, relationship_type
        )
        async for record in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=from_node_type
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=to_node_type
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
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield relationships from a single pre-computed shard window."""
        extractor = self.get_relationships_of_type_between_shard_extractor(
            from_node_type,
            to_node_type,
            relationship_type,
            key_field,
            shard_offset,
            shard_limit,
        )
        async for record in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["a"], node_type=from_node_type
                ),
                to_node=self.map_neo4j_node_to_nodestream_node(
                    record.original["b"], node_type=to_node_type
                ),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    record.original["r"], relationship_type=relationship_type
                ),
            )
