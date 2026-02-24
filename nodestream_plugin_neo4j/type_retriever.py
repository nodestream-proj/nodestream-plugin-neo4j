from typing import AsyncGenerator, Tuple, cast

from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes

from .extractor import Neo4jExtractor
from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

LAST_INGESTED_AT_PROPERTY = "last_ingested_at"

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
{where}
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT = """
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}
RETURN a, r, b SKIP $offset LIMIT $limit
"""

COUNT_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
{where}
RETURN count(n) AS count
"""

COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """
MATCH ()-[r:{relationship_type}]->()
{where}
RETURN count(r) AS count
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

    def _node_where_clause(self, var: str) -> str:
        clauses: list[str] = []

        if self.sample_ratio:
            # Use Neo4j's elementId for sampling without APOC. The internal
            # element id has the form "<label-id>:<internal-id>". We take the
            # numeric suffix, cast it to an integer, and modulo that value to
            # get a deterministic sample.
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
        return "WHERE " + " AND ".join(clauses)

    def _relationship_where_clause(self, rel_var: str) -> str:
        clauses: list[str] = []

        if self.sample_ratio:
            clauses.append(
                f"toInteger(split(elementId({rel_var}), ':')[-1]) % {self.sample_ratio} = 0"
            )

        if self.latest_hours is not None:
            clauses.append(
                f"{rel_var}.`{LAST_INGESTED_AT_PROPERTY}` >= "
                "datetime() - duration({hours: $latest_hours})"
            )

        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses)

    def _filter_parameters(self) -> dict[str, object]:
        """Parameters required by any active filters."""
        if self.latest_hours is None:
            return {}
        return {"latest_hours": self.latest_hours}

    def get_node_type_extractor(self, node_type: str) -> Neo4jExtractor:
        where = self._node_where_clause("n")
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self._filter_parameters(),
            limit=self.limit,
        )

    def get_relationships_of_type_bettween_extractor(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> Neo4jExtractor:
        where = self._relationship_where_clause("r")
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

    async def preview_node_count(self, node_type: str) -> int:
        """Return a quick count of nodes of the given type."""
        where = self._node_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where)
        query = Query(statement, self._filter_parameters() or {})
        results = await self.database_connection.execute(query)
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def preview_relationship_count(self, relationship_type: str) -> int:
        """Return a quick count of relationships of the given type."""
        where = self._relationship_where_clause("r")
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where
        )
        query = Query(statement, self._filter_parameters() or {})
        results = await self.database_connection.execute(query)
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        extractor = self.get_node_type_extractor(node_type)
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
