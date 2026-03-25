from typing import AsyncGenerator, Tuple, cast

from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes

from .extractor import Neo4jExtractor
from .neo4j_database import Neo4jDatabaseConnection

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT = """
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
RETURN a, r, b SKIP $offset LIMIT $limit
"""


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(self, database_connection: Neo4jDatabaseConnection) -> None:
        self.database_connection = database_connection

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

    def get_node_type_extractor(self, node_type: str) -> Neo4jExtractor:
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type),
            self.database_connection,
        )

    def get_relationships_of_type_bettween_extractor(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> Neo4jExtractor:
        return Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
            ),
            self.database_connection,
        )

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
