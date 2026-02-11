import pytest

from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.type_retriever import Neo4jTypeRetriever

from .conftest import TESTED_NEO4J_VERSIONS

# Global test parameters to avoid magic numbers and centralize expectations
PERSON_LABEL = "Person"
COMPANY_LABEL = "Company"
RELATIONSHIP_TYPE = "WORKS_AT"

# Single-container dataset sizes and expected relationship counts
SINGLE_CONTAINER_PERSON_COUNT = 30
SINGLE_CONTAINER_COMPANY_COUNT = 10
SINGLE_CONTAINER_REL_COUNT = (
    SINGLE_CONTAINER_PERSON_COUNT // 2
) * SINGLE_CONTAINER_COMPANY_COUNT

# Two-container dataset sizes (A smaller, B larger) and expected relationship counts
CONTAINER_A_PERSON_COUNT = 20
CONTAINER_A_COMPANY_COUNT = 5
CONTAINER_A_REL_COUNT = (CONTAINER_A_PERSON_COUNT // 2) * CONTAINER_A_COMPANY_COUNT

CONTAINER_B_PERSON_COUNT = 40
CONTAINER_B_COMPANY_COUNT = 8
CONTAINER_B_REL_COUNT = (CONTAINER_B_PERSON_COUNT // 2) * CONTAINER_B_COMPANY_COUNT


def seed_graph(
    session, num_people: int, num_companies: int, rel_type: str = RELATIONSHIP_TYPE
):
    """Populate the database with Persons, Companies, and WORKS_AT relationships."""
    session.run(
        f"""
        UNWIND range(1, $n) AS i
        MERGE (n:{PERSON_LABEL} {{ id: i }})
        SET n += {{
          name: 'p'+toString(i),
          age: i,
          active: i % 2 = 0,
          tags: ['t'+toString(i), 'common'],
          scores: [i, i+1],
          created_at: datetime()
        }}
        """,
        parameters={"n": num_people},
    )
    # Add an extra label to some nodes to validate multi-label handling
    session.run(
        f"""
        MATCH (n:{PERSON_LABEL})
        WHERE n.id % 2 = 0
        SET n:Employee
        """,
    )
    # Add another additional label to a different subset
    session.run(
        f"""
        MATCH (n:{PERSON_LABEL})
        WHERE n.id % 3 = 0
        SET n:Contractor
        """,
    )
    session.run(
        f"""
        UNWIND range(1, $n) AS i
        MERGE (c:{COMPANY_LABEL} {{ id: i }})
        SET c += {{
          name: 'c'+toString(i),
          rating: toFloat(i),
          categories: ['catA','catB']
        }}
        """,
        parameters={"n": num_companies},
    )
    # Add additional labels to a subset of companies
    session.run(
        f"""
        MATCH (c:{COMPANY_LABEL})
        WHERE c.id % 2 = 1
        SET c:Vendor
        """,
    )
    session.run(
        f"""
        MATCH (p:{PERSON_LABEL}), (c:{COMPANY_LABEL})
        WHERE p.id % 2 = 0 AND c.id <= $n
        WITH p, c
        MERGE (p)-[r:{rel_type} {{ since: 2020 }}]->(c)
        SET r += {{ last_ingested_at: datetime() }}
        """,
        parameters={"n": num_companies},
    )


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
async def test_type_retriever_single_container(neo4j_container, neo4j_version):
    # Start a single Neo4j container and seed it with test data
    with neo4j_container(
        neo4j_version
    ) as primary_container, primary_container.get_driver() as primary_driver, primary_driver.session() as primary_session:
        seed_graph(
            primary_session,
            num_people=SINGLE_CONTAINER_PERSON_COUNT,
            num_companies=SINGLE_CONTAINER_COMPANY_COUNT,
            rel_type=RELATIONSHIP_TYPE,
        )

        # Create a database connection and initialize the type retriever
        primary_connection = Neo4jDatabaseConnection.from_configuration(
            uri=primary_container.get_connection_url(),
            username="neo4j",
            password="password",
        )
        type_retriever = Neo4jTypeRetriever(primary_connection)

        # Verify node retrieval by type
        person_nodes = [
            node async for node in type_retriever.get_nodes_of_type(PERSON_LABEL)
        ]
        assert len(person_nodes) == SINGLE_CONTAINER_PERSON_COUNT
        # Ensure complex shapes made it through normalization and mapping
        assert any(
            "tags" in n.properties and isinstance(n.properties["tags"], list)
            for n in person_nodes
        )
        assert any(
            "scores" in n.properties and isinstance(n.properties["scores"], list)
            for n in person_nodes
        )
        assert any(
            "active" in n.properties and isinstance(n.properties["active"], bool)
            for n in person_nodes
        )
        # At least one multi-labeled node should have an additional type
        assert any("Employee" in n.additional_types for n in person_nodes)

        # Verify relationship retrieval between specific node types
        works_at_between_person_company = [
            rel
            async for rel in type_retriever.get_relationships_of_type_between(
                PERSON_LABEL, COMPANY_LABEL, RELATIONSHIP_TYPE
            )
        ]
        assert len(works_at_between_person_company) == SINGLE_CONTAINER_REL_COUNT
        # All relationships should include the seeded property
        assert all(
            "since" in r.relationship.properties
            for r in works_at_between_person_company
        )


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
async def test_type_retriever_two_containers_counts(neo4j_container, neo4j_version):
    """
    Spin up two independent graphs with different sizes and ensure that:
      - The container with larger seeded data returns more (or equal) results for the same queries.
    """
    with neo4j_container(
        neo4j_version
    ) as container_a, container_a.get_driver() as driver_a, driver_a.session() as session_a, neo4j_container(
        neo4j_version
    ) as container_b, container_b.get_driver() as driver_b, driver_b.session() as session_b:
        # Seed different volumes of data in each container
        seed_graph(
            session_a,
            num_people=CONTAINER_A_PERSON_COUNT,
            num_companies=CONTAINER_A_COMPANY_COUNT,
            rel_type=RELATIONSHIP_TYPE,
        )
        seed_graph(
            session_b,
            num_people=CONTAINER_B_PERSON_COUNT,
            num_companies=CONTAINER_B_COMPANY_COUNT,
            rel_type=RELATIONSHIP_TYPE,
        )

        # Initialize connections and type retrievers for each container
        connection_a = Neo4jDatabaseConnection.from_configuration(
            uri=container_a.get_connection_url(), username="neo4j", password="password"
        )
        connection_b = Neo4jDatabaseConnection.from_configuration(
            uri=container_b.get_connection_url(), username="neo4j", password="password"
        )
        type_retriever_a = Neo4jTypeRetriever(connection_a)
        type_retriever_b = Neo4jTypeRetriever(connection_b)

        # Compare counts of Person nodes
        person_nodes_a = [
            n async for n in type_retriever_a.get_nodes_of_type(PERSON_LABEL)
        ]
        person_nodes_b = [
            n async for n in type_retriever_b.get_nodes_of_type(PERSON_LABEL)
        ]
        assert len(person_nodes_a) == CONTAINER_A_PERSON_COUNT
        assert len(person_nodes_b) == CONTAINER_B_PERSON_COUNT
        assert len(person_nodes_b) >= len(person_nodes_a)
        # Spot-check complex shapes exist in both containers
        assert any("tags" in n.properties for n in person_nodes_a)
        assert any("tags" in n.properties for n in person_nodes_b)

        # Compare counts of WORKS_AT relationships specifically between Person and Company
        works_at_between_a = [
            r
            async for r in type_retriever_a.get_relationships_of_type_between(
                PERSON_LABEL, COMPANY_LABEL, RELATIONSHIP_TYPE
            )
        ]
        works_at_between_b = [
            r
            async for r in type_retriever_b.get_relationships_of_type_between(
                PERSON_LABEL, COMPANY_LABEL, RELATIONSHIP_TYPE
            )
        ]
        assert len(works_at_between_a) == CONTAINER_A_REL_COUNT
        assert len(works_at_between_b) == CONTAINER_B_REL_COUNT
        assert len(works_at_between_b) >= len(works_at_between_a)
