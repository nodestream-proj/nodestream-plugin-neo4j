import pytest
from nodestream.schema import Adjacency, AdjacencyCardinality, Cardinality
from nodestream.schema.state import GraphObjectSchema, Schema

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


def _build_test_schema() -> Schema:
    schema = Schema()
    schema.put_node_type(GraphObjectSchema(PERSON_LABEL))
    schema.put_node_type(GraphObjectSchema(COMPANY_LABEL))
    schema.add_adjacency(
        Adjacency(PERSON_LABEL, COMPANY_LABEL, RELATIONSHIP_TYPE),
        AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    return schema


def _make_retriever(
    connection: Neo4jDatabaseConnection, **kwargs
) -> Neo4jTypeRetriever:
    return Neo4jTypeRetriever(
        connection,
        _build_test_schema(),
        shard_size=1000,
        **kwargs,
    )


async def _collect_all_records(retriever: Neo4jTypeRetriever) -> list:
    await retriever.build_histogram()
    records = []
    async for extractor in retriever.fetch_extractors():
        async for record in extractor.extract_records():
            records.append(record)
    return records


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
    with neo4j_container(
        neo4j_version
    ) as container, container.get_driver() as driver, driver.session() as session:
        seed_graph(
            session,
            num_people=SINGLE_CONTAINER_PERSON_COUNT,
            num_companies=SINGLE_CONTAINER_COMPANY_COUNT,
            rel_type=RELATIONSHIP_TYPE,
        )

        connection = Neo4jDatabaseConnection.from_configuration(
            uri=container.get_connection_url(),
            username="neo4j",
            password="password",
        )
        retriever = _make_retriever(connection)
        await retriever.build_histogram()

        assert (
            retriever.histogram.node_counts.get(PERSON_LABEL, 0)
            == SINGLE_CONTAINER_PERSON_COUNT
        )
        assert (
            retriever.histogram.node_counts.get(COMPANY_LABEL, 0)
            == SINGLE_CONTAINER_COMPANY_COUNT
        )
        assert (
            retriever.histogram.relationship_counts.get(RELATIONSHIP_TYPE, 0)
            == SINGLE_CONTAINER_REL_COUNT
        )

        # Collect all records via fetch_extractors and verify counts
        records = []
        async for extractor in retriever.fetch_extractors():
            async for record in extractor.extract_records():
                records.append(record)

        assert len(records) == (
            SINGLE_CONTAINER_PERSON_COUNT
            + SINGLE_CONTAINER_COMPANY_COUNT
            + SINGLE_CONTAINER_REL_COUNT
        )


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
async def test_type_retriever_relationships_only(neo4j_container, neo4j_version):
    with neo4j_container(
        neo4j_version
    ) as container, container.get_driver() as driver, driver.session() as session:
        seed_graph(
            session,
            num_people=SINGLE_CONTAINER_PERSON_COUNT,
            num_companies=SINGLE_CONTAINER_COMPANY_COUNT,
            rel_type=RELATIONSHIP_TYPE,
        )

        connection = Neo4jDatabaseConnection.from_configuration(
            uri=container.get_connection_url(),
            username="neo4j",
            password="password",
        )
        retriever = _make_retriever(connection, relationships_only=True)
        await retriever.build_histogram()

        records = []
        async for extractor in retriever.fetch_extractors():
            async for record in extractor.extract_records():
                records.append(record)

        # Only relationships should be fetched — no node records
        assert len(records) == SINGLE_CONTAINER_REL_COUNT


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

        connection_a = Neo4jDatabaseConnection.from_configuration(
            uri=container_a.get_connection_url(), username="neo4j", password="password"
        )
        connection_b = Neo4jDatabaseConnection.from_configuration(
            uri=container_b.get_connection_url(), username="neo4j", password="password"
        )
        retriever_a = _make_retriever(connection_a)
        retriever_b = _make_retriever(connection_b)

        await retriever_a.build_histogram()
        await retriever_b.build_histogram()

        assert (
            retriever_a.histogram.node_counts.get(PERSON_LABEL, 0)
            == CONTAINER_A_PERSON_COUNT
        )
        assert (
            retriever_b.histogram.node_counts.get(PERSON_LABEL, 0)
            == CONTAINER_B_PERSON_COUNT
        )
        assert (
            retriever_b.histogram.node_counts[PERSON_LABEL]
            >= retriever_a.histogram.node_counts[PERSON_LABEL]
        )

        assert (
            retriever_a.histogram.relationship_counts.get(RELATIONSHIP_TYPE, 0)
            == CONTAINER_A_REL_COUNT
        )
        assert (
            retriever_b.histogram.relationship_counts.get(RELATIONSHIP_TYPE, 0)
            == CONTAINER_B_REL_COUNT
        )
        assert (
            retriever_b.histogram.relationship_counts[RELATIONSHIP_TYPE]
            >= retriever_a.histogram.relationship_counts[RELATIONSHIP_TYPE]
        )
