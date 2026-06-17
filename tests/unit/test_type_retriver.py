from typing import cast as type_cast
from unittest.mock import AsyncMock

import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.model import Node, PropertySet, Relationship
from nodestream.schema.state import (
    Adjacency,
    AdjacencyCardinality,
    Cardinality,
    GraphObjectSchema,
    PropertyMetadata,
    PropertyType,
    Schema,
)

from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.type_retriever import (
    LAST_INGESTED_AT_PROPERTY,
    Neo4jMappingExtractor,
    Neo4jShardExtractor,
    Neo4jTypeRetriever,
)


class FakeNeo4jNode(dict):
    """Dict-like object with a .labels attribute, mimicking neo4j.graph.Node."""

    def __init__(self, labels, props):
        super().__init__(props)
        self.labels = frozenset(labels)


class FakeNeo4jRel(dict):
    """Dict-like object mimicking neo4j.graph.Relationship."""

    def __init__(self, rel_type, props):
        super().__init__(props)
        self.type = rel_type


class FakeRecord:
    """Mimics a neo4j Record for testing (supports .data() and __getitem__)."""

    def __init__(self, payload):
        self._p = payload

    def data(self):
        return {k: dict(v) if isinstance(v, dict) else v for k, v in self._p.items()}

    def keys(self):
        return list(self._p.keys())

    def __getitem__(self, k):
        return self._p[k]


@pytest.fixture
def empty_schema():
    return Schema()


@pytest.fixture
def subject(mocker, empty_schema):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection, empty_schema)


@pytest.fixture
def filtered_subject(mocker, empty_schema):
    """A retriever with both sampling and recency filters enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(
        connection, empty_schema, limit=500, sample_ratio=5, latest_hours=24
    )


@pytest.fixture
def sampled_subject(mocker, empty_schema):
    """A retriever with only sampling enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection, empty_schema, sample_ratio=3)


async def async_generator(*items):
    for item in items:
        yield item


# -- Mapping tests ----------------------------------------------------------


def test_map_neo4j_node_to_nodestream_node(subject):
    neo_node = FakeNeo4jNode(("Person", "Employee"), {"name": "John", "id": 123})
    result = subject.map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Person"
    )
    assert result == Node(
        type="Person",
        properties=PropertySet({"name": "John", "id": 123}),
        additional_types=("Employee",),
    )


def test_map_neo4j_relationship_to_nodestream_relationship(subject):
    rel = FakeNeo4jRel("KNOWS", {"since": 2019})
    result = subject.map_neo4j_relationship_to_nodestream_relationship(
        type_cast(Neo4jRelationship, rel), relationship_type="KNOWS"
    )
    assert result == Relationship(type="KNOWS", properties=PropertySet({"since": 2019}))


# -- Where-clause / filter tests --------------------------------------------


def test_where_clause_empty_when_no_filters(subject):
    assert_that(subject.build_where_clause("n"), equal_to(""))


def test_where_clause_with_sample_ratio(sampled_subject):
    assert_that(
        sampled_subject.build_where_clause("r", sample=True),
        equal_to("WHERE toInteger(split(elementId(r), ':')[-1]) % 3 = 0\n"),
    )


def test_where_clause_sample_not_applied_to_nodes(sampled_subject):
    assert_that(sampled_subject.build_where_clause("n"), equal_to(""))


def test_where_clause_with_latest_hours(mocker):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(connection, Schema(), latest_hours=12)
    assert_that(
        retriever.build_where_clause("r"),
        equal_to("WHERE r.`last_ingested_at` >= $cutoff\n"),
    )


def test_where_clause_with_both_filters(filtered_subject):
    assert_that(
        filtered_subject.build_where_clause("r", sample=True),
        equal_to(
            "WHERE toInteger(split(elementId(r), ':')[-1]) % 5 = 0"
            " AND r.`last_ingested_at` >= $cutoff\n"
        ),
    )


def test_filter_parameters_empty_when_no_filters(subject):
    assert_that(subject.build_filter_parameters(), equal_to({}))


def test_filter_parameters_with_latest_hours(filtered_subject):
    params = filtered_subject.build_filter_parameters()
    assert set(params.keys()) == {"cutoff"}


def test_sample_ratio_of_one_is_ignored(mocker):
    """sample_ratio=1 would return everything; treat it as disabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(connection, Schema(), sample_ratio=1)
    assert retriever.sample_ratio is None
    assert_that(retriever.build_where_clause("n"), equal_to(""))


# -- Extractor builder tests -------------------------------------------------


def test_build_node_extractor_query(subject):
    extractor = subject._build_node_extractor("Person")
    assert isinstance(extractor, Neo4jMappingExtractor)
    assert "MATCH (n:Person)" in extractor.inner.query
    assert "SKIP $offset LIMIT $limit" in extractor.inner.query


def test_build_node_extractor_with_filters(filtered_subject):
    extractor = filtered_subject._build_node_extractor("Person")
    assert isinstance(extractor, Neo4jMappingExtractor)
    assert "WHERE" in extractor.inner.query
    assert_that(extractor.inner.limit, equal_to(500))
    assert "cutoff" in extractor.inner.parameters


def test_build_rel_extractor_query(subject):
    extractor = subject._build_rel_extractor("Person", "Company", "KNOWS")
    assert isinstance(extractor, Neo4jMappingExtractor)
    assert "MATCH (a:Person)-[r:KNOWS]->(b:Company)" in extractor.inner.query
    assert "SKIP $offset LIMIT $limit" in extractor.inner.query


def test_build_rel_extractor_with_filters(filtered_subject):
    extractor = filtered_subject._build_rel_extractor("Person", "Company", "KNOWS")
    assert isinstance(extractor, Neo4jMappingExtractor)
    assert "WHERE" in extractor.inner.query
    assert_that(extractor.inner.limit, equal_to(500))
    assert "cutoff" in extractor.inner.parameters


def test_build_node_shard_extractor_with_key_field(subject):
    extractor = subject._build_node_shard_extractor("Person", "name", 0, 1000)
    assert isinstance(extractor, Neo4jShardExtractor)
    assert "ORDER BY n.`name`" in extractor.statement
    assert extractor.params["shard_offset"] == 0
    assert extractor.params["shard_limit"] == 1000


def test_build_node_shard_extractor_without_key_field(subject):
    extractor = subject._build_node_shard_extractor("Person", None, 500, 500)
    assert isinstance(extractor, Neo4jShardExtractor)
    assert "ORDER BY elementId(n)" in extractor.statement
    assert extractor.params["shard_offset"] == 500
    assert extractor.params["shard_limit"] == 500


def test_build_rel_shard_extractor_with_key_field(subject):
    extractor = subject._build_rel_shard_extractor(
        "Person", "Person", "BEST_FRIEND_OF", "since", 0, 2000
    )
    assert isinstance(extractor, Neo4jShardExtractor)
    assert "ORDER BY r.`since`" in extractor.statement
    assert extractor.params["shard_offset"] == 0
    assert extractor.params["shard_limit"] == 2000


def test_build_rel_shard_extractor_without_key_field(subject):
    extractor = subject._build_rel_shard_extractor(
        "Person", "Person", "BEST_FRIEND_OF", None, 100, 900
    )
    assert isinstance(extractor, Neo4jShardExtractor)
    assert "ORDER BY elementId(r)" in extractor.statement
    assert extractor.params["shard_offset"] == 100
    assert extractor.params["shard_limit"] == 900


# -- Preview count tests ----------------------------------------------------


@pytest.mark.asyncio
async def test_preview_node_count(subject):
    subject.database_connection.execute.return_value = [{"count": 42}]
    count = await subject.preview_node_count("Person")
    assert_that(count, equal_to(42))
    call_kwargs = subject.database_connection.execute.call_args
    assert_that(call_kwargs.kwargs.get("routing_"), equal_to(RoutingControl.READ))


@pytest.mark.asyncio
async def test_preview_node_count_empty_result(subject):
    subject.database_connection.execute.return_value = []
    assert_that(await subject.preview_node_count("Ghost"), equal_to(0))


@pytest.mark.asyncio
async def test_preview_relationship_count(subject):
    subject.database_connection.execute.return_value = [{"count": 99}]
    count = await subject.preview_relationship_count("KNOWS")
    assert_that(count, equal_to(99))
    call_kwargs = subject.database_connection.execute.call_args
    assert_that(call_kwargs.kwargs.get("routing_"), equal_to(RoutingControl.READ))


@pytest.mark.asyncio
async def test_preview_relationship_count_empty_result(subject):
    subject.database_connection.execute.return_value = []
    assert_that(await subject.preview_relationship_count("GHOST_REL"), equal_to(0))


@pytest.mark.asyncio
async def test_preview_node_count_with_filters(filtered_subject):
    filtered_subject.database_connection.execute.return_value = [{"count": 10}]
    count = await filtered_subject.preview_node_count("Person")
    assert_that(count, equal_to(10))
    query_arg = filtered_subject.database_connection.execute.call_args.args[0]
    assert "WHERE" in query_arg.query_statement
    assert "cutoff" in query_arg.parameters


# -- map_neo4j_node_to_nodestream_node with schema (key extraction) ----------


@pytest.fixture
def basic_schema():
    schema = Schema()
    person = GraphObjectSchema(
        name="Person",
        properties={
            "name": PropertyMetadata(PropertyType.STRING, is_key=True),
            "age": PropertyMetadata(PropertyType.INTEGER),
        },
    )
    organization = GraphObjectSchema(
        name="Organization",
        properties={
            "name": PropertyMetadata(PropertyType.STRING),
        },
    )
    best_friend_of = GraphObjectSchema(
        name="BEST_FRIEND_OF",
        properties={
            "since": PropertyMetadata(PropertyType.DATETIME),
        },
    )
    schema.put_node_type(person)
    schema.put_node_type(organization)
    schema.put_relationship_type(best_friend_of)
    schema.add_adjacency(
        adjacency=Adjacency("Person", "Person", "BEST_FRIEND_OF"),
        cardinality=AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    return schema


def test_map_neo4j_node_to_nodestream_node_with_schema_extracts_keys(
    subject, basic_schema
):
    neo_node = FakeNeo4jNode(("Person",), {"name": "Alice", "age": 30})
    result = subject.map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Person", schema=basic_schema
    )
    assert "name" in result.key_values
    assert result.key_values["name"] == "Alice"
    assert "name" not in result.properties


def test_map_neo4j_node_to_nodestream_node_schema_unknown_type(subject, basic_schema):
    neo_node = FakeNeo4jNode(("Unknown",), {"x": 1})
    result = subject.map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Unknown", schema=basic_schema
    )
    assert result.type == "Unknown"
    assert result.properties["x"] == 1
    assert len(result.key_values) == 0


# -- _compute_shards ----------------------------------------------------------


def test__compute_shards_zero_count_returns_empty(subject):
    assert subject._compute_shards(0, 1000) == []


def test__compute_shards_negative_shard_size_returns_empty(subject):
    assert subject._compute_shards(5000, 0) == []


# -- key_field helpers -------------------------------------------------------


def test_key_field_for_node_type_with_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema, latest_hours=24)
    assert (
        retriever.key_field_for_node_type("Person", basic_schema)
        == LAST_INGESTED_AT_PROPERTY
    )


def test_key_field_for_node_type_from_schema_keys(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema)
    assert retriever.key_field_for_node_type("Person", basic_schema) == "name"


def test_key_field_for_node_type_no_schema_keys(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema)
    assert retriever.key_field_for_node_type("Organization", basic_schema) is None


def test_key_field_for_node_type_unknown_type(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema)
    assert retriever.key_field_for_node_type("Ghost", basic_schema) is None


def test_key_field_for_relationship_type_with_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema, latest_hours=6)
    assert (
        retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema)
        == LAST_INGESTED_AT_PROPERTY
    )


def test_key_field_for_relationship_type_no_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema)
    assert (
        retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema)
        is None
    )


# -- fetch_extractors --------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_extractors_node_only_no_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, node_types=["Person"], node_only=True
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    assert_that(extractors, has_length(1))
    assert isinstance(extractors[0], Neo4jMappingExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_node_only_with_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, node_types=["Person"], node_only=True, shard_size=1000
    )
    retriever.preview_node_count = AsyncMock(return_value=2500)
    extractors = [e async for e in retriever.fetch_extractors()]
    # 2500 / 1000 = 3 shards
    assert_that(extractors, has_length(3))
    for e in extractors:
        assert isinstance(e, Neo4jShardExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_adjacency_no_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, relationship_types=["BEST_FRIEND_OF"]
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    # BEST_FRIEND_OF has one adjacency (Person->Person)
    assert_that(extractors, has_length(1))
    assert isinstance(extractors[0], Neo4jMappingExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_adjacency_with_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, relationship_types=["BEST_FRIEND_OF"], shard_size=500
    )
    retriever.preview_relationship_count = AsyncMock(return_value=1000)
    extractors = [e async for e in retriever.fetch_extractors()]
    # 1000 / 500 = 2 shards, 1 adjacency
    assert_that(extractors, has_length(2))
    for e in extractors:
        assert isinstance(e, Neo4jShardExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_skips_unknown_rel_type(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, relationship_types=["UNKNOWN_REL"]
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    assert extractors == []


# -- Neo4jMappingExtractor / Neo4jShardExtractor extract_records -------------


@pytest.mark.asyncio
async def test_mapping_extractor_extract_records(mocker):
    inner_record = FakeRecord({"n": FakeNeo4jNode(("Person",), {"name": "Alice"})})
    inner_extractor = mocker.AsyncMock()
    inner_extractor.extract_records = mocker.MagicMock(
        return_value=async_generator(inner_record)
    )
    mapped = []
    extractor = Neo4jMappingExtractor(inner_extractor, map_record=lambda r: r["n"])
    async for record in extractor.extract_records():
        mapped.append(record)
    assert_that(mapped, has_length(1))
    assert mapped[0] == inner_record["n"]


@pytest.mark.asyncio
async def test_shard_extractor_extract_records(mocker):
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    fake_node = FakeNeo4jNode(("Person",), {"name": "Bob"})
    connection.execute = AsyncMock(return_value=[{"n": fake_node}])
    extractor = Neo4jShardExtractor(
        connection=connection,
        statement="MATCH (n:Person) SKIP $shard_offset LIMIT $shard_limit RETURN n",
        params={"shard_offset": 0, "shard_limit": 10},
        map_record=lambda r: r["n"],
    )
    results = []
    async for record in extractor.extract_records():
        results.append(record)
    assert_that(results, has_length(1))
    assert results[0] == fake_node
    call_kwargs = connection.execute.call_args
    assert call_kwargs.kwargs.get("routing_") == RoutingControl.READ


# -- RoundRobinDistribution --------------------------------------------------


@pytest.mark.asyncio
async def test_round_robin_distribution_interleaves_types(mocker, basic_schema):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(
        conn,
        basic_schema,
        node_types=["Person", "Organization"],
        node_only=True,
        shard_size=1000,
        distribution="round_robin",
    )
    retriever.preview_node_count = AsyncMock(side_effect=[2000, 1000])
    extractors = [e async for e in retriever.fetch_extractors()]
    # Person: 2 shards, Organization: 1 shard → 3 total, interleaved
    assert_that(extractors, has_length(3))


@pytest.mark.asyncio
async def test_round_robin_distribution_unequal_shard_counts(mocker, basic_schema):
    from nodestream_plugin_neo4j.type_retriever import RoundRobinDistribution

    e1a = mocker.Mock()
    e1b = mocker.Mock()
    e2a = mocker.Mock()

    distribution = RoundRobinDistribution()
    result = [e async for e in distribution.distribute([[e1a, e1b], [e2a]])]
    # Round-robin: e1a, e2a, e1b (sentinel skipped for second slot in round 2)
    assert result == [e1a, e2a, e1b]


# -- build_histogram ---------------------------------------------------------


@pytest.mark.asyncio
async def test_build_histogram_nodes_and_rels(mocker, basic_schema):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(
        conn,
        basic_schema,
        node_types=["Person"],
        relationship_types=["BEST_FRIEND_OF"],
    )
    retriever.preview_node_count = AsyncMock(return_value=42)
    retriever.preview_relationship_count = AsyncMock(return_value=99)
    histogram = await retriever.build_histogram()
    assert histogram.node_counts == {"Person": 42}
    assert histogram.relationship_counts == {"BEST_FRIEND_OF": 99}


@pytest.mark.asyncio
async def test_build_histogram_node_only_skips_rel_counts(mocker, basic_schema):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(
        conn,
        basic_schema,
        node_types=["Person"],
        node_only=True,
    )
    retriever.preview_node_count = AsyncMock(return_value=10)
    retriever.preview_relationship_count = AsyncMock(return_value=999)
    histogram = await retriever.build_histogram()
    assert histogram.node_counts == {"Person": 10}
    assert histogram.relationship_counts == {}
    retriever.preview_relationship_count.assert_not_called()


# -- _snapshot_cutoff ----------------------------------------------------------


def test_snapshot_cutoff_with_latest_hours(mocker):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(conn, Schema(), latest_hours=6)
    cutoff = retriever._snapshot_cutoff()
    assert cutoff is not None
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    delta = now - cutoff
    # Should be ~6 hours (allow 5s drift)
    assert abs(delta.total_seconds() - 6 * 3600) < 5
