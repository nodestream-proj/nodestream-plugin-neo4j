from typing import cast as type_cast
from unittest.mock import AsyncMock

import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases.copy import TypeHistogram
from nodestream.model import PropertySet, Relationship
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
    Neo4jNodeExtractor,
    Neo4jRelationshipExtractor,
    Neo4jTypeRetriever,
    map_neo4j_node_to_nodestream_node,
    map_neo4j_relationship_to_nodestream_relationship,
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
    return Neo4jTypeRetriever(connection, empty_schema, shard_size=1000)


@pytest.fixture
def filtered_subject(mocker, empty_schema):
    """A retriever with both sampling and recency filters enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(
        connection, empty_schema, shard_size=1000, sample_ratio=5, latest_hours=24
    )


@pytest.fixture
def sampled_subject(mocker, empty_schema):
    """A retriever with only sampling enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection, empty_schema, shard_size=1000, sample_ratio=3)


async def async_generator(*items):
    for item in items:
        yield item


# -- Mapping tests ----------------------------------------------------------


def test_map_neo4j_node_to_nodestream_node(basic_schema):
    neo_node = FakeNeo4jNode(("Person", "Employee"), {"name": "John", "id": 123})
    result = map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Person", schema=basic_schema
    )
    assert result is not None
    assert result.type == "Person"
    # "name" is a key field in basic_schema, so it moves to key_values
    assert result.key_values["name"] == "John"
    assert result.properties["id"] == 123
    assert "Employee" in result.additional_types


def test_map_neo4j_relationship_to_nodestream_relationship():
    rel = FakeNeo4jRel("KNOWS", {"since": 2019})
    result = map_neo4j_relationship_to_nodestream_relationship(
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
    retriever = Neo4jTypeRetriever(
        connection, Schema(), shard_size=1000, latest_hours=12
    )
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


def test_filter_parameters_always_includes_cutoff(subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    params = subject.build_filter_parameters(cutoff)
    assert params == {"cutoff": cutoff}


def test_filter_parameters_with_latest_hours(filtered_subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    params = filtered_subject.build_filter_parameters(cutoff)
    assert set(params.keys()) == {"cutoff"}


def test_sample_ratio_of_one_is_ignored(mocker):
    """sample_ratio=1 would return everything; treat it as disabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(
        connection, Schema(), shard_size=1000, sample_ratio=1
    )
    assert retriever.sample_ratio is None
    assert_that(retriever.build_where_clause("n"), equal_to(""))


# -- Extractor builder tests -------------------------------------------------


def test_build_node_extractor_with_key_field(subject, empty_schema):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    extractor = subject.build_node_shard_extractor(
        "Person", "name", 0, 1000, schema=empty_schema, cutoff=cutoff
    )
    assert isinstance(extractor, Neo4jNodeExtractor)
    assert "ORDER BY n.`name`" in extractor.statement
    assert extractor.params["shard_offset"] == 0
    assert extractor.params["shard_limit"] == 1000
    assert extractor.params["cutoff"] == cutoff
    assert extractor.node_type == "Person"


def test_build_node_extractor_without_key_field_uses_element_id(subject, empty_schema):
    """When key_field_for_node_type returns None (no schema key, no latest_hours),
    the builder falls back to elementId ordering — safe for non-nodestream graphs."""
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    extractor = subject.build_node_shard_extractor(
        "Person",
        None,
        500,
        500,
        schema=empty_schema,
        cutoff=cutoff,
    )
    assert isinstance(extractor, Neo4jNodeExtractor)
    assert "ORDER BY elementId(n)" in extractor.statement
    assert extractor.params["shard_offset"] == 500
    assert extractor.params["shard_limit"] == 500


def test_build_relationship_extractor_with_key_field(subject, empty_schema):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    extractor = subject.build_relationship_shard_extractor(
        "Person",
        "Person",
        "BEST_FRIEND_OF",
        "since",
        0,
        2000,
        schema=empty_schema,
        cutoff=cutoff,
    )
    assert isinstance(extractor, Neo4jRelationshipExtractor)
    assert "ORDER BY r.`since`" in extractor.statement
    assert extractor.params["shard_offset"] == 0
    assert extractor.params["shard_limit"] == 2000
    assert extractor.params["cutoff"] == cutoff
    assert extractor.from_node_type == "Person"
    assert extractor.to_node_type == "Person"
    assert extractor.relationship_type == "BEST_FRIEND_OF"


def test_build_relationship_extractor_without_key_field(subject, empty_schema):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    extractor = subject.build_relationship_shard_extractor(
        "Person",
        "Person",
        "BEST_FRIEND_OF",
        None,
        100,
        900,
        schema=empty_schema,
        cutoff=cutoff,
    )
    assert isinstance(extractor, Neo4jRelationshipExtractor)
    assert "ORDER BY elementId(r)" in extractor.statement
    assert extractor.params["shard_offset"] == 100
    assert extractor.params["shard_limit"] == 900


# -- Preview count tests ----------------------------------------------------


@pytest.mark.asyncio
async def test_preview_node_count(subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    subject.database_connection.execute.return_value = [{"count": 42}]
    count = await subject.preview_node_count("Person", cutoff=cutoff)
    assert_that(count, equal_to(42))
    call_kwargs = subject.database_connection.execute.call_args
    assert_that(call_kwargs.kwargs.get("routing_"), equal_to(RoutingControl.READ))


@pytest.mark.asyncio
async def test_preview_node_count_empty_result(subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    subject.database_connection.execute.return_value = []
    assert_that(await subject.preview_node_count("Ghost", cutoff=cutoff), equal_to(0))


@pytest.mark.asyncio
async def test_preview_relationship_count(subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    subject.database_connection.execute.return_value = [{"count": 99}]
    count = await subject.preview_relationship_count("KNOWS", cutoff=cutoff)
    assert_that(count, equal_to(99))
    call_kwargs = subject.database_connection.execute.call_args
    assert_that(call_kwargs.kwargs.get("routing_"), equal_to(RoutingControl.READ))


@pytest.mark.asyncio
async def test_preview_relationship_count_empty_result(subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    subject.database_connection.execute.return_value = []
    assert_that(
        await subject.preview_relationship_count("GHOST_REL", cutoff=cutoff),
        equal_to(0),
    )


@pytest.mark.asyncio
async def test_preview_node_count_with_filters(filtered_subject):
    from datetime import datetime, timezone

    cutoff = datetime.now(timezone.utc)
    filtered_subject.database_connection.execute.return_value = [{"count": 10}]
    count = await filtered_subject.preview_node_count("Person", cutoff=cutoff)
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


def test_map_neo4j_node_to_nodestream_node_with_schema_extracts_keys(basic_schema):
    neo_node = FakeNeo4jNode(("Person",), {"name": "Alice", "age": 30})
    result = map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Person", schema=basic_schema
    )
    assert result is not None
    assert "name" in result.key_values
    assert result.key_values["name"] == "Alice"
    assert "name" not in result.properties


def test_map_neo4j_node_to_nodestream_node_schema_unknown_type_returns_none(
    basic_schema,
):
    neo_node = FakeNeo4jNode(("Unknown",), {"x": 1})
    result = map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Unknown", schema=basic_schema
    )
    assert result is None


# -- computeShards ----------------------------------------------------------


def test_compute_shards_zero_count_returns_empty(subject):
    assert subject.compute_shards(0, 1000) == []


def test_compute_shards_negative_shard_size_returns_empty(subject):
    assert subject.compute_shards(5000, 0) == []


# -- key_field helpers -------------------------------------------------------


def test_key_field_for_node_type_with_latest_hours(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        connection, basic_schema, shard_size=1000, latest_hours=24
    )
    assert (
        retriever.key_field_for_node_type("Person", basic_schema)
        == LAST_INGESTED_AT_PROPERTY
    )


def test_key_field_for_node_type_from_schema_keys(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(connection, basic_schema, shard_size=1000)
    assert retriever.key_field_for_node_type("Person", basic_schema) == "name"


def test_key_field_for_node_type_no_schema_keys_returns_none(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(connection, basic_schema, shard_size=1000)
    assert retriever.key_field_for_node_type("Organization", basic_schema) is None


def test_key_field_for_node_type_unknown_type_returns_none(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(connection, basic_schema, shard_size=1000)
    assert retriever.key_field_for_node_type("Ghost", basic_schema) is None


def test_key_field_for_relationship_type_with_latest_hours(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        connection, basic_schema, shard_size=1000, latest_hours=6
    )
    assert (
        retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema)
        == LAST_INGESTED_AT_PROPERTY
    )


def test_key_field_for_relationship_type_no_latest_hours(basic_schema, mocker):
    connection = mocker.Mock()
    retriever = Neo4jTypeRetriever(connection, basic_schema, shard_size=1000)
    assert (
        retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema)
        is None
    )


# -- fetch_extractors --------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_extractors_relationships_only_by_default(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema, shard_size=1000)
    retriever.histogram = TypeHistogram(
        node_counts={"Person": 0, "Organization": 0},
        relationship_counts={"BEST_FRIEND_OF": 2500},
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    # BEST_FRIEND_OF: 3 shards (2500 / 1000), 1 adjacency
    assert_that(extractors, has_length(3))
    for extractor in extractors:
        assert isinstance(extractor, Neo4jRelationshipExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_preload_nodes_yields_nodes_then_relationships(
    mocker, basic_schema
):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, basic_schema, shard_size=1000, preload_nodes=True
    )
    retriever.histogram = TypeHistogram(
        node_counts={"Person": 1000, "Organization": 1000},
        relationship_counts={"BEST_FRIEND_OF": 1000},
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    node_extractors = [e for e in extractors if isinstance(e, Neo4jNodeExtractor)]
    relationship_extractors = [
        e for e in extractors if isinstance(e, Neo4jRelationshipExtractor)
    ]
    # 2 node types * 1 shard each = 2, then 1 rel type * 1 adjacency * 1 shard = 1
    assert_that(node_extractors, has_length(2))
    assert_that(relationship_extractors, has_length(1))
    # Nodes come first
    assert isinstance(extractors[0], Neo4jNodeExtractor)


@pytest.mark.asyncio
async def test_fetch_extractors_skips_rel_type_with_no_adjacencies(mocker):
    conn = mocker.Mock()
    schema = Schema()
    orphan_relationship = GraphObjectSchema(name="ORPHAN_REL", properties={})
    schema.put_relationship_type(orphan_relationship)
    retriever = Neo4jTypeRetriever(conn, schema, shard_size=1000)
    retriever.histogram = TypeHistogram(
        node_counts={},
        relationship_counts={"ORPHAN_REL": 500},
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    assert extractors == []


@pytest.mark.asyncio
async def test_fetch_extractors_skips_types_with_zero_count(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, basic_schema, shard_size=1000)
    retriever.histogram = TypeHistogram(
        node_counts={"Person": 0, "Organization": 0},
        relationship_counts={"BEST_FRIEND_OF": 0},
    )
    extractors = [e async for e in retriever.fetch_extractors()]
    assert extractors == []


# -- Neo4jNodeExtractor / Neo4jRelationshipExtractor extract_records


@pytest.mark.asyncio
async def test_node_extractor_extract_records(mocker, basic_schema):
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    fakeNode = FakeNeo4jNode(("Person",), {"name": "Bob"})
    connection.execute = AsyncMock(return_value=[{"n": fakeNode}])
    extractor = Neo4jNodeExtractor(
        connection=connection,
        statement="MATCH (n:Person) SKIP $shard_offset LIMIT $shard_limit RETURN n",
        params={"shard_offset": 0, "shard_limit": 10},
        node_type="Person",
        schema=basic_schema,
    )
    results = [record async for record in extractor.extract_records()]
    assert_that(results, has_length(1))
    call_kwargs = connection.execute.call_args
    assert call_kwargs.kwargs.get("routing_") == RoutingControl.READ


@pytest.mark.asyncio
async def test_node_extractor_skips_record_for_unknown_type(mocker, basic_schema):
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    fakeNode = FakeNeo4jNode(("UnknownType",), {"x": 1})
    connection.execute = AsyncMock(return_value=[{"n": fakeNode}])
    extractor = Neo4jNodeExtractor(
        connection=connection,
        statement="MATCH (n:UnknownType) RETURN n",
        params={"shard_offset": 0, "shard_limit": 10},
        node_type="UnknownType",
        schema=basic_schema,
    )
    results = [record async for record in extractor.extract_records()]
    assert_that(results, has_length(0))


@pytest.mark.asyncio
async def test_relationship_extractor_extract_records(mocker, basic_schema):
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    fakeFromNode = FakeNeo4jNode(("Person",), {"name": "Alice"})
    fakeToNode = FakeNeo4jNode(("Person",), {"name": "Bob"})
    fakeRel = FakeNeo4jRel("BEST_FRIEND_OF", {"since": 2020})
    connection.execute = AsyncMock(
        return_value=[{"a": fakeFromNode, "r": fakeRel, "b": fakeToNode}]
    )
    extractor = Neo4jRelationshipExtractor(
        connection=connection,
        statement="MATCH (a:Person)-[r:BEST_FRIEND_OF]->(b:Person) SKIP $shard_offset LIMIT $shard_limit RETURN a, r, b",
        params={"shard_offset": 0, "shard_limit": 10},
        from_node_type="Person",
        to_node_type="Person",
        relationship_type="BEST_FRIEND_OF",
        schema=basic_schema,
    )
    results = [record async for record in extractor.extract_records()]
    assert_that(results, has_length(1))
    call_kwargs = connection.execute.call_args
    assert call_kwargs.kwargs.get("routing_") == RoutingControl.READ


@pytest.mark.asyncio
async def test_relationship_extractor_skips_record_when_endpoint_type_unknown(
    mocker, basic_schema
):
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    fakeFromNode = FakeNeo4jNode(("Person",), {"name": "Alice"})
    fakeToNode = FakeNeo4jNode(("UnknownType",), {"x": 1})
    fakeRel = FakeNeo4jRel("BEST_FRIEND_OF", {"since": 2020})
    connection.execute = AsyncMock(
        return_value=[{"a": fakeFromNode, "r": fakeRel, "b": fakeToNode}]
    )
    extractor = Neo4jRelationshipExtractor(
        connection=connection,
        statement="MATCH (a:Person)-[r:BEST_FRIEND_OF]->(b:UnknownType) RETURN a, r, b",
        params={"shard_offset": 0, "shard_limit": 10},
        from_node_type="Person",
        to_node_type="UnknownType",
        relationship_type="BEST_FRIEND_OF",
        schema=basic_schema,
    )
    results = [record async for record in extractor.extract_records()]
    assert_that(results, has_length(0))


# -- RoundRobinDistribution --------------------------------------------------


@pytest.mark.asyncio
async def test_round_robin_distribution_interleaves_types(mocker, basic_schema):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(
        conn,
        basic_schema,
        shard_size=1000,
        preload_nodes=True,
        distribution="round_robin",
    )
    retriever.histogram = TypeHistogram(
        node_counts={"Person": 2000, "Organization": 1000},
        relationship_counts={"BEST_FRIEND_OF": 0},
    )
    nodeExtractors = [e async for e in retriever.fetch_node_extractors()]
    # Person: 2 shards, Organization: 1 shard → 3 total, interleaved
    assert_that(nodeExtractors, has_length(3))


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
    retriever = Neo4jTypeRetriever(conn, basic_schema, shard_size=1000)
    retriever.preview_node_count = AsyncMock(return_value=42)
    retriever.preview_relationship_count = AsyncMock(return_value=99)
    histogram = await retriever.build_histogram()
    assert histogram.node_counts == {"Person": 42, "Organization": 42}
    assert histogram.relationship_counts == {"BEST_FRIEND_OF": 99}
    # histogram is also stored on the retriever for use by fetch_extractors
    assert retriever.histogram is histogram


# -- snapshotCutoff ----------------------------------------------------------


def test_snapshot_cutoff_with_latest_hours(mocker):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(conn, Schema(), shard_size=1000, latest_hours=6)
    cutoff = retriever.snapshot_cutoff()
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    delta = now - cutoff
    # Should be ~6 hours (allow 5s drift)
    assert abs(delta.total_seconds() - 6 * 3600) < 5


def test_snapshot_cutoff_without_latest_hours_returns_now(mocker):
    conn = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(conn, Schema(), shard_size=1000)
    cutoff = retriever.snapshot_cutoff()
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    # Should be within 1 second of now
    assert abs((now - cutoff).total_seconds()) < 1
