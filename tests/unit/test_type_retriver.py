from typing import cast as type_cast

import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j import Record, RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
from nodestream.schema.state import (
    Adjacency,
    AdjacencyCardinality,
    Cardinality,
    GraphObjectSchema,
    PropertyMetadata,
    PropertyType,
    Schema,
)

from nodestream_plugin_neo4j.extractor import Neo4jRecordWrapper
from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.type_retriever import (
    LAST_INGESTED_AT_PROPERTY,
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
def subject(mocker):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection)


@pytest.fixture
def filtered_subject(mocker):
    """A retriever with both sampling and recency filters enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection, limit=500, sample_ratio=5, latest_hours=24)


@pytest.fixture
def sampled_subject(mocker):
    """A retriever with only sampling enabled."""
    connection = mocker.Mock(Neo4jDatabaseConnection)
    return Neo4jTypeRetriever(connection, sample_ratio=3)


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
        additional_types=["Employee"],
    )


def test_map_neo4j_relationship_to_nodestream_relationship(subject):
    rel = FakeNeo4jRel("KNOWS", {"since": 2019})
    result = subject.map_neo4j_relationship_to_nodestream_relationship(
        type_cast(Neo4jRelationship, rel), relationship_type="KNOWS"
    )
    assert result == Relationship(type="KNOWS", properties=PropertySet({"since": 2019}))


# -- Extractor construction (no filters) ------------------------------------


def test_get_node_type_extractor(subject):
    extractor = subject.get_node_type_extractor("Person")
    assert_that(
        extractor.query,
        equal_to("MATCH (n:Person)\nRETURN n SKIP $offset LIMIT $limit\n"),
    )


def test_get_relationships_of_type_between_extractor(subject):
    extractor = subject.get_relationships_of_type_between_extractor(
        "Person", "Company", "KNOWS"
    )
    assert_that(
        extractor.query,
        equal_to(
            "MATCH (a:Person)-[r:KNOWS]->(b:Company)\nRETURN a, r, b SKIP $offset LIMIT $limit\n"
        ),
    )


# -- Where-clause / filter tests --------------------------------------------


def test_where_clause_empty_when_no_filters(subject):
    assert_that(subject.build_where_clause("n"), equal_to(""))


def test_where_clause_with_sample_ratio(sampled_subject):
    # Sampling is only applied to relationship variables (sample=True).
    assert_that(
        sampled_subject.build_where_clause("r", sample=True),
        equal_to("WHERE toInteger(split(elementId(r), ':')[-1]) % 3 = 0\n"),
    )


def test_where_clause_sample_not_applied_to_nodes(sampled_subject):
    # Nodes are never sampled — build_where_clause without sample=True produces no clause.
    assert_that(sampled_subject.build_where_clause("n"), equal_to(""))


def test_where_clause_with_latest_hours(mocker):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    retriever = Neo4jTypeRetriever(connection, latest_hours=12)
    assert_that(
        retriever.build_where_clause("r"),
        equal_to("WHERE r.`last_ingested_at` >= $cutoff\n"),
    )


def test_where_clause_with_both_filters(filtered_subject):
    # When sample=True and latest_hours are both set, both clauses appear.
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
    retriever = Neo4jTypeRetriever(connection, sample_ratio=1)
    assert retriever.sample_ratio is None
    assert_that(retriever.build_where_clause("n"), equal_to(""))


# -- Extractor construction (with filters) ----------------------------------


def test_get_node_type_extractor_with_filters(filtered_subject):
    extractor = filtered_subject.get_node_type_extractor("Person")
    assert "WHERE" in extractor.query
    assert_that(extractor.limit, equal_to(500))
    assert "cutoff" in extractor.parameters


def test_get_relationships_extractor_with_filters(filtered_subject):
    extractor = filtered_subject.get_relationships_of_type_between_extractor(
        "Person", "Company", "KNOWS"
    )
    assert "WHERE" in extractor.query
    assert_that(extractor.limit, equal_to(500))
    assert "cutoff" in extractor.parameters


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
    """Count query should include the WHERE clause when filters are active."""
    filtered_subject.database_connection.execute.return_value = [{"count": 10}]
    count = await filtered_subject.preview_node_count("Person")
    assert_that(count, equal_to(10))
    query_arg = filtered_subject.database_connection.execute.call_args.args[0]
    assert "WHERE" in query_arg.query_statement
    assert "cutoff" in query_arg.parameters


# -- get_nodes_of_type / get_relationships_of_type_between ------------------


@pytest.mark.asyncio
async def test_get_nodes_of_type(subject, mocker):
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock()
    subject.get_node_type_extractor = mocker.Mock()
    extractor = subject.get_node_type_extractor.return_value
    n1 = FakeNeo4jNode(("Person",), {"id": 1, "name": "p1"})
    n2 = FakeNeo4jNode(("Person", "Employee"), {"id": 2, "name": "p2"})
    extractor.extract_records.return_value = async_generator(
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"n": n1}))),
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"n": n2}))),
    )
    results = [r async for r in subject.get_nodes_of_type("Person")]
    assert_that(results, has_length(2))
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(
        n1, node_type="Person", schema=None
    )
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(
        n2, node_type="Person", schema=None
    )


@pytest.mark.asyncio
async def test_get_relationships_of_type_between(subject, mocker):
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock()
    subject.map_neo4j_relationship_to_nodestream_relationship = mocker.Mock()
    subject.get_relationships_of_type_between_extractor = mocker.Mock()
    extractor = subject.get_relationships_of_type_between_extractor.return_value
    a1 = FakeNeo4jNode(("Person",), {"id": 1})
    b1 = FakeNeo4jNode(("Company",), {"id": 10})
    r1 = FakeNeo4jRel("KNOWS", {"since": 2019})
    a2 = FakeNeo4jNode(("Person",), {"id": 2})
    b2 = FakeNeo4jNode(("Company",), {"id": 11})
    r2 = FakeNeo4jRel("KNOWS", {"since": 2020})
    extractor.extract_records.return_value = async_generator(
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"a": a1, "b": b1, "r": r1}))),
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"a": a2, "b": b2, "r": r2}))),
    )
    results = [
        r
        async for r in subject.get_relationships_of_type_between(
            "Person", "Company", "KNOWS"
        )
    ]
    assert_that(results, has_length(2))
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(
        a1, node_type="Person", schema=None
    )
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(
        b1, node_type="Company", schema=None
    )
    subject.map_neo4j_relationship_to_nodestream_relationship.assert_any_call(
        r1, relationship_type="KNOWS"
    )


# -- compute_shards ----------------------------------------------------------


def test_compute_shards_zero_count_returns_empty(subject):
    assert subject.compute_shards(0, 1000) == []


def test_compute_shards_negative_shard_size_returns_empty(subject):
    assert subject.compute_shards(5000, 0) == []


# -- Schema fixtures ---------------------------------------------------------


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


# -- map_neo4j_node_to_nodestream_node with schema (key extraction) ----------


def test_map_neo4j_node_to_nodestream_node_with_schema_extracts_keys(
    subject, basic_schema
):
    neo_node = FakeNeo4jNode(("Person",), {"name": "Alice", "age": 30})
    result = subject.map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Person", schema=basic_schema
    )
    # "name" is the key field — it should be in key_values, not properties
    assert "name" in result.key_values
    assert result.key_values["name"] == "Alice"
    assert "name" not in result.properties


def test_map_neo4j_node_to_nodestream_node_schema_unknown_type(subject, basic_schema):
    """When node_type has no entry in schema, behaves like schema=None."""
    neo_node = FakeNeo4jNode(("Unknown",), {"x": 1})
    result = subject.map_neo4j_node_to_nodestream_node(
        type_cast(Neo4jNode, neo_node), node_type="Unknown", schema=basic_schema
    )
    assert result.type == "Unknown"
    assert result.properties["x"] == 1
    assert len(result.key_values) == 0


# -- get_node_type_shard_extractor -------------------------------------------


def test_get_node_type_shard_extractor_with_key_field(subject):
    extractor = subject.get_node_type_shard_extractor(
        "Person", "name", shard_offset=0, shard_limit=1000
    )
    assert "ORDER BY n.`name`" in extractor.query
    assert extractor.parameters["shard_offset"] == 0
    assert extractor.parameters["shard_limit"] == 1000


def test_get_node_type_shard_extractor_without_key_field(subject):
    extractor = subject.get_node_type_shard_extractor(
        "Person", None, shard_offset=500, shard_limit=500
    )
    assert "ORDER BY elementId(n)" in extractor.query
    assert extractor.parameters["shard_offset"] == 500
    assert extractor.parameters["shard_limit"] == 500


# -- get_relationships_of_type_between_shard_extractor -----------------------


def test_get_relationships_shard_extractor_with_key_field(subject):
    extractor = subject.get_relationships_of_type_between_shard_extractor(
        "Person", "Person", "BEST_FRIEND_OF", "since", shard_offset=0, shard_limit=2000
    )
    assert "ORDER BY r.`since`" in extractor.query
    assert extractor.parameters["shard_offset"] == 0
    assert extractor.parameters["shard_limit"] == 2000


def test_get_relationships_shard_extractor_without_key_field(subject):
    extractor = subject.get_relationships_of_type_between_shard_extractor(
        "Person", "Person", "BEST_FRIEND_OF", None, shard_offset=100, shard_limit=900
    )
    assert "ORDER BY elementId(r)" in extractor.query
    assert extractor.parameters["shard_offset"] == 100
    assert extractor.parameters["shard_limit"] == 900


# -- get_nodes_of_type_shard -------------------------------------------------


@pytest.mark.asyncio
async def test_get_nodes_of_type_shard(subject, mocker):
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock(
        return_value=Node(type="Person", properties=PropertySet({"name": "p1"}))
    )
    subject.get_node_type_shard_extractor = mocker.Mock()
    extractor = subject.get_node_type_shard_extractor.return_value
    n1 = FakeNeo4jNode(("Person",), {"name": "p1"})
    extractor.extract_records.return_value = async_generator(
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"n": n1}))),
    )
    results = [
        r async for r in subject.get_nodes_of_type_shard("Person", "name", 0, 1000)
    ]
    assert_that(results, has_length(1))
    subject.get_node_type_shard_extractor.assert_called_once_with(
        "Person", "name", 0, 1000, cutoff=None
    )


# -- get_relationships_of_type_between_shard ---------------------------------


@pytest.mark.asyncio
async def test_get_relationships_of_type_between_shard(subject, mocker):
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock(
        side_effect=lambda n, node_type, schema: Node(
            type=node_type, properties=PropertySet(dict(n))
        )
    )
    subject.map_neo4j_relationship_to_nodestream_relationship = mocker.Mock(
        return_value=Relationship(type="BEST_FRIEND_OF", properties=PropertySet({}))
    )
    subject.get_relationships_of_type_between_shard_extractor = mocker.Mock()
    extractor = subject.get_relationships_of_type_between_shard_extractor.return_value
    a1 = FakeNeo4jNode(("Person",), {"id": 1})
    b1 = FakeNeo4jNode(("Person",), {"id": 2})
    r1 = FakeNeo4jRel("BEST_FRIEND_OF", {})
    extractor.extract_records.return_value = async_generator(
        Neo4jRecordWrapper(type_cast(Record, FakeRecord({"a": a1, "b": b1, "r": r1}))),
    )
    results = [
        r
        async for r in subject.get_relationships_of_type_between_shard(
            "Person", "Person", "BEST_FRIEND_OF", "since", 0, 1000
        )
    ]
    assert_that(results, has_length(1))
    assert isinstance(results[0], RelationshipWithNodes)
    subject.get_relationships_of_type_between_shard_extractor.assert_called_once_with(
        "Person", "Person", "BEST_FRIEND_OF", "since", 0, 1000, cutoff=None
    )


# -- key_field_for_node_type -------------------------------------------------


def test_key_field_for_node_type_with_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, latest_hours=24)
    assert retriever.key_field_for_node_type("Person", basic_schema) == LAST_INGESTED_AT_PROPERTY


def test_key_field_for_node_type_from_schema_keys(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn)  # no latest_hours
    key = retriever.key_field_for_node_type("Person", basic_schema)
    # Person has "name" as key field
    assert key == "name"


def test_key_field_for_node_type_no_schema_keys(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn)
    # Organization has no key fields in our basic_schema fixture
    key = retriever.key_field_for_node_type("Organization", basic_schema)
    assert key is None


def test_key_field_for_node_type_unknown_type(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn)
    assert retriever.key_field_for_node_type("Ghost", basic_schema) is None


# -- key_field_for_relationship_type -----------------------------------------


def test_key_field_for_relationship_type_with_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn, latest_hours=6)
    assert (
        retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema)
        == LAST_INGESTED_AT_PROPERTY
    )


def test_key_field_for_relationship_type_no_latest_hours(basic_schema):
    import unittest.mock as mock

    conn = mock.Mock()
    retriever = Neo4jTypeRetriever(conn)
    assert retriever.key_field_for_relationship_type("BEST_FRIEND_OF", basic_schema) is None


# -- plan_relationship_fetches -----------------------------------------------


@pytest.mark.asyncio
async def test_plan_relationship_fetches_no_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, relationship_types=["BEST_FRIEND_OF"]
    )
    specs = await retriever.plan_relationship_fetches(basic_schema)
    # BEST_FRIEND_OF has one adjacency (Person->Person), no sharding => one spec
    assert len(specs) == 1
    rel_type, adj, cutoff, shard_offset, shard_limit, key_field = specs[0]
    assert rel_type == "BEST_FRIEND_OF"
    assert adj.from_node_type == "Person"
    assert adj.to_node_type == "Person"
    assert shard_offset is None
    assert shard_limit is None
    assert key_field is None


@pytest.mark.asyncio
async def test_plan_relationship_fetches_with_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, relationship_types=["BEST_FRIEND_OF"], shard_size=1000
    )
    # count_relationship_type needs the async execute mock
    retriever.preview_relationship_count = mocker.AsyncMock(return_value=2500)
    specs = await retriever.plan_relationship_fetches(basic_schema)
    # 2500 / 1000 = 3 shards, 1 adjacency => 3 specs
    assert len(specs) == 3
    offsets = [s[3] for s in specs]
    assert offsets == [0, 1000, 2000]
    limits = [s[4] for s in specs]
    assert limits == [1000, 1000, 500]


@pytest.mark.asyncio
async def test_plan_relationship_fetches_skips_type_with_no_adjacencies(
    mocker, basic_schema
):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, relationship_types=["UNKNOWN_REL"]
    )
    specs = await retriever.plan_relationship_fetches(basic_schema)
    assert specs == []


# -- fetch_nodes -------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_nodes_relationships_only_skips_all(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, node_types=["Person"], relationships_only=True)
    results = [r async for r in retriever.fetch_nodes(basic_schema)]
    assert results == []


@pytest.mark.asyncio
async def test_fetch_nodes_no_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, node_types=["Person"])
    node = Node(type="Person", properties=PropertySet({"name": "Alice"}))
    retriever.get_nodes_of_type = mocker.Mock(return_value=async_generator(node))
    results = [r async for r in retriever.fetch_nodes(basic_schema)]
    assert results == [node]
    retriever.get_nodes_of_type.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_nodes_with_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, node_types=["Person"], shard_size=1000)
    retriever.preview_node_count = mocker.AsyncMock(return_value=2000)
    node1 = Node(type="Person", properties=PropertySet({"name": "Alice"}))
    node2 = Node(type="Person", properties=PropertySet({"name": "Bob"}))
    retriever.get_nodes_of_type_shard = mocker.Mock(
        side_effect=[async_generator(node1), async_generator(node2)]
    )
    results = [r async for r in retriever.fetch_nodes(basic_schema)]
    assert results == [node1, node2]
    # 2000 / 1000 = 2 shards
    assert retriever.get_nodes_of_type_shard.call_count == 2


# -- fetch_relationships -----------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_relationships_empty_when_no_specs(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(conn, relationship_types=["UNKNOWN_REL"])
    results = [r async for r in retriever.fetch_relationships(basic_schema)]
    assert results == []


@pytest.mark.asyncio
async def test_fetch_relationships_yields_items(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn, relationship_types=["BEST_FRIEND_OF"], concurrency_limit=2
    )
    rwn = RelationshipWithNodes(
        from_node=Node(type="Person", properties=PropertySet({})),
        to_node=Node(type="Person", properties=PropertySet({})),
        relationship=Relationship(type="BEST_FRIEND_OF", properties=PropertySet({})),
    )
    retriever.get_relationships_of_type_between = mocker.Mock(
        return_value=async_generator(rwn)
    )
    retriever.preview_relationship_count = mocker.AsyncMock(return_value=1)
    results = [r async for r in retriever.fetch_relationships(basic_schema)]
    assert len(results) == 1
    assert results[0] == rwn


@pytest.mark.asyncio
async def test_fetch_relationships_with_sharding(mocker, basic_schema):
    conn = mocker.Mock()
    retriever = Neo4jTypeRetriever(
        conn,
        relationship_types=["BEST_FRIEND_OF"],
        shard_size=500,
        concurrency_limit=2,
    )
    retriever.preview_relationship_count = mocker.AsyncMock(return_value=1000)
    rwn1 = RelationshipWithNodes(
        from_node=Node(type="Person", properties=PropertySet({})),
        to_node=Node(type="Person", properties=PropertySet({})),
        relationship=Relationship(type="BEST_FRIEND_OF", properties=PropertySet({})),
    )
    rwn2 = RelationshipWithNodes(
        from_node=Node(type="Person", properties=PropertySet({})),
        to_node=Node(type="Person", properties=PropertySet({})),
        relationship=Relationship(type="BEST_FRIEND_OF", properties=PropertySet({})),
    )
    retriever.get_relationships_of_type_between_shard = mocker.Mock(
        side_effect=[async_generator(rwn1), async_generator(rwn2)]
    )
    results = [r async for r in retriever.fetch_relationships(basic_schema)]
    assert len(results) == 2
    assert retriever.get_relationships_of_type_between_shard.call_count == 2
