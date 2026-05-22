from typing import cast as type_cast

import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j import Record, RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.model import Node, PropertySet, Relationship

from nodestream_plugin_neo4j.extractor import Neo4jRecordWrapper
from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.type_retriever import Neo4jTypeRetriever


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
        equal_to(
            "WHERE r.`last_ingested_at` >= $cutoff\n"
        ),
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
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(n1, node_type="Person", schema=None)
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(n2, node_type="Person", schema=None)


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
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(a1, node_type="Person", schema=None)
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(b1, node_type="Company", schema=None)
    subject.map_neo4j_relationship_to_nodestream_relationship.assert_any_call(
        r1, relationship_type="KNOWS"
    )
