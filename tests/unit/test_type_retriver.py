from typing import cast as type_cast

import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j import Record
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
    """Mimics a neo4j Record for testing (supports .data() and __getitem__).

    .data() returns plain dicts (like the real driver), but __getitem__
    returns the original objects (FakeNeo4jNode / FakeNeo4jRel) so that
    record.original[key] preserves graph-object attributes like .labels.
    """

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


def test_get_node_type_extractor(subject):
    extractor = subject.get_node_type_extractor("Person")
    expected_query = """
MATCH (n:Person)
RETURN n SKIP $offset LIMIT $limit
"""
    assert_that(extractor.query, equal_to(expected_query))


def test_get_relationships_of_type_bettween_extractor(subject):
    extractor = subject.get_relationships_of_type_bettween_extractor(
        "Person", "Company", "KNOWS"
    )
    expected_query = """
MATCH (a:Person)-[r:KNOWS]->(b:Company)
RETURN a, r, b SKIP $offset LIMIT $limit
"""
    assert_that(extractor.query, equal_to(expected_query))


async def async_generator(*items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_get_nodes_of_type(subject, mocker):
    # Stub out the extractor to return specific values and
    # stub the conversion processes. This is a unit test to
    # test the loop itself not the entire process.
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
    # Verify the mapping function received the original node objects
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(n1, node_type="Person")
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(n2, node_type="Person")


@pytest.mark.asyncio
async def test_get_relationships_of_type_between(subject, mocker):
    # Stub out the extractor to return specific values and
    # stub the conversion processes. This is a unit test to
    # test the loop itself not the entire process.
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock()
    subject.map_neo4j_relationship_to_nodestream_relationship = mocker.Mock()
    subject.get_relationships_of_type_bettween_extractor = mocker.Mock()
    extractor = subject.get_relationships_of_type_bettween_extractor.return_value
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
    # Verify the mapping functions received the original objects
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(a1, node_type="Person")
    subject.map_neo4j_node_to_nodestream_node.assert_any_call(b1, node_type="Company")
    subject.map_neo4j_relationship_to_nodestream_relationship.assert_any_call(
        r1, relationship_type="KNOWS"
    )
