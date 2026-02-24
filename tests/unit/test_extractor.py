import json

import pytest
from hamcrest import assert_that, equal_to, has_length, is_
from neo4j import Record

from nodestream_plugin_neo4j.extractor import Neo4jExtractor, Neo4jRecordWrapper
from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.query import Query

from .matchers import ran_query


class FakeRecord(Record):
    def __init__(self, payload):
        self._payload = payload

    def data(self):
        return self._payload

    def keys(self):
        return list(self._payload.keys())

    def __getitem__(self, key):
        return self._payload[key]


@pytest.mark.asyncio
async def test_extract_records(mocker):
    query = "MATCH (n:{test: $test}) RETURN n.name as name"
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    extractor = Neo4jExtractor(
        query=query,
        database_connection=connection,
        parameters={"test": "test"},
        limit=2,
    )

    connection.execute.side_effect = [
        [FakeRecord({"name": "test1"}), FakeRecord({"name": "test2"})],
        [FakeRecord({"name": "test3"})],
        [],
    ]
    rows = [row async for row in extractor.extract_records()]
    assert_that(rows, has_length(3))
    # Should be our wrapper mapping view
    assert isinstance(rows[0], Neo4jRecordWrapper)
    names = [row["name"] for row in rows]
    assert_that(names, equal_to(["test1", "test2", "test3"]))
    # original should expose the underlying record with the same values
    assert_that(rows[0].original["name"], is_("test1"))

    expected_query = Query(query, {"test": "test", "limit": 2, "offset": 0})

    assert_that(extractor, ran_query(expected_query))


def test_neo4j_record_wrapper_is_json_serializable():
    record = FakeRecord({"name": "test", "value": 42})
    wrapper = Neo4jRecordWrapper(record)

    # Should be encodable by the standard library JSON encoder without
    # needing any special handling or custom encoder.
    encoded = json.dumps(wrapper)

    assert_that(json.loads(encoded), equal_to({"name": "test", "value": 42}))
