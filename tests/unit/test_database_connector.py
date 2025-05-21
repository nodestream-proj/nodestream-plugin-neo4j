import pytest
from unittest.mock import Mock
from hamcrest import assert_that, equal_to, instance_of

from nodestream.databases.ingest_monitor import IngestMonitor
from nodestream_plugin_neo4j import Neo4jDatabaseConnector
from nodestream_plugin_neo4j.neo4j_ingest_monitor import Neo4jIngestMonitor


@pytest.fixture
def mocker():
    return Mock()


def test_make_query_executor(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    executor = connector.make_query_executor()
    assert_that(executor.database_connection, equal_to(connector.database_connection))
    assert_that(executor.ingest_query_builder.apoc_iterate, equal_to(True))


def test_make_type_retriever(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever()
    assert_that(retriever.database_connection, equal_to(connector.database_connection))


def test_from_file_data_no_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=False,
        use_apoc=False,
    )
    assert_that(connector.use_enterprise_features, equal_to(False))


def test_from_file_data_with_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=True,
        use_apoc=False,
    )
    assert_that(connector.use_enterprise_features, equal_to(True))


def test_make_ingest_monitor():
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    monitor = connector.make_ingest_monitor()
    assert_that(monitor, instance_of(Neo4jIngestMonitor))
    assert_that(monitor._similarity_threshold, equal_to(0.8))


def test_make_ingest_monitor_with_custom_threshold():
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    monitor = connector.make_ingest_monitor(similarity_threshold=0.9)
    assert_that(monitor, instance_of(Neo4jIngestMonitor))
    assert_that(monitor._similarity_threshold, equal_to(0.9))


def test_make_query_executor_with_default_monitor():
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    executor = connector.make_query_executor()
    assert_that(executor.database_connection, equal_to(connector.database_connection))
    assert_that(executor.ingest_query_builder.apoc_iterate, equal_to(True))
    assert_that(executor.ingest_monitor, instance_of(Neo4jIngestMonitor))


def test_make_query_executor_with_custom_monitor():
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    custom_monitor = connector.make_ingest_monitor(similarity_threshold=0.9)
    executor = connector.make_query_executor(ingest_monitor=custom_monitor)
    assert_that(executor.database_connection, equal_to(connector.database_connection))
    assert_that(executor.ingest_query_builder.apoc_iterate, equal_to(True))
    assert_that(executor.ingest_monitor, equal_to(custom_monitor))
