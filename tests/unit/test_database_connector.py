from hamcrest import assert_that, equal_to, is_
from nodestream.schema.state import Schema

from nodestream_plugin_neo4j import Neo4jDatabaseConnector
from nodestream_plugin_neo4j.query_executor import Neo4jQueryExecutor
from nodestream_plugin_neo4j.type_retriever import Neo4jTypeRetriever


def test_make_query_executor(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    executor = connector.make_query_executor()
    assert isinstance(executor, Neo4jQueryExecutor)
    assert_that(executor.database_connection, equal_to(connector.database_connection))
    assert_that(executor.ingest_query_builder.apoc_iterate, is_(True))


def test_make_type_retriever_defaults(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever(schema=Schema())
    assert isinstance(retriever, Neo4jTypeRetriever)
    assert_that(retriever.database_connection, equal_to(connector.database_connection))
    assert_that(retriever.shard_size, equal_to(10000))
    assert_that(retriever.sample_ratio, equal_to(None))
    assert_that(retriever.latest_hours, equal_to(None))


def test_make_type_retriever_with_filters(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever(
        schema=Schema(),
        shard_size=5000,
        sample_ratio=3,
        latest_hours=24,
    )
    assert isinstance(retriever, Neo4jTypeRetriever)
    assert_that(retriever.database_connection, equal_to(connector.database_connection))
    assert_that(retriever.shard_size, equal_to(5000))
    assert_that(retriever.sample_ratio, equal_to(3))
    assert_that(retriever.latest_hours, equal_to(24))


def test_make_type_retriever_with_relationships_only(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever(
        schema=Schema(),
        relationships_only=True,
        distribution="round_robin",
    )
    assert isinstance(retriever, Neo4jTypeRetriever)
    assert_that(retriever.relationships_only, equal_to(True))


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


def test_make_migrator(mocker):
    from nodestream_plugin_neo4j.migrator import Neo4jMigrator

    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    migrator = connector.make_migrator()
    assert isinstance(migrator, Neo4jMigrator)
    assert migrator.database_connection is connector.database_connection
    assert migrator.use_enterprise_features is True
