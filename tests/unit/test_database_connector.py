from typing import cast

from hamcrest import assert_that, equal_to, is_

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
    # Should be the concrete Neo4j implementation of QueryExecutor.
    assert_that(isinstance(executor, Neo4jQueryExecutor), is_(True))


def test_make_type_retriever_defaults(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever()
    neo4j_retriever = cast(Neo4jTypeRetriever, retriever)

    assert_that(
        neo4j_retriever.database_connection, equal_to(connector.database_connection)
    )
    assert_that(neo4j_retriever.limit, equal_to(1000))
    # By default, no sampling or recency filters are applied.
    assert_that(neo4j_retriever.sample_ratio, equal_to(None))
    assert_that(neo4j_retriever.latest_hours, equal_to(None))


def test_make_type_retriever_with_filters(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever(
        limit=500,
        sample_ratio=3,
        latest_hours=24,
    )
    neo4j_retriever = cast(Neo4jTypeRetriever, retriever)

    assert_that(
        neo4j_retriever.database_connection, equal_to(connector.database_connection)
    )
    assert_that(neo4j_retriever.limit, equal_to(500))
    assert_that(neo4j_retriever.sample_ratio, equal_to(3))
    assert_that(neo4j_retriever.latest_hours, equal_to(24))


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
