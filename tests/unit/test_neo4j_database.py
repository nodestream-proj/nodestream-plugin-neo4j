import pytest
from hamcrest import assert_that, equal_to
from neo4j import AsyncDriver, RoutingControl
from neo4j.exceptions import TransientError
from nodestream.file_io import LazyLoadedArgument

from nodestream_plugin_neo4j.neo4j_database import (
    Neo4jDatabaseConnection,
    auth_provider_factory,
)
from nodestream_plugin_neo4j.query import Query

A_QUERY = Query("MATCH (n) RETURN n LIMIT $limit", {"limit": 2})
SOME_RECORDS = [
    {"n": {"name": "foo"}},
    {"n": {"name": "bar"}},
]


@pytest.fixture
def database_connection(mocker):
    return Neo4jDatabaseConnection(
        lambda: mocker.AsyncMock(AsyncDriver), "neo4j", 2, 0.1
    )


@pytest.mark.asyncio
async def test_execute(database_connection, mocker):
    # Mock driver result with required attributes
    driver_result = database_connection.driver.execute_query.return_value
    driver_result.records = SOME_RECORDS
    driver_result.keys = ["n"]
    # Summary with timings and empty counters
    summary = mocker.Mock()
    summary.result_available_after = 0
    summary.result_consumed_after = 0
    counters = mocker.Mock()
    counters.nodes_created = 0
    counters.nodes_deleted = 0
    counters.relationships_created = 0
    counters.relationships_deleted = 0
    counters.properties_set = 0
    counters.labels_added = 0
    counters.labels_removed = 0
    counters.constraints_added = 0
    counters.constraints_removed = 0
    counters.indexes_added = 0
    counters.indexes_removed = 0
    summary.counters = counters
    driver_result.summary = summary
    result = await database_connection.execute(A_QUERY, log_result=True)
    assert_that(result, equal_to(SOME_RECORDS))
    database_connection.driver.execute_query.assert_called_once_with(
        A_QUERY.query_statement,
        A_QUERY.parameters,
        database_="neo4j",
        routing_=RoutingControl.WRITE,
    )


@pytest.mark.asyncio
async def test_execute_fail_and_then_succeed(database_connection, mocker):
    database_connection.acquire_driver = mocker.Mock(
        wraps=database_connection.acquire_driver
    )
    # First call fails, second call returns a proper driver result
    driver_result = mocker.Mock()
    driver_result.records = SOME_RECORDS
    driver_result.keys = ["n"]
    summary = mocker.Mock()
    summary.result_available_after = 0
    summary.result_consumed_after = 0
    counters = mocker.Mock()
    counters.nodes_created = 0
    counters.nodes_deleted = 0
    counters.relationships_created = 0
    counters.relationships_deleted = 0
    counters.properties_set = 0
    counters.labels_added = 0
    counters.labels_removed = 0
    counters.constraints_added = 0
    counters.constraints_removed = 0
    counters.indexes_added = 0
    counters.indexes_removed = 0
    summary.counters = counters
    driver_result.summary = summary
    database_connection.driver.execute_query.side_effect = [
        TransientError("Failed to execute query"),
        driver_result,
    ]
    await database_connection.execute(A_QUERY)
    assert_that(database_connection.acquire_driver.call_count, equal_to(2))


@pytest.mark.asyncio
async def test_execute_fail_and_then_fail(database_connection, mocker):
    def driver_factory():
        driver = mocker.AsyncMock(AsyncDriver)
        driver.execute_query.side_effect = TransientError("Failed to execute query")
        return driver

    database_connection.driver_factory = driver_factory
    with pytest.raises(TransientError):
        await database_connection.execute(A_QUERY)


@pytest.mark.asyncio
async def test_session(database_connection):
    session = database_connection.session()
    assert_that(session, equal_to(database_connection.driver.session.return_value))
    database_connection.driver.session.assert_called_once_with(database="neo4j")


@pytest.mark.asyncio
async def test_auth_provider_factory_with_dynamic_values(mocker):
    username = mocker.Mock(LazyLoadedArgument)
    password = mocker.Mock(LazyLoadedArgument)
    provider = auth_provider_factory(username, password)
    retrieved_username, retrieved_password = await provider()
    assert_that(retrieved_username, equal_to(username.get_value.return_value))
    assert_that(retrieved_password, equal_to(password.get_value.return_value))


@pytest.mark.asyncio
async def test_auth_provider_factory_with_static_values():
    username = "neo4j"
    password = "password"
    provider = auth_provider_factory(username, password)
    retrieved_username, retrieved_password = await provider()
    assert_that(retrieved_username, equal_to(username))
    assert_that(retrieved_password, equal_to(password))


@pytest.mark.asyncio
async def test_execute_implicit_uses_session_run(database_connection, mocker):
    # Build a query that should run implicitly
    implicit_query = Query.from_statement(
        "MATCH (n) RETURN n LIMIT $limit", is_implicit=True, limit=2
    )

    # Prepare an async result compatible with "async for"
    async_result = mocker.MagicMock()
    # Return a plain iterable; AsyncMock wrapper will convert to async iterator
    async_result.__aiter__.return_value = SOME_RECORDS
    async_result.keys.return_value = ["n"]
    # Provide a minimal summary mock
    summary = mocker.Mock()
    summary.result_available_after = 0
    summary.result_consumed_after = 0
    counters = mocker.Mock()
    counters.nodes_created = 0
    counters.nodes_deleted = 0
    counters.relationships_created = 0
    counters.relationships_deleted = 0
    counters.properties_set = 0
    counters.labels_added = 0
    counters.labels_removed = 0
    counters.constraints_added = 0
    counters.constraints_removed = 0
    counters.indexes_added = 0
    counters.indexes_removed = 0
    summary.counters = counters
    async_result.consume = mocker.AsyncMock(return_value=summary)

    # Mock session context manager and run()
    session_cm = mocker.AsyncMock()
    session_cm.__aenter__.return_value = session_cm
    session_cm.__aexit__.return_value = False
    session_cm.run = mocker.AsyncMock(return_value=async_result)
    database_connection.driver.session.return_value = session_cm

    # Execute
    result = await database_connection.execute(implicit_query, log_result=True)

    # Assert records collected and driver.execute_query not used
    assert_that(result, equal_to(SOME_RECORDS))
    database_connection.driver.execute_query.assert_not_called()
    database_connection.driver.session.assert_called_once()
    session_cm.run.assert_called_once_with(
        implicit_query.query_statement, parameters=implicit_query.parameters
    )
