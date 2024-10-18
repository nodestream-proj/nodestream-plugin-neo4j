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
async def test_execute(database_connection):
    database_connection.driver.execute_query.return_value.records = SOME_RECORDS
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
    database_connection.driver.execute_query.side_effect = [
        TransientError("Failed to execute query"),
        SOME_RECORDS,
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
