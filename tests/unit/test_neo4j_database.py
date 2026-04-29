import asyncio

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
def mock_driver(mocker):
    return mocker.AsyncMock(AsyncDriver)


@pytest.fixture
def database_connection(mock_driver):
    db = Neo4jDatabaseConnection(lambda: mock_driver, "neo4j", 2, 0.1)
    db._driver = mock_driver
    return db


@pytest.mark.asyncio
async def test_execute(database_connection, mock_driver, mocker):
    # Mock driver result with required attributes
    driver_result = mock_driver.execute_query.return_value
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
    mock_driver.execute_query.assert_called_once_with(
        A_QUERY.query_statement,
        A_QUERY.parameters,
        database_="neo4j",
        routing_=RoutingControl.WRITE,
    )


@pytest.mark.asyncio
async def test_execute_fail_and_then_succeed(database_connection, mocker):
    # First driver raises a transient error; second driver succeeds.
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

    failing_driver = mocker.AsyncMock(AsyncDriver)
    failing_driver.execute_query.side_effect = TransientError("Failed to execute query")
    succeeding_driver = mocker.AsyncMock(AsyncDriver)
    succeeding_driver.execute_query.return_value = driver_result

    call_count = 0

    def driver_factory():
        nonlocal call_count
        call_count += 1
        return failing_driver if call_count == 1 else succeeding_driver

    database_connection._driver = None
    database_connection.driver_factory = driver_factory
    await database_connection.execute(A_QUERY)
    assert_that(call_count, equal_to(2))


@pytest.mark.asyncio
async def test_execute_fail_and_then_fail(database_connection, mocker):
    def driver_factory():
        driver = mocker.AsyncMock(AsyncDriver)
        driver.execute_query.side_effect = TransientError("Failed to execute query")
        return driver

    database_connection._driver = None
    database_connection.driver_factory = driver_factory
    with pytest.raises(TransientError):
        await database_connection.execute(A_QUERY)


@pytest.mark.asyncio
async def test_concurrent_rotation_does_not_close_fresh_driver(mocker):
    """Two concurrent failing queries must not double-rotate the driver.

    Before the fix, query A and query B would both observe the failing
    initial driver, both raise, both call ``_rotate_driver``. A's rotation
    would build a fresh replacement driver; B's rotation would then close
    that just-built driver, potentially breaking an in-flight query on it
    and creating yet another rotation. The fix uses a stale_driver guard:
    when B reaches ``_rotate_driver`` it sees that ``self._driver`` no
    longer matches the driver B's failing query was using, so B skips the
    rotation and retries against A's replacement driver.

    This test asserts the post-fix invariant: exactly one rotation occurs,
    so the factory builds the initial driver plus one replacement (2 total),
    and only the failing initial driver is closed.
    """
    failing_driver = mocker.AsyncMock(AsyncDriver)

    async def fail_then_yield(*args, **kwargs):
        # Yield once before raising so both concurrent tasks reach the
        # failing call before either handles the exception. Without this
        # yield, asyncio may run one task to completion before the other
        # starts, which would mask the race.
        await asyncio.sleep(0)
        raise TransientError("simulated transient")

    failing_driver.execute_query = fail_then_yield

    succeeding_result = mocker.Mock()
    succeeding_result.records = []
    succeeding_result.keys = []
    summary = mocker.Mock()
    summary.result_available_after = 0
    summary.result_consumed_after = 0
    counters = mocker.Mock()
    for attr in (
        "nodes_created",
        "nodes_deleted",
        "relationships_created",
        "relationships_deleted",
        "properties_set",
        "labels_added",
        "labels_removed",
        "constraints_added",
        "constraints_removed",
        "indexes_added",
        "indexes_removed",
    ):
        setattr(counters, attr, 0)
    summary.counters = counters
    succeeding_result.summary = summary

    succeeding_driver = mocker.AsyncMock(AsyncDriver)
    succeeding_driver.execute_query.return_value = succeeding_result

    factory_call_count = 0

    def driver_factory():
        nonlocal factory_call_count
        factory_call_count += 1
        return failing_driver if factory_call_count == 1 else succeeding_driver

    db = Neo4jDatabaseConnection(
        driver_factory=driver_factory,
        database_name="neo4j",
        max_retry_attempts=3,
        retry_factor=0.001,
    )

    # Two concurrent queries against the same connection.
    results = await asyncio.gather(
        db.execute(A_QUERY),
        db.execute(A_QUERY),
        return_exceptions=True,
    )

    # Both queries succeed on retry.
    for r in results:
        assert not isinstance(r, BaseException), f"unexpected error: {r!r}"

    # Exactly one rotation: factory called twice (initial + one replacement).
    assert_that(factory_call_count, equal_to(2))
    # The failing driver was closed exactly once; the replacement was not closed.
    assert_that(failing_driver.close.call_count, equal_to(1))
    assert_that(succeeding_driver.close.call_count, equal_to(0))


@pytest.mark.asyncio
async def test_rotate_driver_skips_when_stale_driver_already_replaced(
    database_connection, mock_driver, mocker
):
    """Direct test for the stale_driver guard in _rotate_driver.

    When the caller's stale_driver no longer matches the connection's
    current driver, the rotation must be a no-op: no close, no factory
    call, no sleep.
    """
    # Connection currently holds mock_driver (set by the fixture).
    fresh_driver = database_connection._driver

    # Pretend the caller observed a failure on a different (older) driver.
    older_driver = mocker.AsyncMock(AsyncDriver)

    # Replace the factory with a tripwire — it must not be called.
    database_connection.driver_factory = mocker.Mock(
        side_effect=AssertionError("driver_factory should not be called")
    )

    await database_connection._rotate_driver(attempts=1, stale_driver=older_driver)

    # No close, no replacement, current driver unchanged.
    assert_that(fresh_driver.close.call_count, equal_to(0))
    assert_that(database_connection._driver is fresh_driver, equal_to(True))
    database_connection.driver_factory.assert_not_called()


@pytest.mark.asyncio
async def test_session(database_connection, mock_driver):
    session = await database_connection.session()
    assert_that(session, equal_to(mock_driver.session.return_value))
    mock_driver.session.assert_called_once_with(database="neo4j")


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
async def test_execute_implicit_uses_session_run(  # covers _run_implicit_query via execute()
    database_connection, mock_driver, mocker
):
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
    mock_driver.session.return_value = session_cm

    # Execute
    result = await database_connection.execute(implicit_query, log_result=True)

    # Assert records collected and driver.execute_query not used
    assert_that(result, equal_to(SOME_RECORDS))
    mock_driver.execute_query.assert_not_called()
    mock_driver.session.assert_called_once()
    session_cm.run.assert_called_once_with(
        implicit_query.query_statement, parameters=implicit_query.parameters
    )
