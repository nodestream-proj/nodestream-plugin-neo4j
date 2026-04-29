"""
Integration tests for Neo4jDatabaseConnection driver rotation behaviour.

These tests are outcome-driven: given a driver that raises a retryable error,
we assert that the bad driver is only called once (rotation replaces it) and
that concurrent callers do not re-use the bad driver while rotation is in
progress.

No lock internals are inspected — only observable call counts on the drivers.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from neo4j import AsyncDriver
from neo4j.exceptions import ClientError, ServiceUnavailable

from nodestream_plugin_neo4j.neo4j_database import (
    AUTH_RATE_LIMIT_CODE,
    Neo4jDatabaseConnection,
)
from nodestream_plugin_neo4j.query import Query

A_QUERY = Query("MATCH (n) RETURN n", {})
ANOTHER_QUERY = Query("MATCH (r) RETURN r", {})


def _make_driver_result(mocker) -> Mock:
    result = mocker.Mock()
    result.records = []
    result.keys = []
    summary = mocker.Mock()
    summary.result_available_after = 0
    summary.result_consumed_after = 0
    counters = mocker.Mock()
    for attr in [
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
    ]:
        setattr(counters, attr, 0)
    summary.counters = counters
    result.summary = summary
    return result


def make_bad_driver(mocker, error: Exception) -> AsyncMock:
    driver = mocker.AsyncMock(AsyncDriver)
    driver.execute_query.side_effect = error
    return driver


def make_good_driver(mocker) -> AsyncMock:
    driver = mocker.AsyncMock(AsyncDriver)
    driver.execute_query.return_value = _make_driver_result(mocker)
    return driver


def make_db(driver_sequence, retry_factor=0) -> Neo4jDatabaseConnection:
    drivers = iter(driver_sequence)
    return Neo4jDatabaseConnection(
        driver_factory=lambda: next(drivers),
        database_name="neo4j",
        max_retry_attempts=3,
        retry_factor=retry_factor,
    )


# ---------------------------------------------------------------------------
# Test 1 — bad driver is only called once; rotation produces a working driver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bad_driver_called_once_then_rotated(mocker):
    """
    When the first driver raises a retryable Neo4j error, execute() rotates
    to a fresh driver and retries.  The bad driver must be called exactly
    once — rotation must not cause it to be re-used.
    """
    bad_driver = make_bad_driver(mocker, ServiceUnavailable("neo4j is down"))
    good_driver = make_good_driver(mocker)

    db = make_db([bad_driver, good_driver])
    await db.execute(A_QUERY)

    assert bad_driver.execute_query.call_count == 1, (
        f"Bad driver should have been called exactly once before rotation, "
        f"got {bad_driver.execute_query.call_count}"
    )
    assert good_driver.execute_query.call_count == 1, (
        f"Good driver should have been called exactly once after rotation, "
        f"got {good_driver.execute_query.call_count}"
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_auth_rate_limit_error_treated_as_retryable(mocker):
    """
    Neo.ClientError.Security.AuthenticationRateLimit is a ClientError that is
    specifically whitelisted as retryable. The bad driver is rotated and the
    good driver is used for the retry — same outcome as TransientError etc.
    Other ClientErrors (syntax errors, constraint violations) are not retried.
    """
    rate_limit_error = ClientError("too many attempts")
    rate_limit_error._neo4j_code = AUTH_RATE_LIMIT_CODE

    bad_driver = make_bad_driver(mocker, rate_limit_error)
    good_driver = make_good_driver(mocker)

    db = make_db([bad_driver, good_driver])
    await db.execute(A_QUERY)

    assert bad_driver.execute_query.call_count == 1
    assert good_driver.execute_query.call_count == 1


# ---------------------------------------------------------------------------
# Test 2 — concurrent callers do not re-use the bad driver during rotation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_concurrent_callers_do_not_reuse_bad_driver(mocker):
    """
    When caller A hits an error and refreshes the driver, any concurrent
    caller B that arrives while the refresh is in progress must wait for
    _driver_ready and then use the fresh driver — it must NOT call
    execute_query on the bad driver.

    We gate the refresh by patching refresh_driver so it signals when the
    refresh has started, waits for B to arrive, then proceeds.  B must block
    on _driver_ready (cleared by refresh_driver) and not touch the bad driver.
    """
    a_refreshing = asyncio.Event()
    a_may_finish = asyncio.Event()

    bad_driver = make_bad_driver(mocker, ServiceUnavailable("down"))
    good_driver = make_good_driver(mocker)

    db = make_db([bad_driver, good_driver], retry_factor=0)

    original_refresh = db.refresh_driver

    async def controlled_refresh(*args, **kwargs):
        db._driver_ready.clear()
        a_refreshing.set()
        await a_may_finish.wait()
        db._driver_ready.set()
        await original_refresh(*args, **kwargs)

    db.refresh_driver = controlled_refresh

    a_task = asyncio.ensure_future(db.execute(A_QUERY))
    await a_refreshing.wait()

    # B arrives while A's refresh has cleared _driver_ready.
    b_task = asyncio.ensure_future(db.execute(ANOTHER_QUERY))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert bad_driver.execute_query.call_count == 1, (
        f"Bad driver should have been called exactly once (by A before it errored), "
        f"got {bad_driver.execute_query.call_count}"
    )

    a_may_finish.set()
    await a_task
    await b_task

    # Both A's retry and B should have used the good driver.
    assert good_driver.execute_query.call_count == 2, (
        f"Good driver should have been called twice (A retry + B), "
        f"got {good_driver.execute_query.call_count}"
    )
