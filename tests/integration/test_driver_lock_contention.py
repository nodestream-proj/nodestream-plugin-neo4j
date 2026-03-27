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
    When caller A hits an error and rotates the driver, any concurrent caller B
    that arrives while rotation is in progress must wait and then use the fresh
    driver — it must NOT call execute_query on the bad driver.
    """
    a_rotating = asyncio.Event()
    a_may_finish = asyncio.Event()

    bad_driver = make_bad_driver(mocker, ServiceUnavailable("down"))
    good_driver = make_good_driver(mocker)

    db = make_db([bad_driver, good_driver], retry_factor=1)

    real_sleep = asyncio.sleep

    async def controlled_sleep(delay):
        # A is now inside _rotate_driver() — signal B to try and then wait.
        a_rotating.set()
        await a_may_finish.wait()

    mocker.patch("asyncio.sleep", controlled_sleep)

    a_task = asyncio.ensure_future(db.execute(A_QUERY))
    await a_rotating.wait()

    # B arrives while A is rotating — it must not call the bad driver.
    b_task = asyncio.ensure_future(db.execute(ANOTHER_QUERY))
    await real_sleep(0)
    await real_sleep(0)

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


# ---------------------------------------------------------------------------
# Test 3 — rate limit window simulation: backoff must outlast the window
# ---------------------------------------------------------------------------


def _make_rate_limit_error() -> ClientError:
    error = ClientError("too many attempts")
    error._neo4j_code = AUTH_RATE_LIMIT_CODE
    return error


def make_time_gated_db(mocker, rate_limit_window: float, retry_factor: float, max_retry_attempts: int = 10) -> tuple:
    """
    Build a Neo4jDatabaseConnection whose driver factory raises
    AuthenticationRateLimit until `rate_limit_window` seconds of virtual
    time have elapsed (tracked via a patched asyncio.sleep), then returns a
    good driver.

    Returns (db, virtual_clock) so tests can inspect elapsed time.
    """
    virtual_clock = {"elapsed": 0.0}

    async def fake_sleep(delay):
        virtual_clock["elapsed"] += delay

    mocker.patch("nodestream_plugin_neo4j.neo4j_database.asyncio.sleep", fake_sleep)

    good_driver = make_good_driver(mocker)

    def driver_factory():
        if virtual_clock["elapsed"] < rate_limit_window:
            driver = mocker.AsyncMock(AsyncDriver)
            driver.execute_query.side_effect = _make_rate_limit_error()
            return driver
        return good_driver

    db = Neo4jDatabaseConnection(
        driver_factory=driver_factory,
        database_name="neo4j",
        max_retry_attempts=max_retry_attempts,
        retry_factor=retry_factor,
    )
    return db, virtual_clock, good_driver


@pytest.mark.asyncio
@pytest.mark.integration
async def test_insufficient_backoff_exhausts_retries_inside_rate_limit_window(mocker):
    """
    When retry_factor is too small, all retry attempts occur within the
    rate limit window and every driver factory call returns a bad driver.
    execute() must exhaust max_retry_attempts and re-raise the error —
    proving that a pod restarting with tiny backoff stays in the crash loop.

    Window: 30s.  retry_factor=1 → total backoff = 1+2+3 = 6s < 30s.
    """
    WINDOW = 30.0
    db, clock, good_driver = make_time_gated_db(
        mocker, rate_limit_window=WINDOW, retry_factor=1, max_retry_attempts=3
    )

    with pytest.raises(ClientError) as exc_info:
        await db.execute(A_QUERY)

    assert exc_info.value.code == AUTH_RATE_LIMIT_CODE, (
        "Should have re-raised the AuthenticationRateLimit error"
    )
    assert clock["elapsed"] < WINDOW, (
        f"Total backoff {clock['elapsed']}s should be less than window {WINDOW}s — "
        f"all retries happened inside the rate limit window"
    )
    assert good_driver.execute_query.call_count == 0, (
        "Good driver should never have been reached"
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_sufficient_backoff_outlasts_rate_limit_window(mocker):
    """
    When retry_factor is large enough that the cumulative backoff exceeds
    the rate limit window, the driver factory eventually returns a good driver
    and execute() succeeds — proving that sufficient backoff breaks the loop.

    Window: 30s.  retry_factor=30 → first backoff = 30s >= window.
    """
    WINDOW = 30.0
    db, clock, good_driver = make_time_gated_db(
        mocker, rate_limit_window=WINDOW, retry_factor=30, max_retry_attempts=3
    )

    await db.execute(A_QUERY)

    assert clock["elapsed"] >= WINDOW, (
        f"Total backoff {clock['elapsed']}s should have reached or exceeded "
        f"the {WINDOW}s rate limit window"
    )
    assert good_driver.execute_query.call_count == 1, (
        "Good driver should have been called exactly once after backoff cleared the window"
    )
