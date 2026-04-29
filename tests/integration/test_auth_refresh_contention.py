"""
Integration tests for auth-refresh concurrency behaviour.

These tests verify that concurrent query failures are handled correctly:
- only one driver refresh happens even when multiple queries fail simultaneously
- queries that arrive mid-refresh wait for the ready driver rather than
  triggering their own refresh

Root bug
--------
The old _rotate_driver held an asyncio.Lock and unconditionally called
driver_factory() at the end of its lock body.  When two coroutines both hit
a retryable error simultaneously, both would queue on the lock, and the second
caller would close the fresh driver the first had just created and call
driver_factory() again — a double-refresh.

We guarantee both queries hold the bad driver simultaneously by making
execute_query on the bad driver block until both queries have received it,
then raise AuthError on both.  This is exactly what happens in production
when two concurrent queries hit an expired-credential error at the same time.

Fix
---
refresh_driver uses _refresh_lock and a _driver_ready asyncio.Event.
The first caller acquires the lock and performs the refresh.  Any concurrent
caller that acquires the lock after the first has finished sees the stale
driver no longer matches and skips the refresh.  _get_driver waits on
_driver_ready so callers that arrive mid-refresh block cleanly.

API compatibility
-----------------
These tests hook into whichever refresh method the implementation exposes
(refresh_driver in the new design, _rotate_driver in the PR) so that the
behavioural assertions are what fail — not an AttributeError.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from neo4j import AsyncDriver
from neo4j.exceptions import AuthError

from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.query import Query

TIMEOUT = 2.0

Q1 = Query("MATCH (n) RETURN n", {})
Q2 = Query("MATCH (r) RETURN r", {})
Q3 = Query("MATCH (p) RETURN p", {})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def make_good_driver(mocker) -> AsyncMock:
    driver = mocker.AsyncMock(AsyncDriver)
    driver.execute_query.return_value = _make_driver_result(mocker)
    return driver


def make_auth_error_driver(mocker) -> AsyncMock:
    """A bad driver that immediately raises AuthError on every execute_query call."""
    driver = mocker.AsyncMock(AsyncDriver)
    driver.execute_query.side_effect = AuthError("credentials expired")
    return driver


def make_simultaneous_auth_error_driver(mocker, n_concurrent: int) -> AsyncMock:
    """
    A bad driver whose execute_query blocks until n_concurrent callers are
    all waiting, then raises AuthError on all of them simultaneously.

    This replicates the production scenario: multiple in-flight queries all
    discover at the same moment that their credentials are expired.
    """
    barrier = asyncio.Barrier(n_concurrent)
    driver = mocker.AsyncMock(AsyncDriver)

    async def execute_query_side_effect(*args, **kwargs):
        await barrier.wait()
        raise AuthError("credentials expired")

    driver.execute_query.side_effect = execute_query_side_effect
    return driver


def make_db(
    bad_driver: AsyncMock,
    good_drivers: list[AsyncMock],
) -> tuple[Neo4jDatabaseConnection, list]:
    """
    Build a Neo4jDatabaseConnection with bad_driver pre-seeded as _driver
    and a factory that returns good_drivers in order, recording each call.
    """
    factory_calls: list = []
    drivers = iter(good_drivers)

    def driver_factory() -> AsyncMock:
        factory_calls.append(1)
        return next(drivers)

    db = Neo4jDatabaseConnection(
        driver_factory=driver_factory,
        database_name="neo4j",
        max_retry_attempts=3,
        retry_factor=0,
    )
    db._driver = bad_driver
    return db, factory_calls


def patch_refresh_method(db: Neo4jDatabaseConnection, signal: asyncio.Event):
    """Wrap refresh_driver to set signal when called."""
    original = db.refresh_driver

    async def signalling(*args, **kwargs):
        signal.set()
        await original(*args, **kwargs)

    db.refresh_driver = signalling


# ---------------------------------------------------------------------------
# Scenario 1: Q1 errors → refreshes; Q2 arrives mid-refresh
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_scenario_1_q2_gets_good_driver_after_q1_rotates(mocker):
    """
    Baseline: one error, one refresh, Q2 arrives while the refresh is in
    progress and must wait — not call execute_query on the bad driver.

    Pass conditions:
    - factory called exactly once
    - bad driver execute_query called exactly once (Q1's failing attempt)
    - good driver execute_query called exactly twice (Q1 retry + Q2)
    - completes within TIMEOUT
    """
    bad = make_auth_error_driver(mocker)
    good = make_good_driver(mocker)
    db, factory_calls = make_db(bad, [good])

    refresh_started = asyncio.Event()
    patch_refresh_method(db, refresh_started)

    async def run():
        q1 = asyncio.ensure_future(db.execute(Q1))
        await refresh_started.wait()
        q2 = asyncio.ensure_future(db.execute(Q2))
        await asyncio.gather(q1, q2)

    await asyncio.wait_for(run(), timeout=TIMEOUT)

    assert (
        len(factory_calls) == 1
    ), f"driver_factory should be called exactly once, got {len(factory_calls)}"
    assert (
        bad.execute_query.call_count == 1
    ), f"Bad driver called {bad.execute_query.call_count} times, expected 1"
    assert (
        good.execute_query.call_count == 2
    ), f"Good driver called {good.execute_query.call_count} times, expected 2"


# ---------------------------------------------------------------------------
# Scenario 2: Q1 and Q2 both error simultaneously
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_scenario_2_only_one_refresh_when_both_queries_error(mocker):
    """
    Q1 and Q2 both obtain the bad driver and hit AuthError simultaneously.
    Only one refresh must occur — the second caller must detect the driver
    has already been replaced and skip its own refresh.

    Pass conditions:
    - factory called exactly once (no double-refresh)
    - good driver called exactly twice (Q1 retry + Q2 retry)
    - completes within TIMEOUT
    """
    bad = make_simultaneous_auth_error_driver(mocker, n_concurrent=2)
    good = make_good_driver(mocker)
    # Deliberately one good driver — double-refresh exhausts the iterator
    # and surfaces as RuntimeError, proving the bug.
    db, factory_calls = make_db(bad, [good])

    async def run():
        await asyncio.gather(
            db.execute(Q1),
            db.execute(Q2),
        )

    await asyncio.wait_for(run(), timeout=TIMEOUT)

    assert (
        len(factory_calls) == 1
    ), f"driver_factory should be called exactly once, got {len(factory_calls)}"
    assert (
        good.execute_query.call_count == 2
    ), f"Good driver called {good.execute_query.call_count} times, expected 2"


# ---------------------------------------------------------------------------
# Scenario 3: Q1 + Q2 both error; Q3 just wants a driver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_scenario_3_q3_waits_without_triggering_another_refresh(mocker):
    """
    Q1 and Q2 both error concurrently.  Q3 arrives while the refresh is in
    progress — it must wait for the ready driver without triggering its own
    refresh.

    Pass conditions:
    - factory called exactly once
    - good driver called exactly 3 times (Q1 retry + Q2 retry + Q3)
    - completes within TIMEOUT
    """
    bad = make_simultaneous_auth_error_driver(mocker, n_concurrent=2)
    good = make_good_driver(mocker)
    db, factory_calls = make_db(bad, [good])

    refresh_started = asyncio.Event()
    patch_refresh_method(db, refresh_started)

    async def run():
        q1 = asyncio.ensure_future(db.execute(Q1))
        q2 = asyncio.ensure_future(db.execute(Q2))
        await refresh_started.wait()
        q3 = asyncio.ensure_future(db.execute(Q3))
        await asyncio.gather(q1, q2, q3)

    await asyncio.wait_for(run(), timeout=TIMEOUT)

    assert (
        len(factory_calls) == 1
    ), f"driver_factory should be called exactly once, got {len(factory_calls)}"
    assert (
        good.execute_query.call_count == 3
    ), f"Good driver called {good.execute_query.call_count} times, expected 3"
