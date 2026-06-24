"""
Integration test reproducing the zombie pipeline bug with the exact log trace
seen in production (2026-05-24T23:41:* on nodestream 0.15.2).

PRODUCTION LOG SEQUENCE (chronological)
----------------------------------------
1. 4x concurrent "Error executing query, rotating driver and backing off. Attempt 1"
   (AuthError — Task-12666/67/68/70, parallel writer chunks)
2. 4x "Fetching new neo4j credentials" + "Attempt 2" (RateLimit after refresh)
3. 4x "Fetching new neo4j credentials" + "Attempt 3" (RateLimit again)
4. "Error processing record"  (root logger, fatal=True, step_name=GraphDatabaseWriter)
5. "Pipeline Failed"          (root logger, ERROR)
6. "Pipeline Completed"       (root logger, INFO)
7. "Secondary exception during pipeline cancellation cleanup"  (root logger, WARNING)
   — the StreamExtractor task was mid-flush when it was cancelled; it raised
   another RateLimit during its own cancellation cleanup.

THE BUG
-------
After "Pipeline Completed", the StreamExtractor step's finish() is NEVER called.
In prod the StreamExtractor holds the Kafka/stream consumer.  Because finish()
is skipped, the consumer (and any IDPS credential-refresh thread tied to it)
stays alive after the pipeline claims to be done — the process is a zombie.

ROOT CAUSE
----------
CancelledError is a BaseException, not an Exception.  When the gather() cleanup
in pipeline.run() cancels the StreamExtractor task, CancelledError is raised
inside ProcessRecordsState / EmitOutstandingRecordsState.  Those states only
catch `except Exception`, so CancelledError escapes, short-circuiting the state
machine before StopStepExecution — where step.finish() is called.

FIX (nodestream bundle/0.15-fixes)
------------------------------------
ProcessRecordsState and EmitOutstandingRecordsState catch BaseException instead
of Exception, store the CancelledError on context.cancellation_cause, and
transition to StopStepExecution so that finish() is always called.

WHAT THIS TEST PROVES
---------------------
- The full production log sequence (steps 1-7) is reproduced in order.
- After "Pipeline Completed", extractor.finish_called is False on 0.15.2.
- On the fixed branch extractor.finish_called is True (the assertion flips
  and the test fails with a clear "the bug appears to be fixed" message).

PIPELINE TOPOLOGY (mirrors production mds-github-repos)
---------------------------------------------------------
  _PerpetualExtractorStep  →  _DbWriterStep
      (Task-5 in prod)          (Task-3/12666-70 in prod)

_DbWriterStep calls db.execute() multiple times in parallel (mirroring
DebouncedIngestStrategy.flush_nodes_updates → asyncio.gather(upserts)),
so all 4 sub-tasks fail with AuthError/RateLimit in the same pattern.
"""

import asyncio
import logging
from typing import Optional
from unittest.mock import AsyncMock

import pytest
from neo4j import AsyncDriver
from neo4j.exceptions import AuthError, ClientError

from nodestream_plugin_neo4j.neo4j_database import (
    AUTH_RATE_LIMIT_CODE,
    Neo4jDatabaseConnection,
)
from nodestream_plugin_neo4j.query import Query

from nodestream.pipeline.object_storage import NullObjectStore
from nodestream.pipeline.pipeline import Pipeline
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import Step


# ---------------------------------------------------------------------------
# Log capture
# ---------------------------------------------------------------------------


class _LogCapture(logging.Handler):
    """Captures log records emitted during the test."""

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def messages(self) -> list[str]:
        return [r.getMessage() for r in self.records]

    def names(self) -> list[str]:
        return [r.name for r in self.records]

    def levelnames(self) -> list[str]:
        return [r.levelname for r in self.records]


# ---------------------------------------------------------------------------
# Driver / connection helpers
# ---------------------------------------------------------------------------


def _make_rate_limit_error() -> ClientError:
    err = ClientError(
        "The client has provided incorrect authentication details too many times in a row."
    )
    err.code = AUTH_RATE_LIMIT_CODE
    return err


def _make_auth_then_rate_limit_driver(mocker, auth_failures: int = 4) -> AsyncMock:
    """
    Driver that raises AuthError for the first `auth_failures` calls, then
    AuthenticationRateLimit for all subsequent calls — mirroring the prod
    sequence where 4 concurrent chunks first get Unauthorized then RateLimit.
    """
    driver = mocker.AsyncMock(AsyncDriver)
    call_count = 0

    async def failing_execute(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= auth_failures:
            raise AuthError("The client is unauthorized due to authentication failure.")
        raise _make_rate_limit_error()

    driver.execute_query.side_effect = failing_execute
    return driver


def _make_db(mocker, auth_failures: int = 4) -> tuple[Neo4jDatabaseConnection, AsyncMock]:
    """
    Build a Neo4jDatabaseConnection whose driver always fails with auth errors.
    max_retry_attempts=3 matches prod (Attempt 1, 2, 3 in logs).
    retry_factor=0 keeps the test fast (no real sleep).

    driver_factory logs "Fetching new neo4j credentials" (identical to the real
    auth_provider_factory message) so the production log sequence is reproduced.
    """
    bad_driver = _make_auth_then_rate_limit_driver(mocker, auth_failures)

    import logging as _logging
    _cred_logger = _logging.getLogger("nodestream_plugin_neo4j.neo4j_database")

    def driver_factory():
        _cred_logger.info("Fetching new neo4j credentials")
        return bad_driver

    db = Neo4jDatabaseConnection(
        driver_factory=driver_factory,
        database_name="neo4j",
        max_retry_attempts=3,
        retry_factor=0,
    )
    # Do NOT pre-seed _driver so driver_factory() (and its credential log) fires
    # on first use, matching production.
    return db, bad_driver


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


class _PerpetualExtractorStep(Step):
    """
    Emits one sentinel record then blocks indefinitely — simulating a Kafka
    StreamExtractor that is alive and polling when the writer step fails fatally.

    In production this is Task-5.  When pipeline.run() cancels it (step 5 of
    the sequence above), CancelledError is raised inside emit_outstanding_records.
    Because EmitOutstandingRecordsState only catches Exception (not BaseException),
    CancelledError escapes and StopStepExecution / finish() is never reached.

    finish_called lets the test assert that resource cleanup was (or wasn't) done.
    """

    SENTINEL = object()

    def __init__(self):
        self.finish_called = False

    async def emit_outstanding_records(self, context):
        yield self.SENTINEL
        # Block forever — simulating a stream consumer waiting for the next message.
        await asyncio.Event().wait()

    async def finish(self, context):
        self.finish_called = True


class _DbWriterStep(Step):
    """
    Simulates GraphDatabaseWriter flushing a batch: fires N parallel db.execute()
    calls (matching DebouncedIngestStrategy.flush_nodes_updates → asyncio.gather),
    all of which exhaust auth retries and raise — becoming the fatal error that
    triggers pipeline cancellation.

    parallel_chunks=4 matches the 4 concurrent Task-12666/67/68/70 in production.
    """

    def __init__(self, db: Neo4jDatabaseConnection, parallel_chunks: int = 4) -> None:
        self.db = db
        self.parallel_chunks = parallel_chunks

    async def process_record(self, record, context):
        # Fire parallel db.execute() calls, mirroring flush_nodes_updates().
        await asyncio.gather(
            *(
                self.db.execute(Query("MATCH (n) RETURN n", {}))
                for _ in range(self.parallel_chunks)
            )
        )
        yield record  # pragma: no cover — never reached

    async def finish(self, context):
        await self.db.close()


# ---------------------------------------------------------------------------
# Reporter wiring (mirrors nodestream CLI's JsonProgressIndicator)
# ---------------------------------------------------------------------------


def _make_reporter(logger: logging.Logger) -> tuple[PipelineProgressReporter, list]:
    """
    Build a PipelineProgressReporter that re-raises fatal errors in on_finish,
    exactly as the CLI does. Returns (reporter, fatal_errors_list).
    """
    fatal_errors: list[Exception] = []

    def on_fatal_error(exc: Exception) -> None:
        logger.error("Pipeline Failed", exc_info=exc)
        fatal_errors.append(exc)

    def on_finish(metrics) -> None:
        elapsed = 0.0
        logger.info("Pipeline Completed", extra={"elapsed_seconds": elapsed})
        if fatal_errors:
            raise fatal_errors[0]

    return (
        PipelineProgressReporter(
            reporting_frequency=1,
            on_fatal_error_callback=on_fatal_error,
            on_finish_callback=on_finish,
            logger=logger,
        ),
        fatal_errors,
    )


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_zombie_pipeline_log_trace_and_missing_finish(mocker):
    """
    Reproduce the production zombie pipeline scenario end-to-end.

    Asserts:
      1. The log messages appear in the same order as observed in production
         on nodestream 0.15.2 (the version where the bug was confirmed).
      2. After "Pipeline Completed", extractor.finish() was NOT called —
         demonstrating that the StreamExtractor step's cleanup was skipped,
         leaving any resource it holds (Kafka consumer, IDPS thread) open.

    Against the fixed nodestream (bundle/0.15-fixes) the test fails with:
      "extractor.finish() WAS called — the zombie bug appears to be fixed."
    """
    capture = _LogCapture()
    capture.setLevel(logging.DEBUG)

    root = logging.getLogger()
    neo4j_db_logger = logging.getLogger("Neo4jDatabaseConnection")
    neo4j_plugin_logger = logging.getLogger("nodestream_plugin_neo4j.neo4j_database")

    # Ensure INFO-level messages pass through (loggers may default to WARNING).
    old_levels = {
        root: root.level,
        neo4j_db_logger: neo4j_db_logger.level,
        neo4j_plugin_logger: neo4j_plugin_logger.level,
    }
    for lg in old_levels:
        lg.setLevel(logging.DEBUG)
        lg.addHandler(capture)

    try:
        await _run_scenario(mocker, capture)
    finally:
        for lg, lvl in old_levels.items():
            lg.removeHandler(capture)
            lg.setLevel(lvl)


async def _run_scenario(mocker, capture: _LogCapture):
    db, bad_driver = _make_db(mocker)

    extractor = _PerpetualExtractorStep()
    writer = _DbWriterStep(db)

    pipeline = Pipeline(
        steps=(extractor, writer),
        step_outbox_size=10,
        object_store=NullObjectStore(),
    )

    pipeline_logger = logging.getLogger()
    reporter, fatal_errors = _make_reporter(pipeline_logger)

    try:
        await asyncio.wait_for(pipeline.run(reporter), timeout=15.0)
    except Exception:
        pass  # fatal error propagating out of pipeline.run() is expected

    # -----------------------------------------------------------------------
    # 1. Verify the production log sequence is reproduced in order
    # -----------------------------------------------------------------------
    messages = capture.messages()
    levelnames = capture.levelnames()
    names = capture.names()

    # Deduplicate by (levelname, message prefix) — each message is emitted once
    # per logger it propagates through; we want to find it at least once.
    def find(substr: str, after: int = 0) -> Optional[int]:
        for i, m in enumerate(messages):
            if i >= after and substr in m:
                return i
        return None

    # 1. "Fetching new neo4j credentials" — auth provider called on driver refresh
    idx_fetch = find("Fetching new neo4j credentials")
    assert idx_fetch is not None, (
        "Expected 'Fetching new neo4j credentials' in logs.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )

    # 2. "Error executing query, rotating driver and backing off. Attempt N"
    idx_retry = find("Error executing query, rotating driver and backing off. Attempt", after=idx_fetch)
    assert idx_retry is not None, (
        "Expected retry warning after credential fetch.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )
    assert levelnames[idx_retry] == "WARNING"

    # 3. "Error processing record" — fatal error from GraphDatabaseWriter
    idx_proc_err = find("Error processing record", after=idx_retry)
    assert idx_proc_err is not None, (
        "Expected 'Error processing record' fatal log.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )
    assert levelnames[idx_proc_err] == "ERROR"

    # 4. "Pipeline Failed"
    idx_failed = find("Pipeline Failed", after=idx_proc_err)
    assert idx_failed is not None, (
        "Expected 'Pipeline Failed' log.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )
    assert levelnames[idx_failed] == "ERROR"

    # 5. "Pipeline Completed" — pipeline claims to be done
    idx_completed = find("Pipeline Completed", after=idx_failed)
    assert idx_completed is not None, (
        "Expected 'Pipeline Completed' after Pipeline Failed.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )
    assert levelnames[idx_completed] == "INFO"

    # 6. "Secondary exception during pipeline cancellation cleanup" — present in
    #    0.15.2 because the cancel-and-wait fix is there; the cancelled extractor/
    #    interpreter tasks raise again during cleanup.
    idx_secondary = find("Secondary exception during pipeline cancellation cleanup", after=idx_completed)
    assert idx_secondary is not None, (
        "Expected 'Secondary exception during pipeline cancellation cleanup'.\n"
        "This is logged in 0.15.2 during the gather() cancel-and-wait cleanup.\nGot:\n"
        + "\n".join(f"  [{l}] {m}" for l, m in zip(levelnames, messages))
    )
    assert levelnames[idx_secondary] == "WARNING"

    # Print the log trace for visibility
    print("\n--- Captured log trace (matches production 0.15.2) ---")
    key = {idx_fetch, idx_retry, idx_proc_err, idx_failed, idx_completed, idx_secondary}
    for i, (lv, nm, msg) in enumerate(zip(levelnames, names, messages)):
        marker = " <--" if i in key else ""
        print(f"  [{lv:7}] {nm}: {msg[:120]}{marker}")
    print("--- end log trace ---\n")

    # -----------------------------------------------------------------------
    # 2. THE ZOMBIE: extractor.finish() was NOT called after "Pipeline Completed"
    #
    # In 0.15.2, pipeline.run() cancels the StreamExtractor task (step 5).
    # CancelledError is raised inside EmitOutstandingRecordsState, which only
    # catches `except Exception`.  CancelledError escapes → StopStepExecution
    # never runs → finish() never called → Kafka consumer / IDPS thread leaks.
    #
    # Fix (bundle/0.15-fixes): catch BaseException, store cancellation_cause,
    # always transition through StopStepExecution so finish() runs.
    # -----------------------------------------------------------------------
    assert not extractor.finish_called, (
        "extractor.finish() WAS called — the zombie bug appears to be fixed.\n"
        "Update this assertion to assert extractor.finish_called is True."
    )

    print(
        "\n[ZOMBIE CONFIRMED] 'Pipeline Completed' was logged but "
        "extractor.finish() was never called.\n"
        "Any resource the extractor holds (Kafka consumer, IDPS thread) "
        "is still alive — the process is a zombie.\n"
    )
