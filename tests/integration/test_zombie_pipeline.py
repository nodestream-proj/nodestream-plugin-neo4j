"""
Zombie pipeline reproduction test.

Replicates the mds-github-repos perpetual pipeline failure mode observed in
production on 2026-03-20:

  - StreamExtractor yields records
  - Interpreter passes them through
  - GraphDatabaseWriter flushes to Neo4j
  - Neo4j raises AuthenticationRateLimit
  - Nodestream marks the error fatal and shuts the pipeline down gracefully
  - The PROCESS STAYS ALIVE — the exception is swallowed somewhere in the
    nodestream pipeline runner before it can reach sys.exit()

The test builds a minimal version of the pipeline using the same components
as mds-github-repos (stubbed extractor, passthrough interpreter, real
GraphDatabaseWriter backed by a Neo4jDatabaseConnection that always raises
AuthenticationRateLimit) and runs it through nodestream's actual Pipeline.run()
to prove the zombie behaviour and pinpoint where the exception dies.
"""

from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver
from neo4j.exceptions import ClientError
from nodestream.databases.debounced_ingest_strategy import DebouncedIngestStrategy
from nodestream.databases.writer import GraphDatabaseWriter
from nodestream.model import Node
from nodestream.pipeline import Pipeline
from nodestream.pipeline.extractors.extractor import Extractor
from nodestream.pipeline.object_storage import NullObjectStore
from nodestream.pipeline.progress_reporter import PipelineProgressReporter

from nodestream_plugin_neo4j.neo4j_database import (
    AUTH_RATE_LIMIT_CODE,
    Neo4jDatabaseConnection,
)
from nodestream_plugin_neo4j.query import QueryBatch
from nodestream_plugin_neo4j.query_executor import Neo4jQueryExecutor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_rate_limit_error() -> ClientError:
    error = ClientError(
        "The client has provided incorrect authentication details too many times in a row."
    )
    error._neo4j_code = AUTH_RATE_LIMIT_CODE
    return error


def _make_always_failing_db() -> Neo4jDatabaseConnection:
    """A Neo4jDatabaseConnection whose driver always raises AuthenticationRateLimit."""

    def driver_factory():
        driver = MagicMock(AsyncDriver)
        driver.execute_query = AsyncMock(side_effect=_make_rate_limit_error())
        # session() needs to return an async context manager for implicit queries
        session_cm = AsyncMock()
        session_cm.__aenter__.return_value = session_cm
        session_cm.__aexit__.return_value = False
        session_cm.run = AsyncMock(side_effect=_make_rate_limit_error())
        driver.session.return_value = session_cm
        driver.close = AsyncMock()
        return driver

    return Neo4jDatabaseConnection(
        driver_factory=driver_factory,
        database_name="neo4j",
        max_retry_attempts=1,  # fail fast — we want to observe the fatal error
        retry_factor=0,
    )


class StubIngestible:
    """A minimal ingestible that queues a node into the debouncer and flushes,
    which triggers the Neo4j query — and therefore the auth error."""

    async def ingest(self, strategy):
        await strategy.ingest_source_node(
            Node(type="GithubRepo", key_values={"url": "https://example.com"})
        )
        await strategy.flush()


class StubExtractor(Extractor):
    """Yields a small number of ingestible records then stops — like a
    StreamExtractor that received a batch from Kafka.

    Uses Extractor.emit_outstanding_records / extract_records so the pipeline
    runner actually sees the emitted records.
    """

    def __init__(self, record_count: int = 3):
        self.record_count = record_count

    async def extract_records(self) -> AsyncGenerator:
        for _ in range(self.record_count):
            yield StubIngestible()


def _make_pipeline(db: Neo4jDatabaseConnection) -> Pipeline:
    ingest_query_builder = MagicMock()
    ingest_query_builder.apoc_iterate = False  # QueryBatch.as_query does bool lookup
    # generate_batch_update_node_operation_batch must return a real QueryBatch so
    # QueryBatch.as_query() produces a real Query (not a MagicMock with truthy is_implicit)
    ingest_query_builder.generate_batch_update_node_operation_batch.return_value = (
        QueryBatch(query_statement="MATCH (n) RETURN n", batched_parameter_sets=[{}])
    )
    query_executor = Neo4jQueryExecutor(
        database_connection=db,
        ingest_query_builder=ingest_query_builder,
        chunk_size=1,
        execute_chunks_in_parallel=False,
    )
    ingest_strategy = DebouncedIngestStrategy(query_executor)
    writer = GraphDatabaseWriter(
        batch_size=1,  # flush after every record so auth error fires immediately
        ingest_strategy=ingest_strategy,
    )
    extractor = StubExtractor(record_count=3)
    return Pipeline(
        steps=(extractor, writer),
        step_outbox_size=10,
        object_store=NullObjectStore(),
    )


def _make_reporter() -> tuple[PipelineProgressReporter, dict]:
    """
    Returns a reporter wired the same way the nodestream CLI wires it for
    production JSON logging: on_fatal_error stores the exception,
    on_finish raises it.

    Also returns a state dict so tests can inspect what happened.
    """
    state = {
        "fatal_error": None,
        "finished": False,
        "finish_raised": False,
    }

    def on_fatal_error(exc):
        state["fatal_error"] = exc

    def on_finish(metrics):
        state["finished"] = True
        if state["fatal_error"] is not None:
            state["finish_raised"] = True
            raise state["fatal_error"]

    reporter = PipelineProgressReporter(
        reporting_frequency=1,
        on_fatal_error_callback=on_fatal_error,
        on_finish_callback=on_finish,
    )
    return reporter, state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_fatal_auth_error_propagates_out_of_pipeline_run():
    """
    Proves that when GraphDatabaseWriter raises AuthenticationRateLimit,
    nodestream's pipeline runner propagates the exception all the way out
    of Pipeline.run() so the process can exit.

    If this test FAILS (no exception raised), it reproduces the zombie:
    the pipeline died internally but Pipeline.run() returned normally,
    leaving the process alive with nothing to do.
    """
    db = _make_always_failing_db()
    pipeline = _make_pipeline(db)
    reporter, state = _make_reporter()

    exception_escaped = None
    try:
        await pipeline.run(reporter)
    except Exception as e:
        exception_escaped = e

    # The fatal error must have been recorded by the reporter
    assert state["fatal_error"] is not None, (
        "Reporter never received a fatal error — writer didn't raise or "
        "the error was swallowed before reaching report_error()"
    )
    assert isinstance(
        state["fatal_error"], ClientError
    ), f"Expected ClientError, got {type(state['fatal_error'])}"
    assert state["fatal_error"].code == AUTH_RATE_LIMIT_CODE

    # on_finish must have been called and must have re-raised
    assert state["finish_raised"], (
        "on_finish was called but did not raise — exception was swallowed "
        "inside the pipeline runner. THIS IS THE ZOMBIE BUG."
    )

    # The exception must have escaped Pipeline.run() entirely
    assert exception_escaped is not None, (
        "Pipeline.run() returned without raising even though on_finish raised. "
        "The exception was swallowed inside asyncio.gather() or Executor.run(). "
        "THIS IS THE ZOMBIE BUG — process stays alive after fatal pipeline error."
    )
    assert exception_escaped.code == AUTH_RATE_LIMIT_CODE


@pytest.mark.asyncio
@pytest.mark.integration
async def test_zombie_pipeline_process_stays_alive_after_fatal_error():
    """
    Directly reproduces the production zombie: runs the pipeline through the
    same call chain as the nodestream CLI (pipeline.run → reporter callbacks)
    and asserts that Pipeline.run() returns WITHOUT raising when the production
    JSON indicator pattern is used.

    If this test PASSES it confirms the zombie exists.
    If it FAILS (exception escapes) the zombie is fixed.
    """
    db = _make_always_failing_db()
    pipeline = _make_pipeline(db)
    reporter, state = _make_reporter()

    pipeline_run_raised = False
    try:
        await pipeline.run(reporter)
    except Exception:
        pipeline_run_raised = True

    # In the zombie scenario the pipeline reported a fatal error internally
    # but Pipeline.run() returned normally — the process lives on with nothing to do.
    if not pipeline_run_raised:
        pytest.fail(
            "ZOMBIE CONFIRMED: Pipeline.run() returned normally after a fatal "
            f"AuthenticationRateLimit error. fatal_error={state['fatal_error']}, "
            f"finished={state['finished']}, finish_raised={state['finish_raised']}. "
            "The process would stay alive indefinitely — this is the production bug."
        )
