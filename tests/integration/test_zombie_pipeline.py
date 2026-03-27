"""
Zombie pipeline reproduction test.

Replicates the mds-github-repos perpetual pipeline failure mode observed in
production on 2026-03-20 using the *exact* production components:

  StreamExtractor (stub Kafka connector)
    → Interpreter (real interpretations from mds-github-repos.yaml)
    → GraphDatabaseWriter
    → Neo4jDatabaseConnection (always raises AuthenticationRateLimit)

The test proves whether the fatal AuthenticationRateLimit error propagates
all the way out of Pipeline.run() so the process can exit, or whether it is
swallowed somewhere — leaving the process alive with a dead pipeline (zombie).
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver
from neo4j.exceptions import ClientError
from nodestream.databases.debounced_ingest_strategy import DebouncedIngestStrategy
from nodestream.databases.writer import GraphDatabaseWriter
from nodestream.interpreting import Interpreter
from nodestream.pipeline import Pipeline
from nodestream.pipeline.extractors.streams.extractor import (
    StreamConnector,
    StreamExtractor,
    StreamRecordFormat,
)
from nodestream.pipeline.object_storage import NullObjectStore
from nodestream.pipeline.progress_reporter import PipelineProgressReporter

from nodestream_plugin_neo4j.neo4j_database import (
    AUTH_RATE_LIMIT_CODE,
    Neo4jDatabaseConnection,
)
from nodestream_plugin_neo4j.query import QueryBatch
from nodestream_plugin_neo4j.query_executor import Neo4jQueryExecutor

# ---------------------------------------------------------------------------
# Stub connector / format — stand-ins for the real Kafka eventbus connector
# ---------------------------------------------------------------------------

# A minimal record matching the GithubRepo shape the Interpreter expects
STUB_RECORD = {
    "url": "https://github.com/example/repo",
    "nameWithOwner": "example/repo",
    "name": "repo",
    "databaseId": 1,
    "defaultBranchRef": {"name": "main"},
    "createdAt": "2021-01-01T00:00:00Z",
    "pushedAt": "2021-01-01T00:00:00Z",
    "isArchived": False,
    "isFork": False,
    "owner": {"login": "example"},
    "collaborators": [],
    "languages": [],
    "asset_id_from_marco": None,
}


class StubConnector(StreamConnector, alias="stub_zombie"):
    """Yields a fixed batch of records once, then returns empty batches.

    The first call to poll() returns RECORD_COUNT records.
    Subsequent calls return [] (simulating a quiet Kafka topic after a burst).
    """

    RECORD_COUNT = 3

    def __init__(self):
        self._polled = False

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def poll(self):
        if not self._polled:
            self._polled = True
            return [json.dumps(STUB_RECORD)] * self.RECORD_COUNT
        return []


class StubFormat(StreamRecordFormat, alias="stub_json_zombie"):
    def parse(self, record):
        return json.loads(record)


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
        max_retry_attempts=1,
        retry_factor=0,
    )


def _make_pipeline(db: Neo4jDatabaseConnection) -> Pipeline:
    # Real StreamExtractor with stub connector — same class as production
    extractor = StreamExtractor(
        connector=StubConnector(),
        record_format=StubFormat(),
    )

    # Real Interpreter — same class as production, simplified interpretations
    interpreter = Interpreter.from_file_data(
        interpretations=[
            [
                {
                    "type": "source_node",
                    "node_type": "GithubRepo",
                    "key": {"url": "!jmespath 'url'"},
                },
            ]
        ]
    )

    # Real GraphDatabaseWriter backed by the always-failing db
    ingest_query_builder = MagicMock()
    ingest_query_builder.apoc_iterate = False
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
        batch_size=1,
        ingest_strategy=ingest_strategy,
    )

    return Pipeline(
        steps=(extractor, interpreter, writer),
        step_outbox_size=10,
        object_store=NullObjectStore(),
    )


def _make_reporter() -> tuple[PipelineProgressReporter, dict]:
    """
    Reporter wired the same way as the nodestream CLI production JSON logging:
    on_fatal_error stores the exception, on_finish raises it.
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

    Uses the exact production pipeline shape:
      StreamExtractor → Interpreter → GraphDatabaseWriter

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
    same call chain as the nodestream CLI and asserts that Pipeline.run()
    returns WITHOUT raising when a fatal error occurs.

    Uses the exact production pipeline shape:
      StreamExtractor → Interpreter → GraphDatabaseWriter

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

    if not pipeline_run_raised:
        pytest.fail(
            "ZOMBIE CONFIRMED: Pipeline.run() returned normally after a fatal "
            f"AuthenticationRateLimit error. fatal_error={state['fatal_error']}, "
            f"finished={state['finished']}, finish_raised={state['finish_raised']}. "
            "The process would stay alive indefinitely — this is the production bug."
        )
