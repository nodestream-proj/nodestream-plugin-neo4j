from unittest.mock import Mock

from hamcrest import assert_that, empty, equal_to, is_, not_none

from nodestream_plugin_neo4j.query import ApocBatchResponse, ApocUpdateStatistics, Query
from nodestream_plugin_neo4j.result import (
    APOC_TIME,
    CONSTRAINTS_ADDED,
    CONSTRAINTS_REMOVED,
    ERROR_MESSAGES,
    INDEXES_ADDED,
    INDEXES_REMOVED,
    LABELS_ADDED,
    LABELS_REMOVED,
    NODES_CREATED,
    NODES_DELETED,
    OPERATIONS_COMMITTED,
    OPERATIONS_MISSING,
    PLANNING_TIME,
    PROCESSING_TIME,
    PROPERTIES_SET,
    RELATIONSHIPS_CREATED,
    RELATIONSHIPS_DELETED,
    RETRIES,
    TOTAL_TIME,
    WAS_TERMINATED,
    Neo4jQueryStatistics,
    Neo4jResult,
    Neo4jTimingMetrics,
    Neo4jWriteMetrics,
)


def test_neo4j_timing_metrics_default_initialization():
    metrics = Neo4jTimingMetrics()
    assert_that(metrics.planning_time_ms, equal_to(0))
    assert_that(metrics.processing_time_ms, equal_to(0))
    assert_that(metrics.total_time_ms, equal_to(0))
    assert_that(metrics.apoc_time_ms, equal_to(0))


def test_neo4j_timing_metrics_custom_initialization():
    metrics = Neo4jTimingMetrics(
        planning_time_ms=100,
        processing_time_ms=200,
        total_time_ms=300,
        apoc_time_ms=250,
    )
    assert_that(metrics.planning_time_ms, equal_to(100))
    assert_that(metrics.processing_time_ms, equal_to(200))
    assert_that(metrics.total_time_ms, equal_to(300))
    assert_that(metrics.apoc_time_ms, equal_to(250))


def test_neo4j_write_metrics_default_initialization():
    metrics = Neo4jWriteMetrics()
    assert_that(metrics.nodes_created, equal_to(0))
    assert_that(metrics.nodes_deleted, equal_to(0))
    assert_that(metrics.relationships_created, equal_to(0))
    assert_that(metrics.relationships_deleted, equal_to(0))
    assert_that(metrics.properties_set, equal_to(0))
    assert_that(metrics.labels_added, equal_to(0))
    assert_that(metrics.labels_removed, equal_to(0))
    assert_that(metrics.constraints_added, equal_to(0))
    assert_that(metrics.constraints_removed, equal_to(0))
    assert_that(metrics.indexes_added, equal_to(0))
    assert_that(metrics.indexes_removed, equal_to(0))
    assert_that(metrics.operations_committed, equal_to(0))
    assert_that(metrics.operations_missing, equal_to(0))


def test_neo4j_write_metrics_custom_initialization():
    metrics = Neo4jWriteMetrics(
        nodes_created=5,
        nodes_deleted=2,
        relationships_created=10,
        relationships_deleted=3,
        properties_set=15,
        labels_added=4,
        labels_removed=1,
        constraints_added=2,
        constraints_removed=1,
        indexes_added=3,
        indexes_removed=1,
        operations_committed=7,
        operations_missing=1,
    )
    assert_that(metrics.nodes_created, equal_to(5))
    assert_that(metrics.nodes_deleted, equal_to(2))
    assert_that(metrics.relationships_created, equal_to(10))
    assert_that(metrics.relationships_deleted, equal_to(3))
    assert_that(metrics.properties_set, equal_to(15))
    assert_that(metrics.labels_added, equal_to(4))
    assert_that(metrics.labels_removed, equal_to(1))
    assert_that(metrics.constraints_added, equal_to(2))
    assert_that(metrics.constraints_removed, equal_to(1))
    assert_that(metrics.indexes_added, equal_to(3))
    assert_that(metrics.indexes_removed, equal_to(1))
    assert_that(metrics.operations_committed, equal_to(7))
    assert_that(metrics.operations_missing, equal_to(1))


def test_neo4j_query_statistics_default_initialization():
    stats = Neo4jQueryStatistics()
    assert_that(stats.timing, not_none())
    assert_that(stats.timing, is_(Neo4jTimingMetrics))
    assert_that(stats.write_metrics, not_none())
    assert_that(stats.write_metrics, is_(Neo4jWriteMetrics))
    assert_that(stats.was_terminated, equal_to(False))
    assert_that(stats.retries, equal_to(0))
    assert_that(stats.error_messages, empty())


def test_neo4j_query_statistics_custom_initialization():
    timing = Neo4jTimingMetrics(planning_time_ms=50, processing_time_ms=100)
    write_metrics = Neo4jWriteMetrics(nodes_created=5, relationships_created=3)

    stats = Neo4jQueryStatistics(
        timing=timing,
        write_metrics=write_metrics,
        was_terminated=True,
        retries=2,
        error_messages=["error1", "error2"],
    )

    assert_that(stats.timing, equal_to(timing))
    assert_that(stats.write_metrics, equal_to(write_metrics))
    assert_that(stats.was_terminated, equal_to(True))
    assert_that(stats.retries, equal_to(2))
    assert_that(stats.error_messages, equal_to(["error1", "error2"]))


def test_neo4j_query_statistics_from_result_without_apoc():
    # Mock a ResultSummary
    mock_summary = Mock()
    mock_summary.result_available_after = 50
    mock_summary.result_consumed_after = 100

    # Mock counters
    mock_counters = Mock()
    mock_counters.nodes_created = 5
    mock_counters.nodes_deleted = 2
    mock_counters.relationships_created = 10
    mock_counters.relationships_deleted = 3
    mock_counters.properties_set = 15
    mock_counters.labels_added = 4
    mock_counters.labels_removed = 1
    mock_counters.constraints_added = 2
    mock_counters.constraints_removed = 1
    mock_counters.indexes_added = 3
    mock_counters.indexes_removed = 1
    mock_summary.counters = mock_counters

    stats = Neo4jQueryStatistics.from_result(mock_summary)

    assert_that(stats.timing.planning_time_ms, equal_to(50))
    assert_that(stats.timing.processing_time_ms, equal_to(100))
    assert_that(stats.timing.total_time_ms, equal_to(150))
    assert_that(stats.timing.apoc_time_ms, equal_to(0))

    assert_that(stats.write_metrics.nodes_created, equal_to(5))
    assert_that(stats.write_metrics.nodes_deleted, equal_to(2))
    assert_that(stats.write_metrics.relationships_created, equal_to(10))
    assert_that(stats.write_metrics.relationships_deleted, equal_to(3))
    assert_that(stats.write_metrics.properties_set, equal_to(15))
    assert_that(stats.write_metrics.labels_added, equal_to(4))
    assert_that(stats.write_metrics.labels_removed, equal_to(1))
    assert_that(stats.write_metrics.constraints_added, equal_to(2))
    assert_that(stats.write_metrics.constraints_removed, equal_to(1))
    assert_that(stats.write_metrics.indexes_added, equal_to(3))
    assert_that(stats.write_metrics.indexes_removed, equal_to(1))

    # Operations committed/missing should remain defaults for non-APOC
    assert_that(stats.write_metrics.operations_committed, equal_to(0))
    assert_that(stats.write_metrics.operations_missing, equal_to(0))

    assert_that(stats.was_terminated, equal_to(False))
    assert_that(stats.retries, equal_to(0))
    assert_that(stats.error_messages, empty())


MOCK_APOC_RECORD = [
    {
        "batch": 1,
        "total": 10,
        "timeTaken": 1000,
        "wasTerminated": False,
        "retries": 0,
        "updateStatistics": {"nodesCreated": 5},
    }
]


def test_neo4j_query_statistics_from_result_with_apoc():
    # Mock a ResultSummary
    mock_summary = Mock()
    mock_summary.result_available_after = 50
    mock_summary.result_consumed_after = 100

    # Create APOC response
    apoc_update_stats = ApocUpdateStatistics(
        nodesCreated=8,
        nodesDeleted=3,
        relationshipsCreated=12,
        relationshipsDeleted=4,
        propertiesSet=20,
        labelsAdded=5,
        labelsRemoved=2,
    )

    apoc_response = ApocBatchResponse(
        batches=3,
        total=100,
        timeTaken=2500,
        wasTerminated=True,
        retries=2,
        errorMessages={"batch1": "error1", "batch2": "error2"},
        committedOperations=13,
        failedOperations=2,
        updateStatistics=apoc_update_stats,
    )

    stats = Neo4jQueryStatistics.from_result(mock_summary, apoc_response)

    assert_that(stats.timing.planning_time_ms, equal_to(50))
    assert_that(stats.timing.processing_time_ms, equal_to(100))
    assert_that(stats.timing.total_time_ms, equal_to(150))
    assert_that(stats.timing.apoc_time_ms, equal_to(2500))

    assert_that(stats.write_metrics.nodes_created, equal_to(8))
    assert_that(stats.write_metrics.nodes_deleted, equal_to(3))
    assert_that(stats.write_metrics.relationships_created, equal_to(12))
    assert_that(stats.write_metrics.relationships_deleted, equal_to(4))
    assert_that(stats.write_metrics.properties_set, equal_to(20))
    assert_that(stats.write_metrics.labels_added, equal_to(5))
    assert_that(stats.write_metrics.labels_removed, equal_to(2))
    assert_that(stats.write_metrics.operations_committed, equal_to(13))
    assert_that(stats.write_metrics.operations_missing, equal_to(0))

    assert_that(stats.was_terminated, equal_to(True))
    assert_that(stats.retries, equal_to(2))
    assert_that(stats.error_messages, equal_to(["batch1", "batch2"]))


def test_neo4j_query_statistics_from_result_with_apoc_no_update_stats():
    # Mock a ResultSummary
    mock_summary = Mock()
    mock_summary.result_available_after = 25
    mock_summary.result_consumed_after = 75

    # Create APOC response without update statistics
    apoc_response = ApocBatchResponse(
        batches=2,
        total=50,
        timeTaken=1500,
        wasTerminated=False,
        retries=1,
        errorMessages={},
        committedOperations=0,
        failedOperations=4,
        updateStatistics=None,
    )

    stats = Neo4jQueryStatistics.from_result(mock_summary, apoc_response)

    assert_that(stats.was_terminated, equal_to(False))
    assert_that(stats.retries, equal_to(1))
    assert_that(stats.timing.apoc_time_ms, equal_to(1500))
    assert_that(stats.error_messages, empty())

    # Operations committed/missing inferred from APOC response
    assert_that(stats.write_metrics.operations_committed, equal_to(0))
    assert_that(stats.write_metrics.operations_missing, equal_to(4))

    # Write metrics should be defaults since no update statistics
    assert_that(stats.write_metrics.nodes_created, equal_to(0))
    assert_that(stats.write_metrics.relationships_created, equal_to(0))


def test_neo4j_result_initialization():
    query = Query("MATCH (n) RETURN n", {})

    # Mock EagerResult
    mock_result = Mock()
    mock_result.records = [Mock(), Mock()]
    mock_result.keys = ["n"]
    mock_result.summary = Mock()

    result = Neo4jResult(query, mock_result)

    assert_that(result.query, equal_to(query))
    assert_that(result.records, equal_to(mock_result.records))
    assert_that(result.keys, equal_to(["n"]))
    assert_that(result.summary, equal_to(mock_result.summary))


def test_neo4j_result_obtain_query_statistics_non_apoc():
    query = Query("MATCH (n) RETURN n", {}, is_apoc=False)

    # Mock EagerResult
    mock_result = Mock()
    mock_result.records = []
    mock_result.keys = ["n"]

    # Mock summary
    mock_summary = Mock()
    mock_summary.query_type = "read"
    mock_summary.result_available_after = 30
    mock_summary.result_consumed_after = 70
    mock_counters = Mock()
    mock_counters.nodes_created = 0
    mock_counters.nodes_deleted = 0
    mock_counters.relationships_created = 0
    mock_counters.relationships_deleted = 0
    mock_counters.properties_set = 0
    mock_counters.labels_added = 0
    mock_counters.labels_removed = 0
    mock_counters.constraints_added = 0
    mock_counters.constraints_removed = 0
    mock_counters.indexes_added = 0
    mock_counters.indexes_removed = 0
    mock_summary.counters = mock_counters
    mock_result.summary = mock_summary

    result = Neo4jResult(query, mock_result)
    stats = result.obtain_query_statistics()

    assert_that(stats.timing.planning_time_ms, equal_to(30))
    assert_that(stats.timing.processing_time_ms, equal_to(70))
    assert_that(stats.timing.total_time_ms, equal_to(100))


def test_neo4j_result_obtain_query_statistics_apoc_query(mocker):
    # Mock from_dict to return a valid ApocBatchResponse
    mock_from_dict = mocker.patch("nodestream_plugin_neo4j.result.from_dict")
    apoc_response = ApocBatchResponse(
        batches=1,
        total=10,
        timeTaken=1000,
        wasTerminated=False,
        retries=0,
        updateStatistics=ApocUpdateStatistics(nodesCreated=5),
    )
    mock_from_dict.return_value = apoc_response

    query = Query("CALL apoc.test()", {}, is_apoc=True)

    # Mock EagerResult with records
    mock_result = Mock()
    mock_result.records = MOCK_APOC_RECORD
    mock_result.keys = ["result"]

    # Mock summary
    mock_summary = Mock()
    mock_summary.query_type = "write"
    mock_summary.result_available_after = 40
    mock_summary.result_consumed_after = 60
    mock_result.summary = mock_summary

    result = Neo4jResult(query, mock_result)
    stats = result.obtain_query_statistics()

    assert_that(stats.timing.apoc_time_ms, equal_to(1000))
    assert_that(stats.write_metrics.nodes_created, equal_to(5))

    # Verify from_dict was called
    mock_from_dict.assert_called_once()


def test_update_metrics_from_summary(mocker):
    # Mock the Metrics.get() method
    mock_metrics = Mock()
    mock_get = mocker.patch("nodestream_plugin_neo4j.result.Metrics.get")
    mock_get.return_value = mock_metrics

    # Create test statistics
    timing = Neo4jTimingMetrics(
        planning_time_ms=50, processing_time_ms=100, total_time_ms=150, apoc_time_ms=200
    )
    write_metrics = Neo4jWriteMetrics(
        nodes_created=5,
        nodes_deleted=2,
        relationships_created=10,
        relationships_deleted=3,
        properties_set=15,
        labels_added=4,
        labels_removed=1,
        constraints_added=2,
        constraints_removed=1,
        indexes_added=3,
        indexes_removed=1,
        operations_committed=7,
        operations_missing=1,
    )

    stats = Neo4jQueryStatistics(
        timing=timing,
        write_metrics=write_metrics,
        was_terminated=True,
        retries=2,
        error_messages=["error1", "error2"],
    )

    stats.update_metrics_from_summary()

    # Verify all metrics were incremented correctly
    expected_calls = [
        mocker.call(PLANNING_TIME, 50),
        mocker.call(PROCESSING_TIME, 100),
        mocker.call(TOTAL_TIME, 150),
        mocker.call(APOC_TIME, 200),
        mocker.call(NODES_CREATED, 5),
        mocker.call(NODES_DELETED, 2),
        mocker.call(RELATIONSHIPS_CREATED, 10),
        mocker.call(RELATIONSHIPS_DELETED, 3),
        mocker.call(PROPERTIES_SET, 15),
        mocker.call(LABELS_ADDED, 4),
        mocker.call(LABELS_REMOVED, 1),
        mocker.call(CONSTRAINTS_ADDED, 2),
        mocker.call(CONSTRAINTS_REMOVED, 1),
        mocker.call(INDEXES_ADDED, 3),
        mocker.call(INDEXES_REMOVED, 1),
        mocker.call(OPERATIONS_COMMITTED, 7),
        mocker.call(OPERATIONS_MISSING, 1),
        mocker.call(WAS_TERMINATED, 1),  # True converted to int
        mocker.call(RETRIES, 2),
        mocker.call(ERROR_MESSAGES, 2),  # len(error_messages)
    ]

    mock_metrics.increment.assert_has_calls(expected_calls, any_order=True)
    assert_that(mock_metrics.increment.call_count, equal_to(20))


def test_metric_constants_are_defined():
    # Test that all metric constants are properly defined
    metrics = [
        PLANNING_TIME,
        PROCESSING_TIME,
        TOTAL_TIME,
        APOC_TIME,
        NODES_CREATED,
        NODES_DELETED,
        RELATIONSHIPS_CREATED,
        RELATIONSHIPS_DELETED,
        PROPERTIES_SET,
        LABELS_ADDED,
        LABELS_REMOVED,
        CONSTRAINTS_ADDED,
        CONSTRAINTS_REMOVED,
        INDEXES_ADDED,
        INDEXES_REMOVED,
        OPERATIONS_COMMITTED,
        OPERATIONS_MISSING,
        WAS_TERMINATED,
        RETRIES,
        ERROR_MESSAGES,
    ]

    for metric in metrics:
        assert_that(metric, not_none())
        assert_that(metric.name, not_none())
        assert_that(metric.description, not_none())
