from dataclasses import dataclass, field
from typing import List, Optional

from dacite import from_dict
from neo4j import EagerResult, Record, ResultSummary
from nodestream.metrics import Metric, Metrics

from .query import ApocBatchResponse, Query

# Timing metrics
PLANNING_TIME = Metric(
    "neo4j_query_planning_time_ms",
    "Time taken to plan the Neo4j query.",
    accumulate=True,
)
PROCESSING_TIME = Metric(
    "neo4j_query_processing_time_ms",
    "Time taken to execute the Neo4j query.",
    accumulate=True,
)
TOTAL_TIME = Metric(
    "neo4j_query_total_time_ms",
    "Total time taken to execute the Neo4j query.",
    accumulate=True,
)
APOC_TIME = Metric(
    "neo4j_query_apoc_time_ms",
    "Time taken to execute the Neo4j query using APOC.",
    accumulate=True,
)

# Write metrics
NODES_CREATED = Metric(
    "neo4j_query_write_metrics_nodes_created",
    "Number of nodes created in the Neo4j query.",
    accumulate=True,
)
NODES_DELETED = Metric(
    "neo4j_query_write_metrics_nodes_deleted",
    "Number of nodes deleted in the Neo4j query.",
    accumulate=True,
)
RELATIONSHIPS_CREATED = Metric(
    "neo4j_query_write_metrics_relationships_created",
    "Number of relationships created in the Neo4j query.",
    accumulate=True,
)
RELATIONSHIPS_DELETED = Metric(
    "neo4j_query_write_metrics_relationships_deleted",
    "Number of relationships deleted in the Neo4j query.",
    accumulate=True,
)
PROPERTIES_SET = Metric(
    "neo4j_query_write_metrics_properties_set",
    "Number of properties set in the Neo4j query.",
    accumulate=True,
)
LABELS_ADDED = Metric(
    "neo4j_query_write_metrics_labels_added",
    "Number of labels added in the Neo4j query.",
    accumulate=True,
)
LABELS_REMOVED = Metric(
    "neo4j_query_write_metrics_labels_removed",
    "Number of labels removed in the Neo4j query.",
    accumulate=True,
)
CONSTRAINTS_ADDED = Metric(
    "neo4j_query_write_metrics_constraints_added",
    "Number of constraints added in the Neo4j query.",
    accumulate=True,
)
CONSTRAINTS_REMOVED = Metric(
    "neo4j_query_write_metrics_constraints_removed",
    "Number of constraints removed in the Neo4j query.",
    accumulate=True,
)
INDEXES_ADDED = Metric(
    "neo4j_query_write_metrics_indexes_added",
    "Number of indexes added in the Neo4j query.",
    accumulate=True,
)
INDEXES_REMOVED = Metric(
    "neo4j_query_write_metrics_indexes_removed",
    "Number of indexes removed in the Neo4j query.",
    accumulate=True,
)

# APOC specific metrics
WAS_TERMINATED = Metric(
    "neo4j_query_was_terminated",
    "Whether the Neo4j query was terminated.",
    accumulate=True,
)
RETRIES = Metric(
    "neo4j_query_retries", "Number of retries in the Neo4j query.", accumulate=True
)

# Error tracking
ERROR_MESSAGES = Metric(
    "neo4j_query_error_messages",
    "Number of error messages in the Neo4j query.",
    accumulate=True,
)


@dataclass
class Neo4jTimingMetrics:
    """Timing metrics for query execution."""

    planning_time_ms: int = 0
    processing_time_ms: int = 0
    total_time_ms: int = 0
    apoc_time_ms: int = 0  # Time from APOC metrics if available


@dataclass
class Neo4jWriteMetrics:
    """Write operation metrics, consolidated from both APOC and query summary."""

    nodes_created: int = 0
    nodes_deleted: int = 0
    relationships_created: int = 0
    relationships_deleted: int = 0
    properties_set: int = 0
    labels_added: int = 0
    labels_removed: int = 0
    constraints_added: int = 0
    constraints_removed: int = 0
    indexes_added: int = 0
    indexes_removed: int = 0


@dataclass
class Neo4jQueryStatistics:
    """Consolidated statistics from both APOC metrics and query summary."""

    # Timing metrics
    timing: Neo4jTimingMetrics = field(default_factory=Neo4jTimingMetrics)

    # Write operation metrics (consolidated)
    write_metrics: Neo4jWriteMetrics = field(default_factory=Neo4jWriteMetrics)

    # APOC specific metrics
    was_terminated: bool = False
    retries: int = 0

    # Error tracking
    error_messages: List[str] = field(default_factory=list)

    @classmethod
    def from_result(
        cls, summary: ResultSummary, apoc_response: Optional[ApocBatchResponse] = None
    ) -> "Neo4jQueryStatistics":
        """Create statistics from a query result and optional APOC response."""
        stats = cls()

        # Set timing metrics
        stats.timing = Neo4jTimingMetrics(
            planning_time_ms=summary.result_available_after,
            processing_time_ms=summary.result_consumed_after,
            total_time_ms=summary.result_available_after
            + summary.result_consumed_after,
        )

        # Handle APOC metrics if present
        if apoc_response:
            stats.was_terminated = apoc_response.wasTerminated
            stats.retries = apoc_response.retries

            # Set APOC timing if available
            if hasattr(apoc_response, "timeTaken"):
                stats.timing.apoc_time_ms = apoc_response.timeTaken

            # Set error messages
            if apoc_response.errorMessages:
                stats.error_messages.extend(apoc_response.errorMessages.keys())

            # Set write metrics from APOC update statistics
            if apoc_response.updateStatistics:
                stats.write_metrics = Neo4jWriteMetrics(
                    nodes_created=apoc_response.updateStatistics.nodesCreated,
                    nodes_deleted=apoc_response.updateStatistics.nodesDeleted,
                    relationships_created=apoc_response.updateStatistics.relationshipsCreated,
                    relationships_deleted=apoc_response.updateStatistics.relationshipsDeleted,
                    properties_set=apoc_response.updateStatistics.propertiesSet,
                    labels_added=apoc_response.updateStatistics.labelsAdded,
                    labels_removed=apoc_response.updateStatistics.labelsRemoved,
                )
        else:
            # Set write metrics from query summary
            stats.write_metrics = Neo4jWriteMetrics(
                nodes_created=summary.counters.nodes_created,
                nodes_deleted=summary.counters.nodes_deleted,
                relationships_created=summary.counters.relationships_created,
                relationships_deleted=summary.counters.relationships_deleted,
                properties_set=summary.counters.properties_set,
                labels_added=summary.counters.labels_added,
                labels_removed=summary.counters.labels_removed,
                constraints_added=summary.counters.constraints_added,
                constraints_removed=summary.counters.constraints_removed,
                indexes_added=summary.counters.indexes_added,
                indexes_removed=summary.counters.indexes_removed,
            )

        return stats

    def update_metrics_from_summary(self):
        metrics = Metrics.get()

        metric_updates: list[tuple[Metric, int]] = [
            (PLANNING_TIME, self.timing.planning_time_ms),
            (PROCESSING_TIME, self.timing.processing_time_ms),
            (TOTAL_TIME, self.timing.planning_time_ms + self.timing.processing_time_ms),
            (APOC_TIME, self.timing.apoc_time_ms),
            (NODES_CREATED, self.write_metrics.nodes_created),
            (NODES_DELETED, self.write_metrics.nodes_deleted),
            (RELATIONSHIPS_CREATED, self.write_metrics.relationships_created),
            (RELATIONSHIPS_DELETED, self.write_metrics.relationships_deleted),
            (PROPERTIES_SET, self.write_metrics.properties_set),
            (LABELS_ADDED, self.write_metrics.labels_added),
            (LABELS_REMOVED, self.write_metrics.labels_removed),
            (CONSTRAINTS_ADDED, self.write_metrics.constraints_added),
            (CONSTRAINTS_REMOVED, self.write_metrics.constraints_removed),
            (INDEXES_ADDED, self.write_metrics.indexes_added),
            (INDEXES_REMOVED, self.write_metrics.indexes_removed),
            (WAS_TERMINATED, int(self.was_terminated)),
            (RETRIES, self.retries),
            (ERROR_MESSAGES, len(self.error_messages)),
        ]

        for metric, value in metric_updates:
            metrics.increment(metric, value)


class Neo4jResult:
    """Container for Neo4j query results with consolidated statistics."""

    def __init__(self, query: Query, result: EagerResult):
        self.query = query
        self.records: List[Record] = result.records
        self.keys: List[str] = result.keys
        self.summary: ResultSummary = result.summary

    def obtain_query_statistics(self) -> Neo4jQueryStatistics:
        # Extract APOC response if this is an APOC query
        apoc_response = None
        if self.query.is_apoc and self.records:
            apoc_response = from_dict(ApocBatchResponse, dict(self.records[0]))

        # Create consolidated statistics
        statistics = Neo4jQueryStatistics.from_result(self.summary, apoc_response)
        return statistics
