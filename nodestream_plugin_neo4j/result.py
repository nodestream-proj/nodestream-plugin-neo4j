from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from neo4j import EagerResult, Record, ResultSummary
from datetime import datetime
from logging import getLogger
from .query import Query, ApocBatchResponse
from dacite import from_dict
from nodestream.metrics import MetricRegistry, Metric, Metrics

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
class Neo4jBatchMetrics:
    """Batch operation metrics from APOC."""
    total: int = 0
    committed: int = 0
    failed: int = 0
    errors: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Neo4jOperationMetrics:
    """Operation metrics from APOC."""
    total: int = 0
    committed: int = 0
    failed: int = 0
    errors: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Neo4jQueryStatistics:
    """Consolidated statistics from both APOC metrics and query summary."""
    query_type: str = "read"  # read, write, or rw
    
    # Timing metrics
    timing: Neo4jTimingMetrics = field(default_factory=Neo4jTimingMetrics)
    
    # Write operation metrics (consolidated)
    write_metrics: Neo4jWriteMetrics = field(default_factory=Neo4jWriteMetrics)
    
    # APOC specific metrics
    is_apoc_query: bool = False
    batches: Neo4jBatchMetrics = field(default_factory=Neo4jBatchMetrics)
    operations: Neo4jOperationMetrics = field(default_factory=Neo4jOperationMetrics)
    was_terminated: bool = False
    retries: int = 0
    
    # Error tracking
    error_messages: List[str] = field(default_factory=list)
    notifications: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_result(cls, summary: ResultSummary, apoc_response: Optional[ApocBatchResponse] = None) -> "Neo4jQueryStatistics":
        """Create statistics from a query result and optional APOC response."""
        stats = cls()
        
        # Set basic info
        stats.query_type = summary.query_type
        
        # Set timing metrics
        stats.timing = Neo4jTimingMetrics(
            planning_time_ms=summary.result_available_after,
            processing_time_ms=summary.result_consumed_after,
            total_time_ms=summary.result_available_after + summary.result_consumed_after
        )
        
        # Set notifications
        if summary.notifications:
            stats.notifications = [
                {
                    "severity": n.severity,
                    "code": n.code,
                    "title": n.title,
                    "description": n.description,
                    "position": n.position.__dict__ if n.position else None
                }
                for n in summary.notifications
            ]
        
        # Handle APOC metrics if present
        if apoc_response:
            stats.is_apoc_query = True
            stats.was_terminated = apoc_response.wasTerminated
            stats.retries = apoc_response.retries
            
            # Set APOC timing if available
            if hasattr(apoc_response, 'timeTaken'):
                stats.timing.apoc_time_ms = apoc_response.timeTaken
            
            # Set batch metrics
            stats.batches = Neo4jBatchMetrics(
                total=apoc_response.batch.total,
                committed=apoc_response.batch.committed,
                failed=apoc_response.batch.failed,
                errors=apoc_response.batch.errors
            )
            
            # Set operation metrics
            stats.operations = Neo4jOperationMetrics(
                total=apoc_response.operations.total,
                committed=apoc_response.operations.committed,
                failed=apoc_response.operations.failed,
                errors=apoc_response.operations.errors
            )
            
            # Set error messages
            if apoc_response.errorMessages:
                stats.error_messages.extend(apoc_response.errorMessages.values())
            
            # Set write metrics from APOC update statistics
            if apoc_response.updateStatistics:
                stats.write_metrics = Neo4jWriteMetrics(
                    nodes_created=apoc_response.updateStatistics.nodesCreated,
                    nodes_deleted=apoc_response.updateStatistics.nodesDeleted,
                    relationships_created=apoc_response.updateStatistics.relationshipsCreated,
                    relationships_deleted=apoc_response.updateStatistics.relationshipsDeleted,
                    properties_set=apoc_response.updateStatistics.propertiesSet,
                    labels_added=apoc_response.updateStatistics.labelsAdded,
                    labels_removed=apoc_response.updateStatistics.labelsRemoved
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
                indexes_removed=summary.counters.indexes_removed
            )
        
        return stats


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
            try:
                apoc_response = from_dict(ApocBatchResponse, dict(self.records[0]))
            except Exception as e:
                getLogger().error(f"Error parsing APOC response: {e}")
                raise e
        
        # Create consolidated statistics
        statistics = Neo4jQueryStatistics.from_result(self.summary, apoc_response)
        return statistics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the result to a dictionary format."""
        return {
            "records": [dict(record) for record in self.records],
            "keys": self.keys,
            "statistics": self.statistics.to_dict()
        }
    

class Neo4jMetricRegistry(MetricRegistry):
    # Timing metrics
    PLANNING_TIME = Metric("neo4j_query_planning_time_ms", "Time taken to plan the Neo4j query.")
    PROCESSING_TIME = Metric("neo4j_query_processing_time_ms", "Time taken to execute the Neo4j query.")
    TOTAL_TIME = Metric("neo4j_query_total_time_ms", "Total time taken to execute the Neo4j query.")
    APOC_TIME = Metric("neo4j_query_apoc_time_ms", "Time taken to execute the Neo4j query using APOC.")

    # Write metrics
    NODES_CREATED = Metric("neo4j_query_write_metrics_nodes_created", "Number of nodes created in the Neo4j query.")
    NODES_DELETED = Metric("neo4j_query_write_metrics_nodes_deleted", "Number of nodes deleted in the Neo4j query.")
    RELATIONSHIPS_CREATED = Metric("neo4j_query_write_metrics_relationships_created", "Number of relationships created in the Neo4j query.")
    RELATIONSHIPS_DELETED = Metric("neo4j_query_write_metrics_relationships_deleted", "Number of relationships deleted in the Neo4j query.")
    PROPERTIES_SET = Metric("neo4j_query_write_metrics_properties_set", "Number of properties set in the Neo4j query.")
    LABELS_ADDED = Metric("neo4j_query_write_metrics_labels_added", "Number of labels added in the Neo4j query.")
    LABELS_REMOVED = Metric("neo4j_query_write_metrics_labels_removed", "Number of labels removed in the Neo4j query.")
    CONSTRAINTS_ADDED = Metric("neo4j_query_write_metrics_constraints_added", "Number of constraints added in the Neo4j query.")
    CONSTRAINTS_REMOVED = Metric("neo4j_query_write_metrics_constraints_removed", "Number of constraints removed in the Neo4j query.")
    INDEXES_ADDED = Metric("neo4j_query_write_metrics_indexes_added", "Number of indexes added in the Neo4j query.")
    INDEXES_REMOVED = Metric("neo4j_query_write_metrics_indexes_removed", "Number of indexes removed in the Neo4j query.")

    # APOC specific metrics
    IS_APOC = Metric("neo4j_query_is_apoc", "Whether the query is an APOC query.")
    WAS_TERMINATED = Metric("neo4j_query_was_terminated", "Whether the Neo4j query was terminated.")
    RETRIES = Metric("neo4j_query_retries", "Number of retries in the Neo4j query.")

    # Batch metrics
    BATCH_TOTAL = Metric("neo4j_query_batch_total", "Total number of batches in the Neo4j query.")
    BATCH_COMMITTED = Metric("neo4j_query_batch_committed", "Number of batches committed in the Neo4j query.")
    BATCH_FAILED = Metric("neo4j_query_batch_failed", "Number of batches failed in the Neo4j query.")

    # Operation metrics
    OPERATIONS_TOTAL = Metric("neo4j_query_operations_total", "Total number of operations in the Neo4j query.")
    OPERATIONS_COMMITTED = Metric("neo4j_query_operations_committed", "Number of operations committed in the Neo4j query.")
    OPERATIONS_FAILED = Metric("neo4j_query_operations_failed", "Number of operations failed in the Neo4j query.")

    # Error tracking
    ERROR_MESSAGES = Metric("neo4j_query_error_messages", "Number of error messages in the Neo4j query.")
    NOTIFICATIONS = Metric("neo4j_query_notifications", "Number of notifications in the Neo4j query.")

    @classmethod
    def update_metrics_from_summary(cls, statistics: Neo4jQueryStatistics):
        metrics = Metrics.get()
        # Timing metrics
        metrics.increment(cls.PLANNING_TIME, statistics.timing.planning_time_ms)
        metrics.increment(cls.PROCESSING_TIME, statistics.timing.processing_time_ms)
        metrics.increment(cls.TOTAL_TIME, statistics.timing.total_time_ms)
        metrics.increment(cls.APOC_TIME, statistics.timing.apoc_time_ms)
        
        # Write metrics
        metrics.increment(cls.NODES_CREATED, statistics.write_metrics.nodes_created)
        metrics.increment(cls.NODES_DELETED, statistics.write_metrics.nodes_deleted)
        metrics.increment(cls.RELATIONSHIPS_CREATED, statistics.write_metrics.relationships_created)
        metrics.increment(cls.RELATIONSHIPS_DELETED, statistics.write_metrics.relationships_deleted)
        metrics.increment(cls.PROPERTIES_SET, statistics.write_metrics.properties_set)
        metrics.increment(cls.LABELS_ADDED, statistics.write_metrics.labels_added)
        metrics.increment(cls.LABELS_REMOVED, statistics.write_metrics.labels_removed)
        metrics.increment(cls.CONSTRAINTS_ADDED, statistics.write_metrics.constraints_added)
        metrics.increment(cls.CONSTRAINTS_REMOVED, statistics.write_metrics.constraints_removed)
        metrics.increment(cls.INDEXES_ADDED, statistics.write_metrics.indexes_added)
        metrics.increment(cls.INDEXES_REMOVED, statistics.write_metrics.indexes_removed)
        
        # APOC specific metrics
        metrics.increment(cls.IS_APOC, int(statistics.is_apoc_query))
        metrics.increment(cls.WAS_TERMINATED, int(statistics.was_terminated))
        metrics.increment(cls.RETRIES, statistics.retries)
        
        # Batch metrics
        metrics.increment(cls.BATCH_TOTAL, statistics.batches.total)
        metrics.increment(cls.BATCH_COMMITTED, statistics.batches.committed)
        metrics.increment(cls.BATCH_FAILED, statistics.batches.failed)
        
        # Operation metrics
        metrics.increment(cls.OPERATIONS_TOTAL, statistics.operations.total)
        metrics.increment(cls.OPERATIONS_COMMITTED, statistics.operations.committed)
        metrics.increment(cls.OPERATIONS_FAILED, statistics.operations.failed)
        
        # Error tracking
        metrics.increment(cls.ERROR_MESSAGES, len(statistics.error_messages))
        metrics.increment(cls.NOTIFICATIONS, len(statistics.notifications))

