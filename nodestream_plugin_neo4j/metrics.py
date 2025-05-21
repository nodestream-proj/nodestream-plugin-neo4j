from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from prometheus_client import Counter, Histogram, Gauge

@dataclass
class MetricDefinition:
    """Definition of a metric including its name, description, and type."""
    name: str
    description: str
    metric_type: str = "counter"  # counter, gauge, or histogram
    labels: Optional[list[str]] = None

class Neo4jMetricType(Enum):
    """Types of metrics supported by Neo4j."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"

class Neo4jMetrics:
    """Neo4j-specific metrics for monitoring database operations.
    
    This class defines all metrics that can be collected from Neo4j operations,
    including both base metrics and Neo4j-specific metrics. All metrics are
    Prometheus-compatible.
    """
    
    # Query Execution Metrics
    QUERY_EXECUTION_TIME = MetricDefinition(
        name="query_execution_time_seconds",
        description="Time taken to execute Cypher queries",
        metric_type=Neo4jMetricType.HISTOGRAM.value,
        labels=["database", "query_type"]
    )
    
    QUERY_PLANNING_TIME = MetricDefinition(
        name="query_planning_time_seconds",
        description="Time taken to plan Cypher queries",
        metric_type=Neo4jMetricType.HISTOGRAM.value,
        labels=["database"]
    )
    
    QUERY_PROCESSING_TIME = MetricDefinition(
        name="query_processing_time_seconds",
        description="Time taken to process query results",
        metric_type=Neo4jMetricType.HISTOGRAM.value,
        labels=["database"]
    )
    
    # Operation Metrics
    NODES_CREATED = MetricDefinition(
        name="nodes_created_total",
        description="Total number of nodes created",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "label"]
    )
    
    NODES_DELETED = MetricDefinition(
        name="nodes_deleted_total",
        description="Total number of nodes deleted",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "label"]
    )
    
    RELATIONSHIPS_CREATED = MetricDefinition(
        name="relationships_created_total",
        description="Total number of relationships created",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "type"]
    )
    
    RELATIONSHIPS_DELETED = MetricDefinition(
        name="relationships_deleted_total",
        description="Total number of relationships deleted",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "type"]
    )
    
    PROPERTIES_SET = MetricDefinition(
        name="properties_set_total",
        description="Total number of properties set",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "entity_type"]
    )
    
    LABELS_ADDED = MetricDefinition(
        name="labels_added_total",
        description="Total number of labels added to nodes",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "label"]
    )
    
    LABELS_REMOVED = MetricDefinition(
        name="labels_removed_total",
        description="Total number of labels removed from nodes",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "label"]
    )
    
    INDEXES_ADDED = MetricDefinition(
        name="indexes_added_total",
        description="Total number of indexes added",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "entity_type"]
    )
    
    INDEXES_REMOVED = MetricDefinition(
        name="indexes_removed_total",
        description="Total number of indexes removed",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "entity_type"]
    )
    
    CONSTRAINTS_ADDED = MetricDefinition(
        name="constraints_added_total",
        description="Total number of constraints added",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "entity_type"]
    )
    
    CONSTRAINTS_REMOVED = MetricDefinition(
        name="constraints_removed_total",
        description="Total number of constraints removed",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "entity_type"]
    )
    
    # APOC Batch Metrics
    BATCHES_PROCESSED = MetricDefinition(
        name="batches_processed_total",
        description="Total number of APOC batches processed",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "operation_type"]
    )
    
    COMMITTED_OPERATIONS = MetricDefinition(
        name="committed_operations_total",
        description="Total number of operations committed in APOC batches",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "operation_type"]
    )
    
    FAILED_OPERATIONS = MetricDefinition(
        name="failed_operations_total",
        description="Total number of operations that failed in APOC batches",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "operation_type", "error_type"]
    )
    
    # Error Metrics
    QUERY_ERRORS = MetricDefinition(
        name="query_errors_total",
        description="Total number of query execution errors",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "error_type", "error_code"]
    )
    
    FAILED_QUERIES = MetricDefinition(
        name="failed_queries_total",
        description="Total number of failed queries",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "error_type"]
    )
    
    # Query Statistics
    QUERIES_EXECUTED = MetricDefinition(
        name="queries_executed_total",
        description="Total number of queries executed",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "query_type"]
    )
    
    # Cache Metrics
    CACHE_HITS = MetricDefinition(
        name="cache_hits_total",
        description="Number of query plan cache hits",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "cache_type"]
    )
    
    CACHE_MISSES = MetricDefinition(
        name="cache_misses_total",
        description="Number of query plan cache misses",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "cache_type"]
    )
    
    # Transaction Metrics
    TRANSACTION_COMMITS = MetricDefinition(
        name="transaction_commits_total",
        description="Number of successful transaction commits",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database"]
    )
    
    TRANSACTION_ROLLBACKS = MetricDefinition(
        name="transaction_rollbacks_total",
        description="Number of transaction rollbacks",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "reason"]
    )
    
    TRANSACTION_ACTIVE = MetricDefinition(
        name="transactions_active",
        description="Number of currently active transactions",
        metric_type=Neo4jMetricType.GAUGE.value,
        labels=["database"]
    )
    
    # Connection Pool Metrics
    CONNECTION_POOL_ACTIVE = MetricDefinition(
        name="connection_pool_active",
        description="Number of active connections in the pool",
        metric_type=Neo4jMetricType.GAUGE.value,
        labels=["database"]
    )
    
    CONNECTION_POOL_IDLE = MetricDefinition(
        name="connection_pool_idle",
        description="Number of idle connections in the pool",
        metric_type=Neo4jMetricType.GAUGE.value,
        labels=["database"]
    )
    
    CONNECTION_POOL_TOTAL = MetricDefinition(
        name="connection_pool_total",
        description="Total number of connections in the pool",
        metric_type=Neo4jMetricType.GAUGE.value,
        labels=["database"]
    )
    
    # Session Metrics
    SESSIONS_ACTIVE = MetricDefinition(
        name="sessions_active",
        description="Number of currently active sessions",
        metric_type=Neo4jMetricType.GAUGE.value,
        labels=["database"]
    )
    
    SESSIONS_CREATED = MetricDefinition(
        name="sessions_created_total",
        description="Total number of sessions created",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database"]
    )
    
    SESSIONS_CLOSED = MetricDefinition(
        name="sessions_closed_total",
        description="Total number of sessions closed",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database"]
    )
    
    # Retry Metrics
    QUERY_RETRIES = MetricDefinition(
        name="query_retries_total",
        description="Total number of query retries",
        metric_type=Neo4jMetricType.COUNTER.value,
        labels=["database", "error_type"]
    )
    
    RETRY_DELAY = MetricDefinition(
        name="retry_delay_seconds",
        description="Time spent waiting between retries",
        metric_type=Neo4jMetricType.HISTOGRAM.value,
        labels=["database", "error_type"]
    )

class Neo4jMetricHandler:
    """Handler for Neo4j metrics that manages Prometheus metric instances."""
    
    def __init__(self):
        self._metrics: Dict[str, Any] = {}
        self._initialize_metrics()
    
    def _initialize_metrics(self):
        """Initialize all Prometheus metrics based on Neo4jMetrics definitions."""
        for metric_name, definition in Neo4jMetrics.__dict__.items():
            if not isinstance(definition, MetricDefinition):
                continue
                
            labels = definition.labels or []
            
            if definition.metric_type == Neo4jMetricType.COUNTER.value:
                self._metrics[metric_name] = Counter(
                    definition.name,
                    definition.description,
                    labels
                )
            elif definition.metric_type == Neo4jMetricType.GAUGE.value:
                self._metrics[metric_name] = Gauge(
                    definition.name,
                    definition.description,
                    labels
                )
            elif definition.metric_type == Neo4jMetricType.HISTOGRAM.value:
                self._metrics[metric_name] = Histogram(
                    definition.name,
                    definition.description,
                    labels
                )
    
    def increment(self, metric_name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        metric = self._metrics.get(metric_name)
        if metric and isinstance(metric, Counter):
            metric.labels(**(labels or {})).inc(value)
    
    def decrement(self, metric_name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Decrement a gauge metric."""
        metric = self._metrics.get(metric_name)
        if metric and isinstance(metric, Gauge):
            metric.labels(**(labels or {})).dec(value)
    
    def set(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value."""
        metric = self._metrics.get(metric_name)
        if metric and isinstance(metric, Gauge):
            metric.labels(**(labels or {})).set(value)
    
    def observe(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a value for a histogram metric."""
        metric = self._metrics.get(metric_name)
        if metric and isinstance(metric, Histogram):
            metric.labels(**(labels or {})).observe(value)
    
    def get_metric(self, metric_name: str):
        """Get a metric instance by name."""
        return self._metrics.get(metric_name) 