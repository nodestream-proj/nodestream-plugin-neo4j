from logging import getLogger
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from nodestream.databases.ingest_monitor import (
    IngestMonitor,
    QueryResult,
    DatabaseMetric,
    MetricHandler,
    NullMetricHandler
)
from neo4j import ResultSummary, EagerResult, Record, AsyncDriver, AsyncSession
from .query import APOC_BATCH_QUERY_RESPONSE_FIELDS
from .metrics import Neo4jMetrics, Neo4jMetricHandler

class Neo4jIngestMonitor(IngestMonitor[EagerResult, Record], alias="neo4j"):
    """Monitor for Neo4j database operations.
    
    This monitor tracks Neo4j-specific metrics including:
    - Query execution times
    - Node and relationship operations
    - APOC batch operations
    - Error tracking
    - Connection pool metrics
    - Session metrics
    - Transaction metrics
    """

    def __init__(self, handler: Optional[MetricHandler] = None, driver: Optional[AsyncDriver] = None):
        super().__init__(handler)
        self._current_database: str = "neo4j"  # Default database name
        self._neo4j_metrics = Neo4jMetricHandler()
        self._driver = driver
        self._active_sessions: Dict[str, AsyncSession] = {}
        self._active_transactions: Dict[str, int] = {}

    def _is_apoc_query(self, record: Dict[str, Any]) -> bool:
        """Check if the record contains APOC periodic iterate metrics."""
        return all(field in record.keys() for field in APOC_BATCH_QUERY_RESPONSE_FIELDS)

    def _update_connection_pool_metrics(self):
        """Update connection pool metrics if driver is available."""
        if not self._driver or not hasattr(self._driver, '_pool'):
            return

        pool = self._driver._pool
        if hasattr(pool, 'size'):
            self._neo4j_metrics.set(
                Neo4jMetrics.CONNECTION_POOL_TOTAL.name,
                pool.size,
                labels={"database": self._current_database}
            )
        if hasattr(pool, 'in_use'):
            self._neo4j_metrics.set(
                Neo4jMetrics.CONNECTION_POOL_ACTIVE.name,
                pool.in_use,
                labels={"database": self._current_database}
            )
        if hasattr(pool, 'free'):
            self._neo4j_metrics.set(
                Neo4jMetrics.CONNECTION_POOL_IDLE.name,
                pool.free,
                labels={"database": self._current_database}
            )

    def _update_session_metrics(self, session: AsyncSession, action: str):
        """Update session metrics."""
        session_id = id(session)
        if action == "created":
            self._active_sessions[session_id] = session
            self._neo4j_metrics.increment(
                Neo4jMetrics.SESSIONS_CREATED.name,
                labels={"database": self._current_database}
            )
        elif action == "closed":
            if session_id in self._active_sessions:
                del self._active_sessions[session_id]
            self._neo4j_metrics.increment(
                Neo4jMetrics.SESSIONS_CLOSED.name,
                labels={"database": self._current_database}
            )
        
        self._neo4j_metrics.set(
            Neo4jMetrics.SESSIONS_ACTIVE.name,
            len(self._active_sessions),
            labels={"database": self._current_database}
        )

    def _update_transaction_metrics(self, session: AsyncSession, action: str, reason: Optional[str] = None):
        """Update transaction metrics."""
        session_id = id(session)
        if action == "started":
            self._active_transactions[session_id] = self._active_transactions.get(session_id, 0) + 1
            self._neo4j_metrics.increment(
                Neo4jMetrics.TRANSACTION_COMMITS.name,
                labels={"database": self._current_database}
            )
        elif action == "rolled_back":
            if session_id in self._active_transactions:
                self._active_transactions[session_id] = max(0, self._active_transactions[session_id] - 1)
            self._neo4j_metrics.increment(
                Neo4jMetrics.TRANSACTION_ROLLBACKS.name,
                labels={"database": self._current_database, "reason": reason or "unknown"}
            )
        elif action == "committed":
            if session_id in self._active_transactions:
                self._active_transactions[session_id] = max(0, self._active_transactions[session_id] - 1)
            self._neo4j_metrics.increment(
                Neo4jMetrics.TRANSACTION_COMMITS.name,
                labels={"database": self._current_database}
            )
        
        total_active = sum(self._active_transactions.values())
        self._neo4j_metrics.set(
            Neo4jMetrics.TRANSACTION_ACTIVE.name,
            total_active,
            labels={"database": self._current_database}
        )

    def _extract_query_metrics(self, result: EagerResult) -> Dict[str, Any]:
        """Extract metrics from a query result and update metrics."""
        summary = result.summary
        records = result.records
        
        # Update database name if available
        if summary.database:
            self._current_database = summary.database

        # Update connection pool metrics
        self._update_connection_pool_metrics()

        # Extract timing metrics
        execution_time = (summary.result_available_after + summary.result_consumed_after) / 1000.0  # Convert to seconds
        planning_time = summary.result_available_after / 1000.0
        processing_time = summary.result_consumed_after / 1000.0

        # Update timing metrics
        self._neo4j_metrics.observe(
            Neo4jMetrics.QUERY_EXECUTION_TIME.name,
            execution_time,
            labels={"database": self._current_database, "query_type": "write" if summary.query_type == "w" else "read"}
        )
        self._neo4j_metrics.observe(
            Neo4jMetrics.QUERY_PLANNING_TIME.name,
            planning_time,
            labels={"database": self._current_database}
        )
        self._neo4j_metrics.observe(
            Neo4jMetrics.QUERY_PROCESSING_TIME.name,
            processing_time,
            labels={"database": self._current_database}
        )

        # Update operation metrics with labels
        for label in summary.counters.labels_added:
            self._neo4j_metrics.increment(
                Neo4jMetrics.LABELS_ADDED.name,
                labels={"database": self._current_database, "label": label}
            )
        for label in summary.counters.labels_removed:
            self._neo4j_metrics.increment(
                Neo4jMetrics.LABELS_REMOVED.name,
                labels={"database": self._current_database, "label": label}
            )

        # Update node metrics
        if summary.counters.nodes_created > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.NODES_CREATED.name,
                summary.counters.nodes_created,
                labels={"database": self._current_database, "label": "unknown"}  # TODO: Extract label from query
            )
        if summary.counters.nodes_deleted > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.NODES_DELETED.name,
                summary.counters.nodes_deleted,
                labels={"database": self._current_database, "label": "unknown"}  # TODO: Extract label from query
            )

        # Update relationship metrics
        if summary.counters.relationships_created > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.RELATIONSHIPS_CREATED.name,
                summary.counters.relationships_created,
                labels={"database": self._current_database, "type": "unknown"}  # TODO: Extract type from query
            )
        if summary.counters.relationships_deleted > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.RELATIONSHIPS_DELETED.name,
                summary.counters.relationships_deleted,
                labels={"database": self._current_database, "type": "unknown"}  # TODO: Extract type from query
            )

        # Update property metrics
        if summary.counters.properties_set > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.PROPERTIES_SET.name,
                summary.counters.properties_set,
                labels={"database": self._current_database, "entity_type": "unknown"}  # TODO: Extract entity type from query
            )

        # Update index and constraint metrics
        if summary.counters.indexes_added > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.INDEXES_ADDED.name,
                summary.counters.indexes_added,
                labels={"database": self._current_database, "entity_type": "unknown"}
            )
        if summary.counters.indexes_removed > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.INDEXES_REMOVED.name,
                summary.counters.indexes_removed,
                labels={"database": self._current_database, "entity_type": "unknown"}
            )
        if summary.counters.constraints_added > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.CONSTRAINTS_ADDED.name,
                summary.counters.constraints_added,
                labels={"database": self._current_database, "entity_type": "unknown"}
            )
        if summary.counters.constraints_removed > 0:
            self._neo4j_metrics.increment(
                Neo4jMetrics.CONSTRAINTS_REMOVED.name,
                summary.counters.constraints_removed,
                labels={"database": self._current_database, "entity_type": "unknown"}
            )

        # Initialize metrics dictionary for logging
        metrics = {
            "execution_time_ms": execution_time * 1000,
            "planning_time_ms": planning_time * 1000,
            "processing_time_ms": processing_time * 1000,
            "database_name": self._current_database,
            "database_url": str(summary.server.address) if summary.server else None,
            "nodes_created": summary.counters.nodes_created,
            "nodes_deleted": summary.counters.nodes_deleted,
            "relationships_created": summary.counters.relationships_created,
            "relationships_deleted": summary.counters.relationships_deleted,
            "properties_set": summary.counters.properties_set,
            "labels_added": summary.counters.labels_added,
            "labels_removed": summary.counters.labels_removed,
            "indexes_added": summary.counters.indexes_added,
            "indexes_removed": summary.counters.indexes_removed,
            "constraints_added": summary.counters.constraints_added,
            "constraints_removed": summary.counters.constraints_removed,
            "is_apoc_query": False,
            "apoc_metrics": {}
        }

        # Handle APOC query metrics
        if records and len(records) > 0 and self._is_apoc_query(records[0]):
            record = records[0]
            metrics.update({
                "is_apoc_query": True,
                "apoc_metrics": {
                    "batches": record['batches'],
                    "committed_operations": record['committedOperations'],
                    "failed_operations": record['failedOperations'],
                    "error_messages": record['errorMessages'] if record['errorMessages'] else []
                }
            })

            # Update APOC metrics with operation type
            operation_type = "unknown"  # TODO: Extract operation type from query
            self._neo4j_metrics.increment(
                Neo4jMetrics.BATCHES_PROCESSED.name,
                record['batches'],
                labels={"database": self._current_database, "operation_type": operation_type}
            )
            self._neo4j_metrics.increment(
                Neo4jMetrics.COMMITTED_OPERATIONS.name,
                record['committedOperations'],
                labels={"database": self._current_database, "operation_type": operation_type}
            )
            self._neo4j_metrics.increment(
                Neo4jMetrics.FAILED_OPERATIONS.name,
                record['failedOperations'],
                labels={"database": self._current_database, "operation_type": operation_type, "error_type": "unknown"}
            )

            # Track APOC errors
            if record['errorMessages']:
                for error_msg in record['errorMessages']:
                    self._neo4j_metrics.increment(
                        Neo4jMetrics.QUERY_ERRORS.name,
                        labels={
                            "database": self._current_database,
                            "error_type": "apoc_error",
                            "error_code": "unknown"
                        }
                    )
                    self._errors.append({
                        "type": "apoc_error",
                        "message": str(error_msg),
                        "batch_info": {
                            "batches": record['batches'],
                            "committed_operations": record['committedOperations'],
                            "failed_operations": record['failedOperations']
                        },
                        "database_url": metrics["database_url"]
                    })

        # Track notifications as errors
        for notification in summary.notifications if summary.notifications else []:
            if notification.severity.lower() in ['error', 'warning']:
                self._neo4j_metrics.increment(
                    Neo4jMetrics.QUERY_ERRORS.name,
                    labels={
                        "database": self._current_database,
                        "error_type": f"notification_{notification.severity.lower()}",
                        "error_code": notification.code
                    }
                )
                self._errors.append({
                    "type": "notification",
                    "severity": notification.severity,
                    "message": notification.message,
                    "code": notification.code,
                    "title": notification.title,
                    "description": notification.description,
                    "position": notification.position.dict() if notification.position else None,
                    "database_url": metrics["database_url"]
                })

        # Update query count
        self._neo4j_metrics.increment(
            Neo4jMetrics.QUERIES_EXECUTED.name,
            labels={"database": self._current_database, "query_type": "write" if summary.query_type == "w" else "read"}
        )

        # Update cache metrics if available
        if hasattr(summary, 'cache_hits'):
            self._neo4j_metrics.increment(
                Neo4jMetrics.CACHE_HITS.name,
                summary.cache_hits,
                labels={"database": self._current_database, "cache_type": "query_plan"}
            )
        if hasattr(summary, 'cache_misses'):
            self._neo4j_metrics.increment(
                Neo4jMetrics.CACHE_MISSES.name,
                summary.cache_misses,
                labels={"database": self._current_database, "cache_type": "query_plan"}
            )

        return metrics

    def record_query_result(self, result: EagerResult) -> None:
        """Record successful query execution metrics."""
        metrics = self._extract_query_metrics(result)
        
        # Log metrics using structured logging
        self.logger.info(
            "Query execution completed successfully",
            extra={
                "metrics": metrics,
                "errors": self._errors
            }
        )

    def record_query_error(self, error: Exception) -> None:
        """Record query execution error and update error statistics."""
        # Update error metrics
        self._neo4j_metrics.increment(
            Neo4jMetrics.QUERY_ERRORS.name,
            labels={
                "database": self._current_database,
                "error_type": type(error).__name__,
                "error_code": getattr(error, 'code', 'unknown')
            }
        )
        self._neo4j_metrics.increment(
            Neo4jMetrics.FAILED_QUERIES.name,
            labels={"database": self._current_database, "error_type": type(error).__name__}
        )

        # Add error to the list
        self._errors.append({
            "type": "query_error",
            "error_type": type(error).__name__,
            "message": str(error),
            "code": getattr(error, 'code', None)
        })

        # Log error using structured logging
        self.logger.error(
            "Query execution failed",
            extra={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "errors": self._errors
            },
            exc_info=error
        )

    def record_retry(self, error: Exception, delay: float) -> None:
        """Record query retry attempt."""
        self._neo4j_metrics.increment(
            Neo4jMetrics.QUERY_RETRIES.name,
            labels={"database": self._current_database, "error_type": type(error).__name__}
        )
        self._neo4j_metrics.observe(
            Neo4jMetrics.RETRY_DELAY.name,
            delay,
            labels={"database": self._current_database, "error_type": type(error).__name__}
        )

    def get_query_statistics(self) -> Dict[str, Any]:
        """Get current operation statistics including Neo4j specific metrics."""
        base_stats = super().get_query_statistics()
        neo4j_stats = {
            "database_name": self._current_database,
            "cumulative_stats": {
                metric_name: self._neo4j_metrics.get_metric(metric_name)._value.get()
                for metric_name in Neo4jMetrics.__dict__
                if isinstance(getattr(Neo4jMetrics, metric_name), MetricDefinition)
            }
        }
        return {**base_stats, **neo4j_stats}