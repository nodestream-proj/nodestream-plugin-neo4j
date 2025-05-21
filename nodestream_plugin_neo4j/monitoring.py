from dataclasses import dataclass
from typing import Optional, Set, Dict, Any
from enum import Enum, auto

class MonitoringLevel(Enum):
    """Levels of monitoring detail available."""
    NONE = auto()  # No monitoring
    BASIC = auto()  # Basic query execution metrics
    DETAILED = auto()  # Detailed metrics including driver internals
    DEBUG = auto()  # All metrics including query parsing and planning

@dataclass
class MonitoringConfig:
    """Configuration for Neo4j monitoring.
    
    This class allows users to configure what metrics they want to collect
    and how they want to collect them. By default, monitoring is disabled.
    """
    enabled: bool = False
    level: MonitoringLevel = MonitoringLevel.NONE
    collect_driver_metrics: bool = False  # Whether to collect driver-level metrics
    collect_query_metrics: bool = False  # Whether to collect query-level metrics
    collect_session_metrics: bool = False  # Whether to collect session-level metrics
    collect_transaction_metrics: bool = False  # Whether to collect transaction-level metrics
    collect_apoc_metrics: bool = False  # Whether to collect APOC-specific metrics
    collect_error_metrics: bool = False  # Whether to collect detailed error metrics
    collect_cache_metrics: bool = False  # Whether to collect cache metrics
    collect_connection_metrics: bool = False  # Whether to collect connection pool metrics
    
    # Optional metric filters
    included_metrics: Optional[Set[str]] = None  # Only collect these metrics if specified
    excluded_metrics: Optional[Set[str]] = None  # Don't collect these metrics
    
    # Optional metric labels to include
    additional_labels: Optional[Dict[str, str]] = None  # Additional labels to add to all metrics
    
    @classmethod
    def basic(cls) -> 'MonitoringConfig':
        """Create a basic monitoring configuration."""
        return cls(
            enabled=True,
            level=MonitoringLevel.BASIC,
            collect_query_metrics=True,
            collect_error_metrics=True
        )
    
    @classmethod
    def detailed(cls) -> 'MonitoringConfig':
        """Create a detailed monitoring configuration."""
        return cls(
            enabled=True,
            level=MonitoringLevel.DETAILED,
            collect_driver_metrics=True,
            collect_query_metrics=True,
            collect_session_metrics=True,
            collect_transaction_metrics=True,
            collect_apoc_metrics=True,
            collect_error_metrics=True,
            collect_cache_metrics=True,
            collect_connection_metrics=True
        )
    
    @classmethod
    def debug(cls) -> 'MonitoringConfig':
        """Create a debug monitoring configuration."""
        return cls(
            enabled=True,
            level=MonitoringLevel.DEBUG,
            collect_driver_metrics=True,
            collect_query_metrics=True,
            collect_session_metrics=True,
            collect_transaction_metrics=True,
            collect_apoc_metrics=True,
            collect_error_metrics=True,
            collect_cache_metrics=True,
            collect_connection_metrics=True
        )
    
    def should_collect_metric(self, metric_name: str) -> bool:
        """Determine if a metric should be collected based on configuration."""
        if not self.enabled:
            return False
            
        if self.included_metrics and metric_name not in self.included_metrics:
            return False
            
        if self.excluded_metrics and metric_name in self.excluded_metrics:
            return False
            
        # Map metric names to their collection flags
        metric_collection_map = {
            # Driver metrics
            "connection_pool": self.collect_driver_metrics,
            "driver": self.collect_driver_metrics,
            
            # Query metrics
            "query_execution": self.collect_query_metrics,
            "query_planning": self.collect_query_metrics,
            "query_processing": self.collect_query_metrics,
            "nodes_": self.collect_query_metrics,
            "relationships_": self.collect_query_metrics,
            "properties_": self.collect_query_metrics,
            "labels_": self.collect_query_metrics,
            "indexes_": self.collect_query_metrics,
            "constraints_": self.collect_query_metrics,
            
            # Session metrics
            "sessions_": self.collect_session_metrics,
            
            # Transaction metrics
            "transaction_": self.collect_transaction_metrics,
            
            # APOC metrics
            "batches_": self.collect_apoc_metrics,
            "committed_operations": self.collect_apoc_metrics,
            "failed_operations": self.collect_apoc_metrics,
            
            # Error metrics
            "query_errors": self.collect_error_metrics,
            "failed_queries": self.collect_error_metrics,
            
            # Cache metrics
            "cache_": self.collect_cache_metrics,
            
            # Connection metrics
            "connection_pool_": self.collect_connection_metrics
        }
        
        # Check if any of the metric collection flags match
        return any(
            metric_name.startswith(prefix) and should_collect
            for prefix, should_collect in metric_collection_map.items()
        )
    
    def get_labels(self, base_labels: Dict[str, str]) -> Dict[str, str]:
        """Get the final set of labels to use for metrics."""
        labels = base_labels.copy()
        if self.additional_labels:
            labels.update(self.additional_labels)
        return labels 