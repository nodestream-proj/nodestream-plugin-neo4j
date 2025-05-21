from dataclasses import dataclass
from typing import Optional, Dict, Any, List, TYPE_CHECKING
from neo4j import AsyncDriver, AsyncSession
from .monitoring import MonitoringConfig
from .neo4j_ingest_monitor import Neo4jIngestMonitor
from .neo4j_database import Neo4jDatabaseConnection
from .ingest_query_builder import Neo4jIngestQueryBuilder
from .metrics import Neo4jMetrics

if TYPE_CHECKING:
    from .enhanced_components import EnhancedSessionManager, EnhancedTransactionManager

@dataclass
class Neo4jExecutorComponents:
    """Container for executor components with proper lifecycle management."""
    database_connection: Neo4jDatabaseConnection
    ingest_query_builder: Neo4jIngestQueryBuilder
    monitor: Optional['Neo4jIngestMonitor'] = None
    monitoring_config: Optional[MonitoringConfig] = None
    chunk_size: int = 1000
    execute_chunks_in_parallel: bool = True
    retries_per_chunk: int = 3
    session_manager: Optional['EnhancedSessionManager'] = None
    transaction_manager: Optional['EnhancedTransactionManager'] = None
    
    def __post_init__(self):
        """Initialize components after creation."""
        if self.monitor and self.monitoring_config and self.monitoring_config.enabled:
            self.monitor._driver = self.database_connection.driver

class Neo4jSessionManager:
    """Manages Neo4j sessions with proper lifecycle and monitoring."""
    
    def __init__(
        self,
        driver: AsyncDriver,
        monitor: Optional[Neo4jIngestMonitor] = None,
        monitoring_config: Optional[MonitoringConfig] = None
    ):
        self._driver = driver
        self._monitor = monitor
        self._monitoring_config = monitoring_config
        self._active_sessions: Dict[str, AsyncSession] = {}
        self._session_metrics: Dict[str, Dict[str, Any]] = {}
    
    async def get_session(
        self,
        database: Optional[str] = None,
        **config
    ) -> AsyncSession:
        """Get a session with proper monitoring and lifecycle management."""
        session = await self._driver.session(database=database, **config)
        session_id = id(session)
        
        # Track session
        self._active_sessions[session_id] = session
        self._session_metrics[session_id] = {
            "database": database,
            "created_at": datetime.now().isoformat(),
            "queries_executed": 0,
            "transactions_started": 0,
            "transactions_committed": 0,
            "transactions_rolled_back": 0
        }
        
        # Update monitoring if enabled
        if self._monitor and self._monitoring_config and self._monitoring_config.collect_session_metrics:
            self._monitor._update_session_metrics(session, "created")
        
        return session
    
    async def close_session(self, session: AsyncSession):
        """Close a session with proper cleanup and monitoring."""
        session_id = id(session)
        if session_id in self._active_sessions:
            # Update monitoring if enabled
            if self._monitor and self._monitoring_config and self._monitoring_config.collect_session_metrics:
                self._monitor._update_session_metrics(session, "closed")
            
            # Record final metrics
            if self._monitor and self._monitoring_config and self._monitoring_config.collect_session_metrics:
                metrics = self._session_metrics[session_id]
                self._monitor._neo4j_metrics.set(
                    Neo4jMetrics.SESSIONS_ACTIVE.name,
                    len(self._active_sessions) - 1,  # -1 because we're about to remove this session
                    labels={"database": metrics["database"]}
                )
            
            # Cleanup
            await session.close()
            del self._active_sessions[session_id]
            del self._session_metrics[session_id]
    
    async def close_all_sessions(self):
        """Close all active sessions."""
        for session in list(self._active_sessions.values()):
            await self.close_session(session)
    
    def get_session_metrics(self, session: AsyncSession) -> Dict[str, Any]:
        """Get metrics for a specific session."""
        return self._session_metrics.get(id(session), {})
    
    def update_session_metrics(self, session: AsyncSession, metric: str, value: Any = 1):
        """Update metrics for a specific session."""
        session_id = id(session)
        if session_id in self._session_metrics:
            if isinstance(value, (int, float)):
                self._session_metrics[session_id][metric] = self._session_metrics[session_id].get(metric, 0) + value
            else:
                self._session_metrics[session_id][metric] = value

class Neo4jTransactionManager:
    """Manages Neo4j transactions with proper monitoring and retry logic."""
    
    def __init__(
        self,
        session_manager: Neo4jSessionManager,
        monitor: Optional[Neo4jIngestMonitor] = None,
        monitoring_config: Optional[MonitoringConfig] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self._session_manager = session_manager
        self._monitor = monitor
        self._monitoring_config = monitoring_config
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._active_transactions: Dict[str, int] = {}
    
    async def execute_in_transaction(
        self,
        session: AsyncSession,
        queries: List[Dict[str, Any]],
        retry_on_errors: Optional[List[str]] = None
    ) -> List[Any]:
        """Execute queries in a transaction with proper monitoring and retry logic."""
        retry_on_errors = retry_on_errors or ["Neo.TransientError.Transaction.Terminated"]
        session_id = id(session)
        retries = 0
        
        while retries <= self._max_retries:
            try:
                # Start transaction
                async with session.begin_transaction() as tx:
                    if self._monitor and self._monitoring_config and self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "started")
                        self._active_transactions[session_id] = self._active_transactions.get(session_id, 0) + 1
                    
                    results = []
                    for query_info in queries:
                        result = await tx.run(
                            query_info["query"],
                            query_info.get("parameters", {})
                        )
                        eager_result = await result.consume()
                        results.append(eager_result)
                        
                        # Update session metrics
                        self._session_manager.update_session_metrics(session, "queries_executed")
                        
                        # Update monitoring if enabled
                        if self._monitor and self._monitoring_config and self._monitoring_config.collect_query_metrics:
                            self._monitor.record_query_result(eager_result)
                    
                    # Commit transaction
                    await tx.commit()
                    if self._monitor and self._monitoring_config and self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "committed")
                        self._session_manager.update_session_metrics(session, "transactions_committed")
                    
                    return results
                    
            except Exception as e:
                # Check if we should retry
                should_retry = (
                    retries < self._max_retries and
                    any(error in str(e) for error in retry_on_errors)
                )
                
                # Update monitoring
                if self._monitor and self._monitoring_config:
                    if self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "rolled_back", str(e))
                        self._session_manager.update_session_metrics(session, "transactions_rolled_back")
                    if self._monitoring_config.collect_error_metrics:
                        self._monitor.record_query_error(e)
                    if should_retry and self._monitoring_config.collect_error_metrics:
                        self._monitor.record_retry(e, self._retry_delay * (2 ** retries))
                
                if not should_retry:
                    raise
                
                # Wait before retrying
                await asyncio.sleep(self._retry_delay * (2 ** retries))
                retries += 1
        
        raise Exception(f"Transaction failed after {self._max_retries} retries")
    
    def get_active_transactions(self, session: AsyncSession) -> int:
        """Get the number of active transactions for a session."""
        return self._active_transactions.get(id(session), 0)
    
    def clear_active_transactions(self, session: AsyncSession):
        """Clear active transaction count for a session."""
        self._active_transactions.pop(id(session), None) 