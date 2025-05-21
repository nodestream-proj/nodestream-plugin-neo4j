from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Optional, Dict, Any, List, Type, Set, TYPE_CHECKING
from uuid import uuid4
import asyncio
import logging
from neo4j import AsyncDriver, AsyncSession, EagerResult
from neo4j.exceptions import Neo4jError, TransientError

from .monitoring import MonitoringConfig
from .metrics import Neo4jMetrics

if TYPE_CHECKING:
    from .neo4j_ingest_monitor import Neo4jIngestMonitor

logger = logging.getLogger(__name__)

class SessionStatus(Enum):
    """Status of a Neo4j session."""
    CREATED = "created"
    ACTIVE = "active"
    IDLE = "idle"
    CLOSING = "closing"
    CLOSED = "closed"
    ERROR = "error"

class TransactionStatus(Enum):
    """Status of a Neo4j transaction."""
    STARTED = "started"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"
    TIMED_OUT = "timed_out"

@dataclass
class SessionState:
    """State tracking for a Neo4j session."""
    id: str
    session: AsyncSession
    database: Optional[str]
    created_at: datetime
    status: SessionStatus
    last_used_at: datetime
    queries_executed: int = 0
    transactions_started: int = 0
    transactions_committed: int = 0
    transactions_rolled_back: int = 0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TransactionState:
    """State tracking for a Neo4j transaction."""
    id: str
    session_id: str
    start_time: datetime
    status: TransactionStatus
    queries: List[Dict[str, Any]]
    error: Optional[str] = None
    retry_count: int = 0
    last_retry_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

class RetryStrategy(Enum):
    """Strategy for retrying failed operations."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    error_types: List[Type[Exception]] = field(default_factory=lambda: [TransientError])
    retry_on_strings: List[str] = field(default_factory=lambda: ["Neo.TransientError.Transaction.Terminated"])

class CircuitBreaker:
    """Circuit breaker for handling repeated failures."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
        half_open_timeout: float = 30.0
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        self.failures: Dict[str, List[datetime]] = {}
        self.state: Dict[str, str] = {}  # "closed", "open", "half-open"
        self._lock = Lock()
    
    def record_failure(self, operation_id: str):
        """Record a failure for an operation."""
        with self._lock:
            now = datetime.now()
            if operation_id not in self.failures:
                self.failures[operation_id] = []
            self.failures[operation_id].append(now)
            
            # Clean old failures
            self.failures[operation_id] = [
                f for f in self.failures[operation_id]
                if (now - f).total_seconds() < self.reset_timeout
            ]
            
            # Update state
            if len(self.failures[operation_id]) >= self.failure_threshold:
                self.state[operation_id] = "open"
    
    def record_success(self, operation_id: str):
        """Record a success for an operation."""
        with self._lock:
            self.failures.pop(operation_id, None)
            self.state[operation_id] = "closed"
    
    def can_execute(self, operation_id: str) -> bool:
        """Check if an operation can be executed."""
        with self._lock:
            state = self.state.get(operation_id, "closed")
            if state == "closed":
                return True
            if state == "open":
                # Check if we should move to half-open
                failures = self.failures.get(operation_id, [])
                if failures and (datetime.now() - failures[-1]).total_seconds() > self.half_open_timeout:
                    self.state[operation_id] = "half-open"
                    return True
                return False
            return state == "half-open"

class EnhancedSessionManager:
    """Enhanced Neo4j session management with proper lifecycle tracking and monitoring."""
    
    def __init__(
        self,
        driver: AsyncDriver,
        monitor: Optional['Neo4jIngestMonitor'] = None,
        monitoring_config: Optional[MonitoringConfig] = None,
        max_sessions: int = 100,
        session_timeout: float = 300.0,  # 5 minutes
        cleanup_interval: float = 60.0   # 1 minute
    ):
        self._driver = driver
        self._monitor = monitor
        self._monitoring_config = monitoring_config
        self._max_sessions = max_sessions
        self._session_timeout = session_timeout
        self._cleanup_interval = cleanup_interval
        
        self._sessions: Dict[str, SessionState] = {}
        self._metrics_lock = Lock()
        self._circuit_breaker = CircuitBreaker()
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Start cleanup task
        self._start_cleanup_task()
    
    def _start_cleanup_task(self):
        """Start the background cleanup task."""
        async def cleanup_loop():
            while True:
                try:
                    await self._cleanup_idle_sessions()
                except Exception as e:
                    logger.error(f"Error in session cleanup: {e}")
                await asyncio.sleep(self._cleanup_interval)
        
        self._cleanup_task = asyncio.create_task(cleanup_loop())
    
    async def _cleanup_idle_sessions(self):
        """Clean up idle sessions that have exceeded the timeout."""
        now = datetime.now()
        to_close = []
        
        for session_id, state in self._sessions.items():
            if (
                state.status in [SessionStatus.IDLE, SessionStatus.ACTIVE] and
                (now - state.last_used_at).total_seconds() > self._session_timeout
            ):
                to_close.append(session_id)
        
        for session_id in to_close:
            try:
                await self.close_session(self._sessions[session_id].session)
            except Exception as e:
                logger.error(f"Error closing idle session {session_id}: {e}")
    
    def _generate_session_id(self) -> str:
        """Generate a unique session ID."""
        return f"session_{uuid4().hex}"
    
    async def get_session(
        self,
        database: Optional[str] = None,
        **config
    ) -> AsyncSession:
        """Get a session with proper lifecycle management and monitoring."""
        # Check circuit breaker
        if not self._circuit_breaker.can_execute("session_creation"):
            raise Exception("Session creation is currently blocked due to repeated failures")
        
        try:
            # Create session
            session = await self._driver.session(database=database, **config)
            session_id = self._generate_session_id()
            
            # Create session state
            state = SessionState(
                id=session_id,
                session=session,
                database=database,
                created_at=datetime.now(),
                status=SessionStatus.CREATED,
                last_used_at=datetime.now()
            )
            
            # Track session
            self._sessions[session_id] = state
            
            # Update monitoring
            if self._monitor and self._monitoring_config and self._monitoring_config.collect_session_metrics:
                self._monitor._update_session_metrics(session, "created")
                with self._metrics_lock:
                    self._monitor._neo4j_metrics.set(
                        Neo4jMetrics.SESSIONS_ACTIVE.name,
                        len(self._sessions),
                        labels={"database": database or "default"}
                    )
            
            self._circuit_breaker.record_success("session_creation")
            return session
            
        except Exception as e:
            self._circuit_breaker.record_failure("session_creation")
            logger.error(f"Failed to create session: {e}")
            raise
    
    async def close_session(self, session: AsyncSession):
        """Close a session with proper cleanup and monitoring."""
        session_id = None
        for sid, state in self._sessions.items():
            if state.session == session:
                session_id = sid
                break
        
        if not session_id:
            logger.warning("Attempted to close unknown session")
            return
        
        state = self._sessions[session_id]
        state.status = SessionStatus.CLOSING
        
        try:
            # Update monitoring
            if self._monitor and self._monitoring_config and self._monitoring_config.collect_session_metrics:
                self._monitor._update_session_metrics(session, "closed")
                with self._metrics_lock:
                    self._monitor._neo4j_metrics.set(
                        Neo4jMetrics.SESSIONS_ACTIVE.name,
                        len(self._sessions) - 1,
                        labels={"database": state.database or "default"}
                    )
            
            # Close session
            await session.close()
            state.status = SessionStatus.CLOSED
            
        except Exception as e:
            state.status = SessionStatus.ERROR
            state.errors.append(str(e))
            logger.error(f"Error closing session {session_id}: {e}")
            raise
        
        finally:
            # Cleanup
            self._sessions.pop(session_id, None)
    
    async def close_all_sessions(self):
        """Close all active sessions."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        for state in list(self._sessions.values()):
            try:
                await self.close_session(state.session)
            except Exception as e:
                logger.error(f"Error closing session {state.id}: {e}")
    
    def get_session_state(self, session: AsyncSession) -> Optional[SessionState]:
        """Get the state for a specific session."""
        for state in self._sessions.values():
            if state.session == session:
                return state
        return None
    
    def update_session_metrics(self, session: AsyncSession, metric: str, value: Any = 1):
        """Update metrics for a specific session in a thread-safe way."""
        state = self.get_session_state(session)
        if not state:
            return
        
        with self._metrics_lock:
            if hasattr(state, metric):
                current = getattr(state, metric)
                if isinstance(current, (int, float)):
                    setattr(state, metric, current + value)
                else:
                    setattr(state, metric, value)
            state.last_used_at = datetime.now()
            state.status = SessionStatus.ACTIVE

class EnhancedTransactionManager:
    """Enhanced Neo4j transaction management with proper monitoring and retry logic."""
    
    def __init__(
        self,
        session_manager: EnhancedSessionManager,
        monitor: Optional['Neo4jIngestMonitor'] = None,
        monitoring_config: Optional[MonitoringConfig] = None,
        retry_config: Optional[RetryConfig] = None
    ):
        self._session_manager = session_manager
        self._monitor = monitor
        self._monitoring_config = monitoring_config
        self._retry_config = retry_config or RetryConfig()
        self._circuit_breaker = CircuitBreaker()
        self._active_transactions: Dict[str, TransactionState] = {}
        self._transaction_lock = Lock()
    
    def _generate_transaction_id(self) -> str:
        """Generate a unique transaction ID."""
        return f"tx_{uuid4().hex}"
    
    def _get_retry_delay(self, retry_count: int) -> float:
        """Calculate retry delay based on strategy."""
        if self._retry_config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self._retry_config.initial_delay * (2 ** retry_count)
        elif self._retry_config.strategy == RetryStrategy.LINEAR:
            delay = self._retry_config.initial_delay * (retry_count + 1)
        else:  # CONSTANT
            delay = self._retry_config.initial_delay
        
        return min(delay, self._retry_config.max_delay)
    
    def _should_retry(self, error: Exception) -> bool:
        """Determine if an error should be retried."""
        # Check error type
        if any(isinstance(error, t) for t in self._retry_config.error_types):
            return True
        
        # Check error string
        error_str = str(error)
        return any(s in error_str for s in self._retry_config.retry_on_strings)
    
    async def execute_in_transaction(
        self,
        session: AsyncSession,
        queries: List[Dict[str, Any]],
        retry_on_errors: Optional[List[str]] = None
    ) -> List[Any]:
        """Execute queries in a transaction with enhanced monitoring and retry logic."""
        if retry_on_errors:
            self._retry_config.retry_on_strings = retry_on_errors
        
        session_state = self._session_manager.get_session_state(session)
        if not session_state:
            raise ValueError("Unknown session")
        
        transaction_id = self._generate_transaction_id()
        retries = 0
        
        # Create transaction state
        tx_state = TransactionState(
            id=transaction_id,
            session_id=session_state.id,
            start_time=datetime.now(),
            status=TransactionStatus.STARTED,
            queries=queries
        )
        
        with self._transaction_lock:
            self._active_transactions[transaction_id] = tx_state
        
        while retries <= self._retry_config.max_retries:
            try:
                # Check circuit breaker
                if not self._circuit_breaker.can_execute(f"transaction_{transaction_id}"):
                    raise Exception("Transaction execution is currently blocked due to repeated failures")
                
                # Start transaction
                async with session.begin_transaction() as tx:
                    if self._monitor and self._monitoring_config and self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "started")
                        self._session_manager.update_session_metrics(session, "transactions_started")
                    
                    results = []
                    for query_info in queries:
                        result = await tx.run(
                            query_info["query"],
                            query_info.get("parameters", {})
                        )
                        eager_result = await result.consume()
                        results.append(eager_result)
                        
                        # Update metrics
                        self._session_manager.update_session_metrics(session, "queries_executed")
                        if self._monitor and self._monitoring_config and self._monitoring_config.collect_query_metrics:
                            self._monitor.record_query_result(eager_result)
                    
                    # Commit transaction
                    await tx.commit()
                    tx_state.status = TransactionStatus.COMMITTED
                    
                    if self._monitor and self._monitoring_config and self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "committed")
                        self._session_manager.update_session_metrics(session, "transactions_committed")
                    
                    self._circuit_breaker.record_success(f"transaction_{transaction_id}")
                    return results
                    
            except Exception as e:
                # Update transaction state
                tx_state.status = TransactionStatus.FAILED
                tx_state.error = str(e)
                tx_state.retry_count = retries
                tx_state.last_retry_time = datetime.now()
                
                # Check if we should retry
                should_retry = (
                    retries < self._retry_config.max_retries and
                    self._should_retry(e)
                )
                
                # Update monitoring
                if self._monitor and self._monitoring_config:
                    if self._monitoring_config.collect_transaction_metrics:
                        self._monitor._update_transaction_metrics(session, "rolled_back", str(e))
                        self._session_manager.update_session_metrics(session, "transactions_rolled_back")
                    if self._monitoring_config.collect_error_metrics:
                        self._monitor.record_query_error(e)
                    if should_retry and self._monitoring_config.collect_error_metrics:
                        self._monitor.record_retry(e, self._get_retry_delay(retries))
                
                if not should_retry:
                    self._circuit_breaker.record_failure(f"transaction_{transaction_id}")
                    raise
                
                # Wait before retrying
                await asyncio.sleep(self._get_retry_delay(retries))
                retries += 1
        
        # If we get here, we've exhausted retries
        self._circuit_breaker.record_failure(f"transaction_{transaction_id}")
        raise Exception(f"Transaction failed after {self._retry_config.max_retries} retries")
    
    def get_active_transactions(self, session: AsyncSession) -> List[TransactionState]:
        """Get all active transactions for a session."""
        session_state = self._session_manager.get_session_state(session)
        if not session_state:
            return []
        
        with self._transaction_lock:
            return [
                tx for tx in self._active_transactions.values()
                if tx.session_id == session_state.id and
                tx.status in [TransactionStatus.STARTED, TransactionStatus.FAILED]
            ]
    
    def clear_transaction(self, transaction_id: str):
        """Clear a transaction from tracking."""
        with self._transaction_lock:
            self._active_transactions.pop(transaction_id, None) 