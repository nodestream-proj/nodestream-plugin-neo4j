from logging import getLogger
from typing import Iterable, Optional, Dict, Any, List, Union
from neo4j import AsyncDriver, AsyncSession, EagerResult, Record
import asyncio
from datetime import datetime

from nodestream.databases.query_executor import (
    QueryExecutor,
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
)
from nodestream.model import (
    IngestionHook,
    Node,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)

from .neo4j_database import Neo4jDatabaseConnection
from .ingest_query_builder import Neo4jIngestQueryBuilder
from .query import Query, QueryBatch
from .monitoring import MonitoringConfig, MonitoringLevel
from .metrics import Neo4jMetrics, Neo4jMetricHandler
from .neo4j_ingest_monitor import Neo4jIngestMonitor
from .enhanced_components import (
    Neo4jExecutorComponents,
    EnhancedSessionManager,
    EnhancedTransactionManager,
    RetryConfig,
    RetryStrategy,
    SessionStatus,
    TransactionStatus
)


class Neo4jQueryExecutor(QueryExecutor):
    """Executes queries against Neo4j with enhanced component and lifecycle management."""
    
    def __init__(
        self,
        components: Neo4jExecutorComponents,
        **kwargs
    ):
        """Initialize the executor with enhanced component management.
        
        Args:
            components: Container for all executor components
            **kwargs: Additional arguments for the parent class
        """
        super().__init__(**kwargs)
        self._components = components
        self._driver = components.database_connection.driver
        
        # Initialize enhanced managers
        self._session_manager = EnhancedSessionManager(
            self._driver,
            components.monitor,
            components.monitoring_config,
            max_sessions=100,  # Configurable
            session_timeout=300.0,  # 5 minutes
            cleanup_interval=60.0  # 1 minute
        )
        
        # Configure retry behavior
        retry_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL,
            max_retries=components.retries_per_chunk,
            initial_delay=1.0,
            max_delay=30.0
        )
        
        self._transaction_manager = EnhancedTransactionManager(
            self._session_manager,
            components.monitor,
            components.monitoring_config,
            retry_config=retry_config
        )
        
        self.logger = getLogger(self.__class__.__name__)
    
    async def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        session_config: Optional[Dict[str, Any]] = None,
        retry_on_errors: Optional[List[str]] = None
    ) -> EagerResult:
        """Execute a query with enhanced session and monitoring management."""
        session = None
        try:
            # Get session with proper lifecycle management
            session = await self._session_manager.get_session(
                database=database,
                **(session_config or {})
            )
            
            # Execute query with enhanced retry logic
            result = await self._transaction_manager.execute_in_transaction(
                session,
                [{"query": query, "parameters": parameters or {}}],
                retry_on_errors=retry_on_errors
            )
            return result[0]  # We know we only executed one query
            
        except Exception as e:
            # Error handling is done in transaction manager
            self.logger.error(f"Query execution failed: {e}")
            raise
            
        finally:
            if session:
                try:
                    await self._session_manager.close_session(session)
                except Exception as e:
                    self.logger.error(f"Error closing session: {e}")
    
    async def execute_batch(
        self,
        query: str,
        batch: List[Dict[str, Any]],
        batch_size: int = None,
        database: Optional[str] = None,
        session_config: Optional[Dict[str, Any]] = None,
        retry_on_errors: Optional[List[str]] = None
    ) -> List[EagerResult]:
        """Execute a query in batches with enhanced session and monitoring management."""
        batch_size = batch_size or self._components.chunk_size
        results = []
        
        for i in range(0, len(batch), batch_size):
            current_batch = batch[i:i + batch_size]
            try:
                result = await self.execute_query(
                    query,
                    parameters={"batch": current_batch},
                    database=database,
                    session_config=session_config,
                    retry_on_errors=retry_on_errors
                )
                results.append(result)
            except Exception as e:
                self.logger.error(f"Batch execution failed at batch {i}: {e}")
                raise
                
        return results
    
    async def execute_transaction(
        self,
        queries: List[Dict[str, Any]],
        database: Optional[str] = None,
        session_config: Optional[Dict[str, Any]] = None,
        retry_on_errors: Optional[List[str]] = None
    ) -> List[EagerResult]:
        """Execute multiple queries in a transaction with enhanced session and monitoring management."""
        session = None
        try:
            # Get session with proper lifecycle management
            session = await self._session_manager.get_session(
                database=database,
                **(session_config or {})
            )
            
            # Execute queries in transaction with enhanced retry logic
            return await self._transaction_manager.execute_in_transaction(
                session,
                queries,
                retry_on_errors=retry_on_errors
            )
            
        except Exception as e:
            self.logger.error(f"Transaction execution failed: {e}")
            raise
            
        finally:
            if session:
                try:
                    await self._session_manager.close_session(session)
                except Exception as e:
                    self.logger.error(f"Error closing session: {e}")
    
    async def execute_query_batch(self, batch: QueryBatch):
        """Execute a query batch with enhanced monitoring."""
        await self.execute_query(
            batch.as_query(
                self._components.ingest_query_builder.apoc_iterate,
                chunk_size=self._components.chunk_size,
                execute_chunks_in_parallel=self._components.execute_chunks_in_parallel,
                retries_per_chunk=self._components.retries_per_chunk,
            )
        )
    
    async def upsert_nodes_in_bulk_with_same_operation(
        self,
        operation: OperationOnNodeIdentity,
        nodes: Iterable[Node]
    ):
        """Upsert nodes in bulk with enhanced monitoring."""
        batched_query = (
            self._components.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation,
                nodes
            )
        )
        await self.execute_query_batch(batched_query)
    
    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        """Upsert relationships in bulk with enhanced monitoring."""
        batched_query = (
            self._components.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape,
                relationships
            )
        )
        await self.execute_query_batch(batched_query)
    
    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        """Perform TTL operation with enhanced monitoring."""
        query = self._components.ingest_query_builder.generate_ttl_query_from_configuration(
            config,
            retries_per_chunk=self._components.retries_per_chunk,
        )
        await self.execute_query(query)
    
    async def execute_hook(self, hook: IngestionHook):
        """Execute a hook with enhanced monitoring."""
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.execute_query(query_string, params)
    
    def get_monitoring_stats(self) -> Optional[Dict[str, Any]]:
        """Get current monitoring statistics if monitoring is enabled."""
        if not self._components.monitor or not self._components.monitoring_config.enabled:
            return None
        
        stats = self._components.monitor.get_query_statistics()
        
        # Add enhanced metrics
        if stats:
            # Add session metrics
            stats["sessions"] = {
                "active": len(self._session_manager._sessions),
                "states": {
                    status.value: len([
                        s for s in self._session_manager._sessions.values()
                        if s.status == status
                    ])
                    for status in SessionStatus
                }
            }
            
            # Add transaction metrics
            stats["transactions"] = {
                "active": len(self._transaction_manager._active_transactions),
                "states": {
                    status.value: len([
                        t for t in self._transaction_manager._active_transactions.values()
                        if t.status == status
                    ])
                    for status in TransactionStatus
                }
            }
        
        return stats
    
    @classmethod
    def create_with_monitoring(
        cls,
        database_connection: Neo4jDatabaseConnection,
        ingest_query_builder: Neo4jIngestQueryBuilder,
        level: MonitoringLevel = MonitoringLevel.BASIC,
        **monitoring_config_kwargs
    ) -> 'Neo4jQueryExecutor':
        """Create a query executor with enhanced monitoring enabled."""
        config = MonitoringConfig(
            enabled=True,
            level=level,
            **monitoring_config_kwargs
        )
        monitor = Neo4jIngestMonitor(driver=database_connection.driver) if config.enabled else None
        
        components = Neo4jExecutorComponents(
            database_connection=database_connection,
            ingest_query_builder=ingest_query_builder,
            monitor=monitor,
            monitoring_config=config
        )
        
        return cls(components)
    
    async def close(self):
        """Close all active sessions and cleanup resources."""
        try:
            await self._session_manager.close_all_sessions()
        except Exception as e:
            self.logger.error(f"Error during executor cleanup: {e}")
            raise
