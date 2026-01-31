import time
from dataclasses import dataclass as _unused_dataclass  # noqa: F401
from logging import getLogger
from typing import Awaitable, Callable, Iterable, Tuple, Union

from neo4j import (
    READ_ACCESS,
    WRITE_ACCESS,
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncResult,
    AsyncSession,
    EagerResult,
    Record,
    ResultSummary,
    RoutingControl,
)
from neo4j.auth_management import AsyncAuthManagers
from neo4j.exceptions import (
    AuthError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)
from nodestream.file_io import LazyLoadedArgument

from .query import Query
from .result import Neo4jQueryStatistics, Neo4jResult

RETRYABLE_EXCEPTIONS = (TransientError, ServiceUnavailable, SessionExpired, AuthError)


def convert_routing_control_to_access_mode(routing_control: RoutingControl) -> str:
    return READ_ACCESS if routing_control == RoutingControl.READ else WRITE_ACCESS


def auth_provider_factory(
    username: Union[str, LazyLoadedArgument],
    password: Union[str, LazyLoadedArgument],
) -> Callable[[], Awaitable[Tuple[str, str]]]:
    logger = getLogger(__name__)

    async def auth_provider():
        logger.info("Fetching new neo4j credentials")

        if isinstance(username, LazyLoadedArgument):
            logger.debug("Fetching username since value is lazy loaded")
            current_username = username.get_value()
        else:
            current_username = username

        if isinstance(password, LazyLoadedArgument):
            logger.debug("Fetching password since value is lazy loaded")
            current_password = password.get_value()
        else:
            current_password = password

        return current_username, current_password

    return auth_provider


class Neo4jDatabaseConnection:
    @classmethod
    def from_configuration(
        cls,
        uri: str,
        username: Union[str, LazyLoadedArgument],
        password: Union[str, LazyLoadedArgument],
        database_name: str = "neo4j",
        max_retry_attempts: int = 3,
        retry_factor: int = 1,
        **driver_kwargs,
    ):
        def driver_factory() -> AsyncDriver:
            auth = AsyncAuthManagers.basic(auth_provider_factory(username, password))
            return AsyncGraphDatabase.driver(uri, auth=auth, **driver_kwargs)

        return cls(driver_factory, database_name, max_retry_attempts, retry_factor)

    def __init__(
        self,
        driver_factory,
        database_name: str,
        max_retry_attempts: int = 3,
        retry_factor: float = 1,
    ) -> None:
        self.driver_factory: Callable[[], AsyncDriver] = driver_factory
        self.database_name = database_name
        self.logger = getLogger(self.__class__.__name__)
        self.max_retry_attempts = max_retry_attempts
        self.retry_factor = retry_factor
        self._driver: AsyncDriver | None = None
        self.query_set: set[str] = set()

    def acquire_driver(self) -> AsyncDriver:
        self._driver = self.driver_factory()
        return self._driver

    @property
    def driver(self) -> AsyncDriver:
        if self._driver is None:
            return self.acquire_driver()
        return self._driver

    def log_query_start(self, query: Query):
        if query.query_statement not in self.query_set:
            self.logger.info(
                "Executing Cypher Query to Neo4j.",
                extra={
                    "query": query.query_statement,
                    "uri": self.driver._pool.address.host,
                },
            )
            self.query_set.add(query.query_statement)

    async def _execute_query(
        self,
        query: Query,
        log_result: bool = False,
        routing_=RoutingControl.WRITE,
    ) -> Iterable[Record]:
        if query.is_implicit:
            return await self.execute_implicit_query(query, log_result, routing_)
        return await self.execute_explicit_query(query, log_result, routing_)

    async def execute_implicit_query(
        self,
        query: Query,
        log_result: bool = False,
        routing_=RoutingControl.WRITE,
    ) -> Iterable[Record]:
        # For implicit transactions, Neo4j's session API expects an access mode
        # (`READ_ACCESS` / `WRITE_ACCESS`), not a `RoutingControl` value. Map
        # the routing hint onto the appropriate access mode here.
        access_mode = convert_routing_control_to_access_mode(routing_)

        async with self.driver.session(
            database=self.database_name,
            default_access_mode=access_mode,
        ) as session:
            # TODO: we need to use Neo4j's Query classes to avoid string interpolation in the future for injection protection.
            async_result: AsyncResult = await session.run(
                query.query_statement,
                parameters=query.parameters,
            )  # type: ignore
            records: list[Record] = [record async for record in async_result]
            keys_list: list[str] = list(async_result.keys())
            summary: ResultSummary = await async_result.consume()
            result = Neo4jResult(query, records, keys_list, summary)
        return self._finalize_query_result(query, result, log_result)

    async def execute_explicit_query(
        self,
        query: Query,
        log_result: bool = False,
        routing_=RoutingControl.WRITE,
    ) -> Iterable[Record]:
        # TODO we need to use Neo4j's Query classes to avoid string interpolation in the future for injection protection.
        native: EagerResult = await self.driver.execute_query(
            query.query_statement,
            query.parameters,
            database_=self.database_name,
            routing_=routing_,
        )  # type: ignore
        result = Neo4jResult(
            query, list(native.records), list(native.keys), native.summary
        )
        return self._finalize_query_result(query, result, log_result)

    def _finalize_query_result(
        self,
        query: Query,
        result: Neo4jResult,
        log_result: bool,
    ) -> Iterable[Record]:
        if log_result:
            statistics: Neo4jQueryStatistics = result.obtain_query_statistics()
            self.log_error_messages_from_statistics(statistics)
            statistics.update_metrics_from_summary()
        return result.records

    def log_error_messages_from_statistics(self, statistics: Neo4jQueryStatistics):
        for error in statistics.error_messages:
            self.logger.info("Query Error Occurred.", extra={"error": error})

    async def execute(
        self,
        query: Query,
        log_result: bool = False,
        routing_=RoutingControl.WRITE,
    ) -> Iterable[Record]:
        self.log_query_start(query)
        attempts = 0
        while True:
            attempts += 1
            try:
                return await self._execute_query(query, log_result, routing_)
            except RETRYABLE_EXCEPTIONS as e:
                self.logger.warning(
                    "Error executing query, retrying. Attempt %s",
                    attempts,
                    exc_info=e,
                )
                # Block synchronously before retrying (do not yield to event loop).
                time.sleep(self.retry_factor * attempts)
                self.acquire_driver()
                if attempts >= self.max_retry_attempts:
                    raise e

    def session(self) -> AsyncSession:
        return self.driver.session(database=self.database_name)
