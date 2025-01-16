import asyncio
from logging import getLogger
from typing import Awaitable, Iterable, Tuple, Union

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, Record, RoutingControl
from neo4j.auth_management import AsyncAuthManagers
from neo4j.exceptions import (
    AuthError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)
from nodestream.file_io import LazyLoadedArgument

from .query import Query

RETRYABLE_EXCEPTIONS = (TransientError, ServiceUnavailable, SessionExpired, AuthError)


def auth_provider_factory(
    username: Union[str, LazyLoadedArgument],
    password: Union[str, LazyLoadedArgument],
) -> Awaitable[Tuple[str, str]]:
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
        def driver_factory():
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
        self.driver_factory = driver_factory
        self.database_name = database_name
        self.logger = getLogger(self.__class__.__name__)
        self.max_retry_attempts = max_retry_attempts
        self.retry_factor = retry_factor
        self._driver = None

    def acquire_driver(self) -> AsyncDriver:
        self._driver = self.driver_factory()

    @property
    def driver(self):
        if self._driver is None:
            self.acquire_driver()
        return self._driver

    def log_query_start(self, query: Query):
        self.logger.info(
            "Executing Cypher Query to Neo4j",
            extra={
                "query": query.query_statement,
                "uri": self.driver._pool.address.host,
            },
        )

    def log_record(self, record: Record):
        self.logger.info(
            "Gathered Query Results",
            extra=dict(**record, uri=self.driver._pool.address.host),
        )

    async def _execute_query(
        self,
        query: Query,
        log_result: bool = False,
        routing_=RoutingControl.WRITE,
    ) -> Record:
        result = await self.driver.execute_query(
            query.query_statement,
            query.parameters,
            database_=self.database_name,
            routing_=routing_,
        )
        records = result.records
        if log_result:
            for record in records:
                self.log_record(record)

        return records

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
                    f"Error executing query, retrying. Attempt {attempts + 1}",
                    exc_info=e,
                )
                await asyncio.sleep(self.retry_factor * attempts)
                self.acquire_driver()
                if attempts >= self.max_retry_attempts:
                    raise e

    def session(self) -> AsyncSession:
        return self.driver.session(database=self.database_name)
