from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Optional

from neo4j import Record, RoutingControl
from nodestream.pipeline import Extractor

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query


class Neo4jRecordWrapper(dict[str, Any]):
    """Thin wrapper around a Neo4j Record that is JSON-serializable.

    - Behaves as a plain dict for serialization / downstream consumers.
    - Keeps a reference to the original Record object for callers that
      need driver-specific metadata.
    """

    def __init__(self, record: Record) -> None:
        # Preserve the original Record for contextual access.
        self.original = record
        # Initialize the dict base class with the record data so that
        # json.dumps can treat this as a normal dictionary.
        super().__init__(record.data())

    def __repr__(self) -> str:
        return f"Neo4jRecordWrapper(data={dict(self)})"


class Neo4jExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        **connection_args,
    ):
        connector = Neo4jDatabaseConnection.from_configuration(**connection_args)
        return cls(query, connector, parameters, limit)

    def __init__(
        self,
        query: str,
        database_connection: Neo4jDatabaseConnection,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
    ) -> None:
        self.database_connection: Neo4jDatabaseConnection = database_connection
        self.query = query
        self.parameters: dict[str, Any] = parameters or {}
        self.limit = limit
        self.logger = getLogger(self.__class__.__name__)

    async def extract_records(self) -> AsyncGenerator[Neo4jRecordWrapper, None]:
        offset = 0
        should_continue = True

        while should_continue:
            params = dict(**self.parameters, limit=self.limit, offset=offset)

            query = Query(
                self.query,
                parameters=params,
            )
            query_results = await self.database_connection.execute(
                query,
                routing_=RoutingControl.READ,
            )
            returned_records = list(query_results)
            should_continue = len(returned_records) > 0
            offset += self.limit
            for record in returned_records:
                yield Neo4jRecordWrapper(record)
