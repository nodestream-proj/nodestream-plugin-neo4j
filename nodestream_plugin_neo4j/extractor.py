from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Iterator, Mapping, Optional

from neo4j import Record, RoutingControl
from nodestream.pipeline import Extractor

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query


class Neo4jRecordWrapper(Mapping[str, Any]):
    def __init__(self, record: Record) -> None:
        self.original = record  # Maintained Context Object
        self.data = record.data()  # Dictionary accessible view of the record.

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return f"Neo4jRecordWrapper(data={self.data})"


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
