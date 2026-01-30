from logging import getLogger
from typing import Any, Dict, Optional

from neo4j import RoutingControl
from nodestream.pipeline import Extractor

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query


class Neo4jExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        is_apoc: bool = False,
        is_implicit: bool = False,
        **connection_args,
    ):
        connector = Neo4jDatabaseConnection.from_configuration(**connection_args)
        return cls(
            query,
            connector,
            parameters,
            limit,
            is_apoc=is_apoc,
            is_implicit=is_implicit,
        )

    def __init__(
        self,
        query: str,
        database_connection: Neo4jDatabaseConnection,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        is_apoc: bool = False,
        is_implicit: bool = False,
    ) -> None:
        self.database_connection = database_connection
        self.query = query
        self.parameters = parameters or {}
        self.limit = limit
        self.is_apoc = is_apoc
        self.is_implicit = is_implicit
        self.logger = getLogger(self.__class__.__name__)

    async def extract_records(self):
        offset = 0
        should_continue = True

        while should_continue:
            params = dict(**self.parameters, limit=self.limit, offset=offset)
            self.logger.info(
                "Running query on neo4j",
                extra=dict(query=self.query, params=params),
            )

            query = Query(
                self.query,
                parameters=params,
                is_apoc=self.is_apoc,
                is_implicit=self.is_implicit,
            )
            query_results = await self.database_connection.execute(
                query,
                routing_=RoutingControl.READ,
            )
            returned_records = list(query_results)
            should_continue = len(returned_records) > 0
            offset += self.limit
            for item in returned_records:
                yield item
