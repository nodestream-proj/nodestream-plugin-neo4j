import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from typing import AsyncGenerator, Callable, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import TypeHistogram
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
from nodestream.pipeline import Extractor
from nodestream.schema import Schema

from .extractor import Neo4jExtractor
from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

LAST_INGESTED_AT_PROPERTY = "last_ingested_at"

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """\
MATCH (n:{type})
{where}RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}RETURN a, r, b SKIP $offset LIMIT $limit
"""

FETCH_NODES_SHARD_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY n.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN n
"""

FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY r.`{key_field}` SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b
"""

FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (n:{type})
{where}WITH n ORDER BY elementId(n) SKIP $shard_offset LIMIT $shard_limit
RETURN n
"""

FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT = """\
MATCH (a:{from_node_type})-[r:{relationship_type}]->(b:{to_node_type})
{where}WITH a, r, b ORDER BY elementId(r) SKIP $shard_offset LIMIT $shard_limit
RETURN a, r, b
"""


COUNT_NODES_BY_TYPE_QUERY_FORMAT = """\
MATCH (n:{type})
{where}RETURN count(n) AS count
"""

COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """\
MATCH ()-[r:{relationship_type}]->()
{where}RETURN count(r) AS count
"""


# ---------------------------------------------------------------------------
# Extractor base classes
# ---------------------------------------------------------------------------


class Neo4jMappingExtractor(Extractor):
    """Paginates a Neo4j query and maps each record."""

    def __init__(self, inner: Neo4jExtractor, map_record: Callable) -> None:
        self.inner = inner
        self.map_record = map_record

    async def extract_records(self) -> AsyncGenerator:
        async for record in self.inner.extract_records():
            yield self.map_record(record)


class Neo4jShardExtractor(Extractor):
    """Executes a single pre-bounded shard query and maps each record."""

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        map_record: Callable,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.map_record = map_record

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            yield self.map_record(record)


# ---------------------------------------------------------------------------
# Distribution strategies
# ---------------------------------------------------------------------------


class DistributionStrategy(ABC):
    """Controls the order in which per-type extractor lists are interleaved."""

    @abstractmethod
    def distribute(
        self, extractors_by_type: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        ...  # pragma: no cover


class SequentialDistribution(DistributionStrategy):
    """Drain all shards of one type before moving to the next."""

    async def distribute(
        self, extractors_by_type: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        for extractors in extractors_by_type:
            for extractor in extractors:
                yield extractor


class RoundRobinDistribution(DistributionStrategy):
    """Yield one shard per type in rotation, so all types make progress together."""

    async def distribute(
        self, extractors_by_type: List[List[Extractor]]
    ) -> AsyncGenerator[Extractor, None]:
        sentinel = object()
        for column in zip_longest(*extractors_by_type, fillvalue=sentinel):
            for extractor in column:
                if extractor is not sentinel:
                    yield extractor


DISTRIBUTION_STRATEGIES = {
    "sequential": SequentialDistribution,
    "round_robin": RoundRobinDistribution,
}
DEFAULT_DISTRIBUTION = "sequential"


# ---------------------------------------------------------------------------
# Retriever
# ---------------------------------------------------------------------------


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        schema: Schema,
        limit: int = 1000,
        *,
        node_types: List[str] | None = None,
        relationship_types: List[str] | None = None,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
        node_only: bool = False,
        shard_size: int | None = None,
        distribution: str = DEFAULT_DISTRIBUTION,
        max_shards_per_type: int = 10000,
    ) -> None:
        super().__init__(schema=schema)
        self.database_connection = database_connection
        self.limit = limit
        self.node_types = (
            node_types if node_types is not None else [n.name for n in schema.nodes]
        )
        self.relationship_types = (
            relationship_types
            if relationship_types is not None
            else [r.name for r in schema.relationships]
        )
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.node_only = node_only
        self.shard_size = shard_size
        self.max_shards_per_type = max_shards_per_type
        self.distribution_strategy: DistributionStrategy = DISTRIBUTION_STRATEGIES.get(
            distribution, SequentialDistribution
        )()

    # -- fetch_extractors --------------------------------------------------------

    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        if self.node_only:
            async for extractor in self._fetch_node_extractors():
                yield extractor
        else:
            async for extractor in self._fetch_rel_extractors():
                yield extractor

    async def _fetch_node_extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self._snapshot_cutoff()
        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(self.preview_node_count(t, cutoff=cutoff) for t in self.node_types)
            )
            extractors_by_type = [
                [
                    self._build_node_shard_extractor(
                        node_type,
                        self.key_field_for_node_type(node_type, schema),
                        offset,
                        limit,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for offset, limit in self._compute_shards(count, self.shard_size)
                ]
                for node_type, count in zip(self.node_types, counts)
                if count > 0
            ]
        else:
            extractors_by_type = [
                [self._build_node_extractor(node_type, schema=schema, cutoff=cutoff)]
                for node_type in self.node_types
            ]
        async for extractor in self.distribution_strategy.distribute(
            extractors_by_type
        ):
            yield extractor

    async def _fetch_rel_extractors(self) -> AsyncGenerator[Extractor, None]:
        schema = self.schema
        cutoff = self._snapshot_cutoff()

        # Build (rel_type, adjacencies) pairs, skipping types with no adjacencies.
        rel_type_adj = [
            (rel_type, list(schema.get_adjacencies_by_relationship_type(rel_type)))
            for rel_type in self.relationship_types
        ]
        rel_type_adj = [(rt, adjs) for rt, adjs in rel_type_adj if adjs]

        if self.shard_size is not None:
            counts = await asyncio.gather(
                *(
                    self.preview_relationship_count(rt, cutoff=cutoff)
                    for rt, _ in rel_type_adj
                )
            )
            extractors_by_type = [
                [
                    self._build_rel_shard_extractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        rel_type,
                        self.key_field_for_relationship_type(rel_type, schema),
                        offset,
                        limit,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for offset, limit in self._compute_shards(count, self.shard_size)
                    for adjacency in adjacencies
                ]
                for (rel_type, adjacencies), count in zip(rel_type_adj, counts)
                if count > 0
            ]
        else:
            extractors_by_type = [
                [
                    self._build_rel_extractor(
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        rel_type,
                        schema=schema,
                        cutoff=cutoff,
                    )
                    for adjacency in adjacencies
                ]
                for rel_type, adjacencies in rel_type_adj
            ]
        async for extractor in self.distribution_strategy.distribute(
            extractors_by_type
        ):
            yield extractor

    # -- Mapping helpers --------------------------------------------------------

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, node_type: str, schema: Schema | None = None
    ) -> Node:
        properties = PropertySet(node)
        key_values = PropertySet.empty()
        if schema is not None:
            node_schema = schema.get_node_type_by_name(node_type)
            if node_schema is not None:
                for key in node_schema.keys:
                    if key in properties:
                        key_values[key] = properties.pop(key)
        additional_types: Tuple[str, ...] = tuple(
            label for label in node.labels if label != node_type
        )
        return Node(
            type=node_type,
            properties=properties,
            key_values=key_values,
            additional_types=additional_types,
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship, relationship_type: str
    ) -> Relationship:
        return Relationship(
            type=relationship_type,
            properties=PropertySet(relationship),
        )

    # -- Record mapper helpers --------------------------------------------------

    def _node_ingest_mapper(
        self, record, node_type: str, schema, *, from_shard: bool = False
    ) -> object:
        raw = record["n"] if from_shard else record.original["n"]
        return self.map_neo4j_node_to_nodestream_node(
            raw, node_type=node_type, schema=schema
        ).into_ingest()

    def _rel_ingest_mapper(
        self,
        record,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema,
        *,
        from_shard: bool = False,
    ) -> object:
        rec = record if from_shard else record.original
        return RelationshipWithNodes(
            from_node=self.map_neo4j_node_to_nodestream_node(
                rec["a"], node_type=from_node_type, schema=schema
            ),
            to_node=self.map_neo4j_node_to_nodestream_node(
                rec["b"], node_type=to_node_type, schema=schema
            ),
            relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                rec["r"], relationship_type=relationship_type
            ),
        ).into_ingest()

    # -- Shard params helper ----------------------------------------------------

    def _shard_params(self, cutoff, shard_offset: int, shard_limit: int) -> dict:
        return dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )

    # -- Extractor builders -----------------------------------------------------

    def _build_node_extractor(
        self,
        node_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("n")
        inner = Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            lambda record, nt=node_type: self._node_ingest_mapper(record, nt, schema),
        )

    def _build_node_shard_extractor(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("n")
        if key_field:
            statement = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=node_type, where=where, key_field=key_field
            )
        else:
            statement = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=node_type, where=where
            )
        params = self._shard_params(cutoff, shard_offset, shard_limit)
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            lambda record, nt=node_type: self._node_ingest_mapper(
                record, nt, schema, from_shard=True
            ),
        )

    def _build_rel_extractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("r", sample=True)
        inner = Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_BETWEEN_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
            ),
            self.database_connection,
            parameters=self.build_filter_parameters(cutoff),
            limit=self.limit,
        )
        return Neo4jMappingExtractor(
            inner,
            lambda record, fnt=from_node_type, tnt=to_node_type, rt=relationship_type: self._rel_ingest_mapper(
                record, fnt, tnt, rt, schema
            ),
        )

    def _build_rel_shard_extractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
        schema: Schema | None = None,
        cutoff: datetime | None = None,
    ) -> Extractor:
        where = self.build_where_clause("r", sample=True)
        if key_field:
            statement = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
                key_field=key_field,
            )
        else:
            statement = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where,
            )
        params = self._shard_params(cutoff, shard_offset, shard_limit)
        return Neo4jShardExtractor(
            self.database_connection,
            statement,
            params,
            lambda record, fnt=from_node_type, tnt=to_node_type, rt=relationship_type: self._rel_ingest_mapper(
                record, fnt, tnt, rt, schema, from_shard=True
            ),
        )

    # -- Query builders ---------------------------------------------------------

    def build_where_clause(self, var: str, *, sample: bool = False) -> str:
        clauses: list[str] = []
        if sample and self.sample_ratio:
            clauses.append(
                f"toInteger(split(elementId({var}), ':')[-1]) % {self.sample_ratio} = 0"
            )
        if self.latest_hours is not None:
            clauses.append(f"{var}.`{LAST_INGESTED_AT_PROPERTY}` >= $cutoff")
        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses) + "\n"

    def build_filter_parameters(
        self, cutoff: datetime | None = None
    ) -> dict[str, object]:
        if self.latest_hours is None:
            return {}
        if cutoff is None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)
        return {"cutoff": cutoff}

    # -- Count helpers ----------------------------------------------------------

    async def preview_node_count(
        self, node_type: str, cutoff: datetime | None = None
    ) -> int:
        where = self.build_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type, where=where)
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def preview_relationship_count(
        self, relationship_type: str, cutoff: datetime | None = None
    ) -> int:
        where = self.build_where_clause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first = next(iter(results), None)
        return int(first["count"]) if first is not None else 0

    async def _gather_counts(self, types, count_fn, cutoff) -> dict:
        counts = await asyncio.gather(*(count_fn(t, cutoff=cutoff) for t in types))
        return dict(zip(types, counts))

    async def build_histogram(self) -> TypeHistogram:
        cutoff = self._snapshot_cutoff()
        node_counts = await self._gather_counts(
            self.node_types, self.preview_node_count, cutoff
        )
        rel_counts = {}
        if not self.node_only:
            rel_counts = await self._gather_counts(
                self.relationship_types, self.preview_relationship_count, cutoff
            )
        return TypeHistogram(node_counts=node_counts, relationship_counts=rel_counts)

    def _compute_shards(
        self, total_count: int, shard_size: int
    ) -> List[Tuple[int, int]]:
        if total_count <= 0 or shard_size <= 0:
            return []
        num_shards = math.ceil(total_count / shard_size)
        return [
            (i * shard_size, min(shard_size, total_count - i * shard_size))
            for i in range(num_shards)
        ]

    def _snapshot_cutoff(self) -> datetime | None:
        if self.latest_hours is None:
            return None
        return datetime.now(timezone.utc) - timedelta(hours=self.latest_hours)

    def key_field_for_node_type(self, node_type: str, schema: Schema) -> Optional[str]:
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        node_schema = schema.get_node_type_by_name(node_type)
        if node_schema and node_schema.keys:
            return next(iter(node_schema.keys))
        return None

    def key_field_for_relationship_type(
        self, relationship_type: str, schema: Schema
    ) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None
