import asyncio
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from logging import getLogger
from typing import AsyncGenerator, Callable, Coroutine, List, Optional, Tuple

from neo4j import RoutingControl
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.databases.copy import TypeHistogram
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes
from nodestream.pipeline import Extractor
from nodestream.schema import Schema

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

LAST_INGESTED_AT_PROPERTY = "last_ingested_at"

logger = getLogger(__name__)

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
# Mapping helpers (module-level, no retriever state needed)
# ---------------------------------------------------------------------------


def map_neo4j_node_to_nodestream_node(
    node: Neo4jNode, node_type: str, schema: Schema
) -> Node | None:
    if not schema.has_node_of_type(node_type):
        logger.warning("Node type %r not found in schema — skipping record", node_type)
        return None
    node_schema = schema.get_node_type_by_name(node_type)
    properties = PropertySet(node)
    key_values = PropertySet(
        {key_name: properties.pop(key_name) for key_name in node_schema.keys}
    )
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
    relationship: Neo4jRelationship, relationship_type: str
) -> Relationship:
    return Relationship(
        type=relationship_type,
        properties=PropertySet(relationship),
    )


# ---------------------------------------------------------------------------
# Typed extractor subclasses — shard-first design
# ---------------------------------------------------------------------------


class Neo4jNodeExtractor(Extractor):
    """Executes a single pre-bounded node shard query and maps each record.

    The caller is responsible for supplying the Cypher statement and a params
    dict that includes ``shard_offset`` and ``shard_limit`` (plus any filter
    parameters such as ``cutoff``).  Records whose node type is absent from the
    schema are skipped with a warning.  Records whose node type is present but
    missing a declared schema key will raise ``KeyError`` — the key contract is
    strict.
    """

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        node_type: str,
        schema: Schema,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.node_type = node_type
        self.schema = schema

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            node = map_neo4j_node_to_nodestream_node(
                record["n"], node_type=self.node_type, schema=self.schema
            )
            if node is not None:
                yield node.into_ingest()


class Neo4jRelationshipExtractor(Extractor):
    """Executes a single pre-bounded relationship shard query and maps each record.

    The caller is responsible for supplying the Cypher statement and a params
    dict that includes ``shard_offset`` and ``shard_limit`` (plus any filter
    parameters such as ``cutoff``).  Records where either endpoint node type is
    absent from the schema are silently skipped with a warning.
    """

    def __init__(
        self,
        connection: Neo4jDatabaseConnection,
        statement: str,
        params: dict,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema: Schema,
    ) -> None:
        self.connection = connection
        self.statement = statement
        self.params = params
        self.from_node_type = from_node_type
        self.to_node_type = to_node_type
        self.relationship_type = relationship_type
        self.schema = schema

    async def extract_records(self) -> AsyncGenerator:
        results = await self.connection.execute(
            Query(self.statement, self.params), routing_=RoutingControl.READ
        )
        for record in results:
            from_node = map_neo4j_node_to_nodestream_node(
                record["a"], node_type=self.from_node_type, schema=self.schema
            )
            to_node = map_neo4j_node_to_nodestream_node(
                record["b"], node_type=self.to_node_type, schema=self.schema
            )
            if from_node is None or to_node is None:
                continue
            relationship = map_neo4j_relationship_to_nodestream_relationship(
                record["r"], relationship_type=self.relationship_type
            )
            yield RelationshipWithNodes(
                from_node=from_node,
                to_node=to_node,
                relationship=relationship,
            ).into_ingest()


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
        shard_size: int,
        *,
        sample_ratio: int | None = None,
        latest_hours: int | None = None,
        preload_nodes: bool = False,
        distribution: str = DEFAULT_DISTRIBUTION,
    ) -> None:
        super().__init__(schema=schema)
        self.database_connection = database_connection
        self.shard_size = shard_size
        self.sample_ratio = sample_ratio if sample_ratio and sample_ratio > 1 else None
        self.latest_hours = latest_hours
        self.preload_nodes = preload_nodes
        self.histogram: TypeHistogram | None = None
        self.cutoff: datetime | None = None
        if distribution not in DISTRIBUTION_STRATEGIES:
            raise ValueError(
                f"Unknown distribution {distribution!r}. "
                f"Valid options: {list(DISTRIBUTION_STRATEGIES)}"
            )
        self.distribution_strategy: DistributionStrategy = DISTRIBUTION_STRATEGIES[
            distribution
        ]()

    # -- fetch_extractors --------------------------------------------------------

    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        if self.preload_nodes:
            async for extractor in self.fetch_node_extractors():
                yield extractor
        async for extractor in self.fetch_relationship_extractors():
            yield extractor

    async def fetch_node_extractors(self) -> AsyncGenerator[Extractor, None]:
        assert (
            self.histogram is not None and self.cutoff is not None
        ), "build_histogram() must be called before fetch_extractors()"
        node_counts = self.histogram.node_counts
        extractors_by_type = [
            [
                self.build_node_shard_extractor(
                    node_type,
                    self.key_field_for_node_type(node_type),
                    shard_offset,
                    shard_limit,
                )
                for shard_offset, shard_limit in self.compute_shards(
                    count, self.shard_size
                )
            ]
            for node_type, count in node_counts.items()
            if count > 0
        ]
        async for extractor in self.distribution_strategy.distribute(
            extractors_by_type
        ):
            yield extractor

    async def fetch_relationship_extractors(self) -> AsyncGenerator[Extractor, None]:
        assert (
            self.histogram is not None and self.cutoff is not None
        ), "build_histogram() must be called before fetch_extractors()"
        relationship_counts = self.histogram.relationship_counts

        # Build (relationship_type, adjacencies) pairs, skipping types with no adjacencies.
        relationship_type_adjacency_pairs = [
            (relationship_type, adjacencies)
            for relationship_type in relationship_counts
            for adjacencies in [
                list(
                    self.schema.get_adjacencies_by_relationship_type(relationship_type)
                )
            ]
            if adjacencies
        ]

        extractors_by_type = [
            [
                self.build_relationship_shard_extractor(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    relationship_type,
                    self.key_field_for_relationship_type(relationship_type),
                    shard_offset,
                    shard_limit,
                )
                for shard_offset, shard_limit in self.compute_shards(
                    relationship_counts[relationship_type], self.shard_size
                )
                for adjacency in adjacencies
            ]
            for relationship_type, adjacencies in relationship_type_adjacency_pairs
            if relationship_counts[relationship_type] > 0
        ]
        async for extractor in self.distribution_strategy.distribute(
            extractors_by_type
        ):
            yield extractor

    # -- Shard params helper ----------------------------------------------------

    def build_shard_parameters(
        self, cutoff: datetime, shard_offset: int, shard_limit: int
    ) -> dict:
        return dict(
            self.build_filter_parameters(cutoff),
            shard_offset=shard_offset,
            shard_limit=shard_limit,
        )

    # -- Extractor builders -----------------------------------------------------

    def build_node_shard_extractor(
        self,
        node_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
    ) -> Neo4jNodeExtractor:
        where_clause = self.build_where_clause("n")
        if key_field:
            statement = FETCH_NODES_SHARD_QUERY_FORMAT.format(
                type=node_type, where=where_clause, key_field=key_field
            )
        else:
            statement = FETCH_NODES_SHARD_ELEMENTID_QUERY_FORMAT.format(
                type=node_type, where=where_clause
            )
        params = self.build_shard_parameters(self.cutoff, shard_offset, shard_limit)
        return Neo4jNodeExtractor(
            self.database_connection,
            statement,
            params,
            node_type=node_type,
            schema=self.schema,
        )

    def build_relationship_shard_extractor(
        self,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        key_field: Optional[str],
        shard_offset: int,
        shard_limit: int,
    ) -> Neo4jRelationshipExtractor:
        where_clause = self.build_where_clause("r", sample=True)
        if key_field:
            statement = FETCH_RELATIONSHIPS_SHARD_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where_clause,
                key_field=key_field,
            )
        else:
            statement = FETCH_RELATIONSHIPS_SHARD_ELEMENTID_QUERY_FORMAT.format(
                from_node_type=from_node_type,
                relationship_type=relationship_type,
                to_node_type=to_node_type,
                where=where_clause,
            )
        params = self.build_shard_parameters(self.cutoff, shard_offset, shard_limit)
        return Neo4jRelationshipExtractor(
            self.database_connection,
            statement,
            params,
            from_node_type=from_node_type,
            to_node_type=to_node_type,
            relationship_type=relationship_type,
            schema=self.schema,
        )

    # -- Query builders ---------------------------------------------------------

    def build_where_clause(self, cypher_variable: str, *, sample: bool = False) -> str:
        clauses: list[str] = []
        if sample and self.sample_ratio:
            clauses.append(
                f"toInteger(split(elementId({cypher_variable}), ':')[-1]) % {self.sample_ratio} = 0"
            )
        if self.latest_hours is not None:
            clauses.append(
                f"{cypher_variable}.`{LAST_INGESTED_AT_PROPERTY}` >= $cutoff"
            )
        if not clauses:
            return ""
        return "WHERE " + " AND ".join(clauses) + "\n"

    def build_filter_parameters(self, cutoff: datetime) -> dict[str, object]:
        return {"cutoff": cutoff}

    # -- Count helpers ----------------------------------------------------------

    async def preview_node_count(self, node_type: str, cutoff: datetime) -> int:
        where_clause = self.build_where_clause("n")
        statement = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(
            type=node_type, where=where_clause
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first_result = next(iter(results), None)
        return int(first_result["count"]) if first_result is not None else 0

    async def preview_relationship_count(
        self, relationship_type: str, cutoff: datetime
    ) -> int:
        where_clause = self.build_where_clause("r", sample=True)
        statement = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
            relationship_type=relationship_type, where=where_clause
        )
        results = await self.database_connection.execute(
            Query(statement, self.build_filter_parameters(cutoff)),
            routing_=RoutingControl.READ,
        )
        first_result = next(iter(results), None)
        return int(first_result["count"]) if first_result is not None else 0

    async def gather_counts(
        self,
        types: List[str],
        count_function: Callable[[str, datetime], Coroutine],
        cutoff: datetime,
    ) -> dict:
        counts = await asyncio.gather(
            *(count_function(type_name, cutoff=cutoff) for type_name in types)
        )
        return dict(zip(types, counts))

    async def build_histogram(self) -> TypeHistogram:
        self.cutoff = self.snapshot_cutoff()
        node_types = [node_type.name for node_type in self.schema.nodes]
        relationship_types = [
            relationship_type.name for relationship_type in self.schema.relationships
        ]
        node_counts = await self.gather_counts(
            node_types, self.preview_node_count, self.cutoff
        )
        relationship_counts = await self.gather_counts(
            relationship_types, self.preview_relationship_count, self.cutoff
        )
        self.histogram = TypeHistogram(
            node_counts=node_counts, relationship_counts=relationship_counts
        )
        return self.histogram

    def compute_shards(
        self, total_count: int, shard_size: int
    ) -> List[Tuple[int, int]]:
        if total_count <= 0 or shard_size <= 0:
            return []
        number_of_shards = math.ceil(total_count / shard_size)
        return [
            (
                shard_index * shard_size,
                min(shard_size, total_count - shard_index * shard_size),
            )
            for shard_index in range(number_of_shards)
        ]

    def snapshot_cutoff(self) -> datetime:
        """Return the snapshot upper bound for this run.

        When ``latest_hours`` is set the cutoff is pushed back by that many
        hours so only recently-ingested records are included.  When it is not
        set the cutoff is simply *now*, acting as a guaranteed upper-bound
        anchor so no records ingested after the run started can slip in.
        """
        now = datetime.now(timezone.utc)
        if self.latest_hours is not None:
            return now - timedelta(hours=self.latest_hours)
        return now

    def key_field_for_node_type(self, node_type: str) -> Optional[str]:
        """Return the field to ORDER BY when paginating nodes, or None for elementId.

        Priority:
        1. ``last_ingested_at`` when recency filtering is active — keeps ordering
           consistent with the WHERE clause filter.
        2. The first declared schema key — if a key is specified in the schema it
           is assumed to exist in the source graph, even for non-nodestream graphs.
        3. None → caller falls back to elementId(n), which is always present on
           any Neo4j graph regardless of origin.
        """
        if self.latest_hours is not None:
            return LAST_INGESTED_AT_PROPERTY
        node_schema = self.schema.get_node_type_by_name(node_type)
        if node_schema and node_schema.keys:
            return next(iter(node_schema.keys))
        return None

    def key_field_for_relationship_type(self, relationship_type: str) -> Optional[str]:
        return LAST_INGESTED_AT_PROPERTY if self.latest_hours is not None else None
