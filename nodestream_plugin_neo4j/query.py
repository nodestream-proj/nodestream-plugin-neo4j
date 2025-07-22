from dataclasses import dataclass, field
from typing import Any


@dataclass
class ApocUpdateStatistics:
    """Type definition for APOC update statistics."""

    relationshipsDeleted: int = 0  # Number of relationships deleted
    relationshipsCreated: int = 0  # Number of relationships created
    nodesDeleted: int = 0  # Number of nodes deleted
    nodesCreated: int = 0  # Number of nodes created
    labelsRemoved: int = 0  # Number of labels removed
    labelsAdded: int = 0  # Number of labels added
    propertiesSet: int = 0  # Number of properties set


@dataclass
class ApocBatchResponse:
    """Type definition for APOC periodic.iterate response fields."""

    batches: int = 0  # Number of batches processed
    total: int = 0  # Total number of operations
    timeTaken: int = 0  # Time taken in milliseconds
    committedOperations: int = 0  # Number of operations committed
    failedOperations: int = 0  # Number of operations that failed
    failedBatches: int = 0  # Number of batches that failed
    retries: int = 0  # Number of retries performed
    errorMessages: dict[str, Any] = field(default_factory=dict)  # Map of error messages
    wasTerminated: bool = False  # Whether the operation was terminated
    updateStatistics: ApocUpdateStatistics = field(default_factory=ApocUpdateStatistics)


# Get fields from ApocBatchResponse dataclass
APOC_BATCH_QUERY_RESPONSE_FIELDS = [
    field.name for field in ApocBatchResponse.__dataclass_fields__.values()
]

UNWIND_QUERY = "UNWIND $batched_parameter_sets as params RETURN params"

# Build the YIELD and RETURN clauses using the standardized fields
YIELD_CLAUSE = f"YIELD {', '.join(APOC_BATCH_QUERY_RESPONSE_FIELDS)}"
RETURN_CLAUSE = f"RETURN {', '.join(APOC_BATCH_QUERY_RESPONSE_FIELDS)}"

COMMIT_QUERY = f"""
CALL apoc.periodic.iterate(
    $iterable_query,
    $batched_query,
    {{batchSize: $chunk_size, parallel: $execute_chunks_in_parallel, retries: $retries_per_chunk, params: $iterate_params}}
)
{YIELD_CLAUSE}
{RETURN_CLAUSE}
"""

NON_APOC_COMMIT_QUERY = """
UNWIND $iterate_params.batched_parameter_sets AS param
CALL apoc.cypher.doIt($batched_query, {params: param})
YIELD value
RETURN value
"""


@dataclass(slots=True, frozen=True)
class Query:
    query_statement: str
    parameters: dict[str, Any]
    is_apoc: bool = False  # Indicates if this is an APOC query

    @classmethod
    def from_statement(cls, query_statement: str, **parameters: Any) -> "Query":
        return cls(query_statement, parameters)

    def feed_batched_query(
        self,
        batched_query: str,
        chunk_size: int = 1000,
        execute_chunks_in_parallel: bool = True,
        retries_per_chunk: int = 3,
    ) -> "Query":
        """Feed the results of the the query into another query that will be executed in batches."""
        return Query(
            COMMIT_QUERY,
            {
                "iterate_params": self.parameters,
                "batched_query": batched_query,
                "iterable_query": self.query_statement,
                "execute_chunks_in_parallel": execute_chunks_in_parallel,
                "chunk_size": chunk_size,
                "retries_per_chunk": retries_per_chunk,
            },
            is_apoc=True,  # This is an APOC query
        )


@dataclass(slots=True, frozen=True)
class QueryBatch:
    query_statement: str
    batched_parameter_sets: list[dict[str, Any]]
    is_apoc: bool = False  # Indicates if this is an APOC query

    def as_query(
        self,
        apoc_iterate: bool,
        chunk_size: int = 1000,
        execute_chunks_in_parallel: bool = True,
        retries_per_chunk: int = 3,
    ) -> Query:
        return Query(
            {True: COMMIT_QUERY, False: NON_APOC_COMMIT_QUERY}[apoc_iterate],
            {
                "iterate_params": {
                    "batched_parameter_sets": self.batched_parameter_sets
                },
                "batched_query": self.query_statement,
                "iterable_query": UNWIND_QUERY,
                "execute_chunks_in_parallel": execute_chunks_in_parallel,
                "chunk_size": chunk_size,
                "retries_per_chunk": retries_per_chunk,
            },
            is_apoc=apoc_iterate,  # Set is_apoc based on apoc_iterate parameter
        )
