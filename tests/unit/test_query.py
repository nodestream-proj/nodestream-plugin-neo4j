from hamcrest import assert_that, equal_to, has_entries, is_, not_none

from nodestream_plugin_neo4j.query import (
    APOC_BATCH_QUERY_RESPONSE_FIELDS,
    COMMIT_QUERY,
    NON_APOC_COMMIT_QUERY,
    RETURN_CLAUSE,
    UNWIND_QUERY,
    YIELD_CLAUSE,
    ApocBatchResponse,
    ApocUpdateStatistics,
    Query,
    QueryBatch,
)


def test_apoc_update_statistics_default_initialization():
    stats = ApocUpdateStatistics()
    assert_that(stats.relationshipsDeleted, equal_to(0))
    assert_that(stats.relationshipsCreated, equal_to(0))
    assert_that(stats.nodesDeleted, equal_to(0))
    assert_that(stats.nodesCreated, equal_to(0))
    assert_that(stats.labelsRemoved, equal_to(0))
    assert_that(stats.labelsAdded, equal_to(0))
    assert_that(stats.propertiesSet, equal_to(0))


def test_apoc_update_statistics_custom_initialization():
    stats = ApocUpdateStatistics(
        relationshipsDeleted=5,
        relationshipsCreated=10,
        nodesDeleted=2,
        nodesCreated=8,
        labelsRemoved=1,
        labelsAdded=3,
        propertiesSet=15,
    )
    assert_that(stats.relationshipsDeleted, equal_to(5))
    assert_that(stats.relationshipsCreated, equal_to(10))
    assert_that(stats.nodesDeleted, equal_to(2))
    assert_that(stats.nodesCreated, equal_to(8))
    assert_that(stats.labelsRemoved, equal_to(1))
    assert_that(stats.labelsAdded, equal_to(3))
    assert_that(stats.propertiesSet, equal_to(15))


def test_apoc_batch_response_default_initialization():
    response = ApocBatchResponse()
    assert_that(response.batches, equal_to(0))
    assert_that(response.total, equal_to(0))
    assert_that(response.timeTaken, equal_to(0))
    assert_that(response.committedOperations, equal_to(0))
    assert_that(response.failedOperations, equal_to(0))
    assert_that(response.failedBatches, equal_to(0))
    assert_that(response.retries, equal_to(0))
    assert_that(response.errorMessages, equal_to({}))
    assert_that(response.wasTerminated, equal_to(False))
    assert_that(response.updateStatistics, not_none())
    assert_that(response.updateStatistics, is_(ApocUpdateStatistics))


def test_apoc_batch_response_post_init_with_existing_values():
    existing_errors = {"error1": "message1"}
    existing_stats = ApocUpdateStatistics(nodesCreated=10)

    response = ApocBatchResponse(
        errorMessages=existing_errors, updateStatistics=existing_stats
    )
    assert_that(response.errorMessages, equal_to(existing_errors))
    assert_that(response.updateStatistics, equal_to(existing_stats))
    assert_that(response.updateStatistics.nodesCreated, equal_to(10))


def test_apoc_batch_response_custom_initialization():
    response = ApocBatchResponse(
        batches=3,
        total=100,
        timeTaken=5000,
        committedOperations=95,
        failedOperations=5,
        failedBatches=1,
        retries=2,
        errorMessages={"batch1": "timeout"},
        wasTerminated=True,
        updateStatistics=ApocUpdateStatistics(nodesCreated=50),
    )
    assert_that(response.batches, equal_to(3))
    assert_that(response.total, equal_to(100))
    assert_that(response.timeTaken, equal_to(5000))
    assert_that(response.committedOperations, equal_to(95))
    assert_that(response.failedOperations, equal_to(5))
    assert_that(response.failedBatches, equal_to(1))
    assert_that(response.retries, equal_to(2))
    assert_that(response.errorMessages, equal_to({"batch1": "timeout"}))
    assert_that(response.wasTerminated, equal_to(True))
    assert_that(response.updateStatistics.nodesCreated, equal_to(50))


def test_query_default_initialization():
    query = Query("MATCH (n) RETURN n", {"param1": "value1"})
    assert_that(query.query_statement, equal_to("MATCH (n) RETURN n"))
    assert_that(query.parameters, equal_to({"param1": "value1"}))
    assert_that(query.is_apoc, equal_to(False))


def test_query_initialization_with_apoc_flag():
    query = Query("CALL apoc.test()", {}, is_apoc=True)
    assert_that(query.query_statement, equal_to("CALL apoc.test()"))
    assert_that(query.parameters, equal_to({}))
    assert_that(query.is_apoc, equal_to(True))


def test_query_from_statement_class_method():
    query = Query.from_statement("CREATE (n:Person {name: $name})", name="John", age=30)
    assert_that(query.query_statement, equal_to("CREATE (n:Person {name: $name})"))
    assert_that(query.parameters, equal_to({"name": "John", "age": 30}))
    assert_that(query.is_apoc, equal_to(False))


def test_query_from_statement_no_parameters():
    query = Query.from_statement("MATCH (n) RETURN count(n)")
    assert_that(query.query_statement, equal_to("MATCH (n) RETURN count(n)"))
    assert_that(query.parameters, equal_to({}))
    assert_that(query.is_apoc, equal_to(False))


def test_query_feed_batched_query_default_parameters():
    original_query = Query("MATCH (p:Person) RETURN p", {"limit": 100})
    batched_query = "CREATE (n:Node {id: params.id})"

    result = original_query.feed_batched_query(batched_query)

    assert_that(result.query_statement, equal_to(COMMIT_QUERY))
    assert_that(result.is_apoc, equal_to(True))
    assert_that(
        result.parameters,
        has_entries(
            {
                "iterate_params": {"limit": 100},
                "batched_query": batched_query,
                "iterable_query": "MATCH (p:Person) RETURN p",
                "execute_chunks_in_parallel": True,
                "chunk_size": 1000,
                "retries_per_chunk": 3,
            }
        ),
    )


def test_query_feed_batched_query_custom_parameters():
    original_query = Query("MATCH (p:Person) RETURN p", {"limit": 50})
    batched_query = "CREATE (n:Node {id: params.id})"

    result = original_query.feed_batched_query(
        batched_query,
        chunk_size=500,
        execute_chunks_in_parallel=False,
        retries_per_chunk=5,
    )

    assert_that(result.query_statement, equal_to(COMMIT_QUERY))
    assert_that(result.is_apoc, equal_to(True))
    assert_that(
        result.parameters,
        has_entries(
            {
                "iterate_params": {"limit": 50},
                "batched_query": batched_query,
                "iterable_query": "MATCH (p:Person) RETURN p",
                "execute_chunks_in_parallel": False,
                "chunk_size": 500,
                "retries_per_chunk": 5,
            }
        ),
    )


def test_query_batch_default_initialization():
    batch = QueryBatch("CREATE (n:Node {id: $id})", [{"id": 1}, {"id": 2}])
    assert_that(batch.query_statement, equal_to("CREATE (n:Node {id: $id})"))
    assert_that(batch.batched_parameter_sets, equal_to([{"id": 1}, {"id": 2}]))
    assert_that(batch.is_apoc, equal_to(False))


def test_query_batch_initialization_with_apoc_flag():
    batch = QueryBatch("CALL apoc.create.node()", [{}], is_apoc=True)
    assert_that(batch.query_statement, equal_to("CALL apoc.create.node()"))
    assert_that(batch.batched_parameter_sets, equal_to([{}]))
    assert_that(batch.is_apoc, equal_to(True))


def test_query_batch_as_query_with_apoc_iterate_true():
    batch = QueryBatch("CREATE (n:Node {id: $id})", [{"id": 1}, {"id": 2}, {"id": 3}])

    result = batch.as_query(apoc_iterate=True)

    assert_that(result.query_statement, equal_to(COMMIT_QUERY))
    assert_that(result.is_apoc, equal_to(True))
    assert_that(
        result.parameters,
        has_entries(
            {
                "iterate_params": {
                    "batched_parameter_sets": [{"id": 1}, {"id": 2}, {"id": 3}]
                },
                "batched_query": "CREATE (n:Node {id: $id})",
                "iterable_query": UNWIND_QUERY,
                "execute_chunks_in_parallel": True,
                "chunk_size": 1000,
                "retries_per_chunk": 3,
            }
        ),
    )


def test_query_batch_as_query_with_apoc_iterate_false():
    batch = QueryBatch("CREATE (n:Node {id: $id})", [{"id": 1}, {"id": 2}, {"id": 3}])

    result = batch.as_query(apoc_iterate=False)

    assert_that(result.query_statement, equal_to(NON_APOC_COMMIT_QUERY))
    assert_that(result.is_apoc, equal_to(False))
    assert_that(
        result.parameters,
        has_entries(
            {
                "iterate_params": {
                    "batched_parameter_sets": [{"id": 1}, {"id": 2}, {"id": 3}]
                },
                "batched_query": "CREATE (n:Node {id: $id})",
                "iterable_query": UNWIND_QUERY,
                "execute_chunks_in_parallel": True,
                "chunk_size": 1000,
                "retries_per_chunk": 3,
            }
        ),
    )


def test_query_batch_as_query_with_custom_parameters():
    batch = QueryBatch("CREATE (n:Node {id: $id})", [{"id": 1}])

    result = batch.as_query(
        apoc_iterate=True,
        chunk_size=2000,
        execute_chunks_in_parallel=False,
        retries_per_chunk=10,
    )

    assert_that(
        result.parameters,
        has_entries(
            {
                "execute_chunks_in_parallel": False,
                "chunk_size": 2000,
                "retries_per_chunk": 10,
            }
        ),
    )


def test_apoc_batch_query_response_fields():
    expected_fields = [
        "batches",
        "total",
        "timeTaken",
        "committedOperations",
        "failedOperations",
        "failedBatches",
        "retries",
        "errorMessages",
        "wasTerminated",
        "updateStatistics",
    ]
    assert_that(
        APOC_BATCH_QUERY_RESPONSE_FIELDS,
        contains_inanyorder(
            "batches",
            "total",
            "timeTaken",
            "committedOperations",
            "failedOperations",
            "failedBatches",
            "retries",
            "errorMessages",
            "wasTerminated",
            "updateStatistics",
        ),
    )


def test_unwind_query():
    assert_that(
        UNWIND_QUERY, equal_to("UNWIND $batched_parameter_sets as params RETURN params")
    )


def test_yield_clause_contains_all_fields():
    expected_yield = f"YIELD {', '.join(APOC_BATCH_QUERY_RESPONSE_FIELDS)}"
    assert_that(YIELD_CLAUSE, equal_to(expected_yield))


def test_return_clause_contains_all_fields():
    expected_return = f"RETURN {', '.join(APOC_BATCH_QUERY_RESPONSE_FIELDS)}"
    assert_that(RETURN_CLAUSE, equal_to(expected_return))


def test_commit_query_structure():
    assert_that("CALL apoc.periodic.iterate" in COMMIT_QUERY, equal_to(True))
    assert_that("YIELD" in COMMIT_QUERY, equal_to(True))
    assert_that("RETURN" in COMMIT_QUERY, equal_to(True))


def test_non_apoc_commit_query_structure():
    assert_that("UNWIND" in NON_APOC_COMMIT_QUERY, equal_to(True))
    assert_that("CALL apoc.cypher.doIt" in NON_APOC_COMMIT_QUERY, equal_to(True))
    assert_that("RETURN" in NON_APOC_COMMIT_QUERY, equal_to(True))
