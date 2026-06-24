import pytest
from hamcrest import assert_that
from neo4j import AsyncSession
from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    DropNodeProperty,
    DropNodeType,
    DropRelationshipProperty,
    DropRelationshipType,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)

from nodestream_plugin_neo4j.migrator import Neo4jDatabaseConnection, Neo4jMigrator
from nodestream_plugin_neo4j.query import Query

from .matchers import ran_query


@pytest.fixture
def database_connection(mocker):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    connection.session.return_value = mocker.AsyncMock(AsyncSession)
    return connection


@pytest.fixture
def migrator(database_connection):
    return Neo4jMigrator(database_connection, False)


@pytest.mark.asyncio
async def test_execute_relationship_key_part_renamed(migrator):
    operation = RelationshipKeyPartRenamed(
        old_key_part_name="old_key",
        new_key_part_name="new_key",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"CALL {{ WITH r SET r.`new_key` = r.`old_key` REMOVE r.`old_key` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_property_renamed(migrator):
    operation = RenameRelationshipProperty(
        old_property_name="old_prop",
        new_property_name="new_prop",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"CALL {{ WITH r SET r.`new_prop` = r.`old_prop` REMOVE r.`old_prop` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_key_extended(migrator):
    operation = RelationshipKeyExtended(
        added_key_property="key", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"WHERE r.`key` IS NULL "
        f"WITH r, $value AS value "
        f"CALL {{ WITH r, value "
        f"SET r.`key` = coalesce(r.`key`, value) }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        value="foo",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_execute_relationship_property_added(migrator):
    operation = AddRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"WHERE r.`prop` IS NULL "
        f"WITH r, $value AS value "
        f"CALL {{ WITH r, value "
        f"SET r.`prop` = coalesce(r.`prop`, value) }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        value="foo",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_property_dropped(migrator):
    operation = DropRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"CALL {{ WITH r REMOVE r.`prop` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_execute_relationship_type_renamed(migrator):
    operation = RenameRelationshipType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH (n)-[r:`OLD_TYPE`]->(m) "
        f"CALL {{ WITH n, r, m "
        f"CREATE (n)-[r2:`NEW_TYPE`]->(m) "
        f"SET r2 += properties(r) "
        f"DELETE r }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_type_created(migrator):
    # Neo4j Does not need us to do anything here — the method is a no-op.
    from nodestream.schema.migrations.operations import CreateRelationshipType

    operation = CreateRelationshipType(name="KNOWS", keys=[], properties=[])
    await migrator.execute_operation(operation)
    # No queries should have been executed.
    migrator.database_connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_execute_relationship_type_dropped(migrator):
    operation = DropRelationshipType(name="RELATIONSHIP_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH ()-[r:`RELATIONSHIP_TYPE`]->() "
        f"CALL {{ WITH r DELETE r }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_node_type_dropped(migrator):
    operation = DropNodeType(name="NodeType")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH (n:`NodeType`) "
        f"CALL {{ WITH n DETACH DELETE n }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_add_additional_node_property_index(migrator):
    operation = AddAdditionalNodePropertyIndex(field_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "CREATE INDEX NodeType_prop_additional_index IF NOT EXISTS FOR (n:`NodeType`) ON (n.`prop`)"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_drop_additional_node_property_index(migrator):
    operation = DropAdditionalNodePropertyIndex(field_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    query = Query.from_statement("DROP INDEX NodeType_prop_additional_index IF EXISTS")
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_add_additional_relationship_property_index(migrator):
    operation = AddAdditionalRelationshipPropertyIndex(
        field_name="prop", relationship_type="RelationshipType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE INDEX RelationshipType_prop_additional_index IF NOT EXISTS FOR ()-[r:`RelationshipType`]-() ON (r.`prop`)"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_drop_additional_relationship_property_index(migrator):
    operation = DropAdditionalRelationshipPropertyIndex(
        field_name="prop", relationship_type="RelationshipType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "DROP INDEX RelationshipType_prop_additional_index IF EXISTS"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_rename_node_property(migrator):
    operation = RenameNodeProperty(
        old_property_name="old_prop", new_property_name="new_prop", node_type="NodeType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        f"MATCH (n:`NodeType`) "
        f"CALL {{ WITH n "
        f"SET n.`new_prop` = n.`old_prop` "
        f"REMOVE n.`old_prop` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_rename_node_type(migrator):
    operation = RenameNodeType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH (n:`OLD_TYPE`) "
        f"CALL {{ WITH n "
        f"SET n:`NEW_TYPE` "
        f"REMOVE n:`OLD_TYPE` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_create_node_type_non_enterprise(migrator):
    operation = CreateNodeType(
        name="Person", keys=["first_name", "last_name"], properties=[]
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE CONSTRAINT Person_node_key IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`first_name`,n.`last_name`) IS UNIQUE"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_create_node_type_enterprise(migrator):
    migrator.use_enterprise_features = True
    operation = CreateNodeType(
        name="Person", keys=["first_name", "last_name"], properties=[]
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE CONSTRAINT Person_node_key IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`first_name`,n.`last_name`) IS NODE KEY"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_add_node_property(migrator):
    operation = AddNodeProperty(
        property_name="prop", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH (n:`NodeType`) WHERE n.`prop` IS NULL WITH n, $value AS value CALL {{ WITH n, value SET n.`prop` = coalesce(n.`prop`, value) }} IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        value="foo",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_drop_node_property(migrator):
    operation = DropNodeProperty(property_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        f"MATCH (n:`NodeType`) "
        f"CALL {{ WITH n REMOVE n.`prop` }} "
        f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_node_key_extended_with_default(migrator):
    operation = NodeKeyExtended(
        added_key_property="key", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        f"MATCH (n:`NodeType`) WHERE n.`key` IS NULL WITH n, $value AS value CALL {{ WITH n, value SET n.`key` = coalesce(n.`key`, value) }} IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS",
        value="foo",
        is_implicit=True,
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_node_key_renamed(migrator, mocker):
    migrator.get_properties_by_constraint_name = mocker.AsyncMock(return_value={"foo"})
    operation = NodeKeyPartRenamed(
        new_key_part_name="key", node_type="NodeType", old_key_part_name="foo"
    )
    await migrator.execute_operation(operation)
    assert_that(
        migrator,
        (
            f"MATCH (n:`NodeType`) "
            f"CALL {{ WITH n "
            f"SET n.`key` = n.`foo` "
            f"REMOVE n.`foo` }} "
            f"IN TRANSACTIONS OF {migrator.transaction_batch_size} ROWS"
        ),
    )


@pytest.mark.asyncio
async def test_node_key_part_renamed_skips_when_old_key_not_in_constraint(
    migrator, mocker
):
    """When old_key_part_name is not in the current constraint, the operation is a no-op."""
    migrator.get_properties_by_constraint_name = mocker.AsyncMock(
        return_value={"other_key"}
    )
    migrator.drop_constraint_by_name = mocker.AsyncMock()
    operation = NodeKeyPartRenamed(
        new_key_part_name="new_key",
        node_type="NodeType",
        old_key_part_name="missing_key",
    )
    await migrator.execute_operation(operation)
    # Should return early — no constraint drop, no rename
    migrator.drop_constraint_by_name.assert_not_called()


@pytest.mark.asyncio
async def test_acquire_lock_raises_cannot_acquire_when_execute_fails(
    migrator, database_connection
):
    from nodestream_plugin_neo4j.migrator import CannotAcquireLockException

    database_connection.execute.side_effect = Exception("neo4j unreachable")
    with pytest.raises(CannotAcquireLockException):
        await migrator.acquire_lock()


@pytest.mark.asyncio
async def test_acquire_lock_success(migrator, database_connection, mocker):
    database_connection.execute.return_value = []
    # Should not raise
    await migrator.acquire_lock()


@pytest.mark.asyncio
async def test_release_lock(migrator, database_connection):
    database_connection.execute.return_value = []
    await migrator.release_lock()
    database_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_mark_migration_as_executed(migrator, database_connection, mocker):
    from nodestream.schema.migrations import Migration

    migration = mocker.Mock(Migration)
    migration.name = "0001_initial"
    database_connection.execute.return_value = []
    await migrator.mark_migration_as_executed(migration)
    database_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_completed_migrations(migrator, database_connection, mocker):
    from nodestream.schema.migrations import MigrationGraph

    graph = mocker.Mock(MigrationGraph)
    m = mocker.Mock()
    graph.get_migration.return_value = m
    database_connection.execute.return_value = [{"name": "0001_initial"}]
    result = await migrator.get_completed_migrations(graph)
    assert result == [m]
    graph.get_migration.assert_called_once_with("0001_initial")


@pytest.mark.asyncio
async def test_drop_all_indexes_on_type_node(migrator, mocker):
    """drop_all_indexes_on_type for NODE calls DropAdditionalNodePropertyIndex."""
    migrator.get_indexed_properties_by_type = mocker.AsyncMock(return_value={"prop"})
    migrator.execute_operation = mocker.AsyncMock()
    result = await migrator.drop_all_indexes_on_type("NODE", "NodeType")
    assert result == {"prop"}
    call_arg = migrator.execute_operation.call_args[0][0]
    assert isinstance(call_arg, DropAdditionalNodePropertyIndex)


@pytest.mark.asyncio
async def test_drop_all_indexes_on_type_relationship(migrator, mocker):
    """drop_all_indexes_on_type for RELATIONSHIP calls DropAdditionalRelationshipPropertyIndex."""
    migrator.get_indexed_properties_by_type = mocker.AsyncMock(return_value={"since"})
    migrator.execute_operation = mocker.AsyncMock()
    result = await migrator.drop_all_indexes_on_type("RELATIONSHIP", "REL_TYPE")
    assert result == {"since"}
    call_arg = migrator.execute_operation.call_args[0][0]
    assert isinstance(call_arg, DropAdditionalRelationshipPropertyIndex)


@pytest.mark.asyncio
async def test_execute_rename_node_type_with_indexes(migrator, mocker):
    """Indexed properties are dropped on old type and recreated on new type."""
    migrator.get_properties_by_constraint_name = mocker.AsyncMock(return_value=set())
    migrator.get_indexed_properties_by_type = mocker.AsyncMock(return_value={"since"})
    migrator.execute_drop_additional_node_property_index = mocker.AsyncMock()
    migrator.execute_add_additional_node_property_index = mocker.AsyncMock()
    operation = RenameNodeType(old_type="OldType", new_type="NewType")
    await migrator.execute_rename_node_type(operation)
    migrator.execute_drop_additional_node_property_index.assert_called_once()
    migrator.execute_add_additional_node_property_index.assert_called_once()
    add_call_arg = migrator.execute_add_additional_node_property_index.call_args[0][0]
    assert isinstance(add_call_arg, AddAdditionalNodePropertyIndex)
    assert add_call_arg.node_type == "NewType"


@pytest.mark.asyncio
async def test_execute_rename_relationship_type_with_indexes(migrator, mocker):
    """Indexed properties are dropped on old rel type and recreated on new one."""
    migrator.get_indexed_properties_by_type = mocker.AsyncMock(return_value={"since"})
    migrator.execute_drop_additional_node_property_index = mocker.AsyncMock()
    migrator.execute_add_additional_relationship_property_index = mocker.AsyncMock()
    operation = RenameRelationshipType(old_type="OLD_REL", new_type="NEW_REL")
    await migrator.execute_rename_relationship_type(operation)
    migrator.execute_add_additional_relationship_property_index.assert_called_once()
    add_call_arg = (
        migrator.execute_add_additional_relationship_property_index.call_args[0][0]
    )
    assert isinstance(add_call_arg, AddAdditionalRelationshipPropertyIndex)
    assert add_call_arg.relationship_type == "NEW_REL"
