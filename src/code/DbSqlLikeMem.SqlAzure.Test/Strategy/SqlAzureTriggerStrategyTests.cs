namespace DbSqlLikeMem.SqlAzure.Test.Strategy;

/// <summary>
/// EN: Contains trigger behavior tests for the SQL Azure strategy.
/// PT: Contem testes de comportamento de gatilhos para a estrategia SQL Azure.
/// </summary>
public sealed class SqlAzureTriggerStrategyTests
{
    /// <summary>
    /// EN: Ensures that insert, update, and delete triggers are executed for a non-temporary SQL Azure table.
    /// PT: Garante que os gatilhos de insert, update e delete sejam executados para uma tabela SQL Azure nao temporaria.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void NonTemporaryTable_ShouldExecuteInsertUpdateDeleteTriggers()
    {
        var db = new SqlAzureDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        var triggerTable = Assert.IsAssignableFrom<TableMock>(table);
        var events = new List<TableTriggerEvent>();
        triggerTable.AddTrigger(TableTriggerEvent.BeforeInsert, _ => events.Add(TableTriggerEvent.BeforeInsert));
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => events.Add(TableTriggerEvent.AfterInsert));
        triggerTable.AddTrigger(TableTriggerEvent.BeforeUpdate, _ => events.Add(TableTriggerEvent.BeforeUpdate));
        triggerTable.AddTrigger(TableTriggerEvent.AfterUpdate, _ => events.Add(TableTriggerEvent.AfterUpdate));
        triggerTable.AddTrigger(TableTriggerEvent.BeforeDelete, _ => events.Add(TableTriggerEvent.BeforeDelete));
        triggerTable.AddTrigger(TableTriggerEvent.AfterDelete, _ => events.Add(TableTriggerEvent.AfterDelete));

        using var connection = new SqlAzureConnectionMock(db);

        using (var insert = new SqlAzureCommandMock(connection) { CommandText = "INSERT INTO users (id, name) VALUES (1, 'john')" })
            insert.ExecuteNonQuery();

        using (var update = new SqlAzureCommandMock(connection) { CommandText = "UPDATE users SET name = 'mary' WHERE id = 1" })
            update.ExecuteNonQuery();

        using (var delete = new SqlAzureCommandMock(connection) { CommandText = "DELETE FROM users WHERE id = 1" })
            delete.ExecuteNonQuery();

        Assert.Equal(
        [
            TableTriggerEvent.BeforeInsert,
            TableTriggerEvent.AfterInsert,
            TableTriggerEvent.BeforeUpdate,
            TableTriggerEvent.AfterUpdate,
            TableTriggerEvent.BeforeDelete,
            TableTriggerEvent.AfterDelete
        ], events);
    }

    /// <summary>
    /// EN: Ensures that triggers are not executed for a temporary SQL Azure table.
    /// PT: Garante que os gatilhos nao sejam executados para uma tabela SQL Azure temporaria.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TemporaryTable_ShouldNotExecuteTriggers()
    {
        var db = new SqlAzureDbMock();
        using var connection = new SqlAzureConnectionMock(db);

        var temp = connection.AddTemporaryTable("#users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);

        var calls = 0;
        var triggerTable = Assert.IsAssignableFrom<TableMock>(temp);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var insert = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO #users (id, name) VALUES (1, 'john')"
        };

        insert.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
