namespace DbSqlLikeMem.SqlServer.Test.Strategy;

/// <summary>
/// EN: Contains trigger behavior tests for the SQL Server strategy.
/// PT: Contém testes de comportamento de gatilhos para a estratégia SQL Server.
/// </summary>
public sealed class SqlServerTriggerStrategyTests
{
    /// <summary>
    /// EN: Ensures that insert, update, and delete triggers are executed for a non-temporary table.
    /// PT: Garante que os gatilhos de insert, update e delete sejam executados para uma tabela não temporária.
    /// </summary>
    [Fact]
    public void NonTemporaryTable_ShouldExecuteInsertUpdateDeleteTriggers()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        var triggerTable = Assert.IsType<TableMock>(table, exactMatch: false);
        var events = new List<TableTriggerEvent>();
        triggerTable.AddTrigger(TableTriggerEvent.BeforeInsert, _ => events.Add(TableTriggerEvent.BeforeInsert));
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => events.Add(TableTriggerEvent.AfterInsert));
        triggerTable.AddTrigger(TableTriggerEvent.BeforeUpdate, _ => events.Add(TableTriggerEvent.BeforeUpdate));
        triggerTable.AddTrigger(TableTriggerEvent.AfterUpdate, _ => events.Add(TableTriggerEvent.AfterUpdate));
        triggerTable.AddTrigger(TableTriggerEvent.BeforeDelete, _ => events.Add(TableTriggerEvent.BeforeDelete));
        triggerTable.AddTrigger(TableTriggerEvent.AfterDelete, _ => events.Add(TableTriggerEvent.AfterDelete));

        using var connection = new SqlServerConnectionMock(db);

        using (var insert = new SqlServerCommandMock(connection) { CommandText = "INSERT INTO users (id, name) VALUES (1, 'john')" })
            insert.ExecuteNonQuery();

        using (var update = new SqlServerCommandMock(connection) { CommandText = "UPDATE users SET name = 'mary' WHERE id = 1" })
            update.ExecuteNonQuery();

        using (var delete = new SqlServerCommandMock(connection) { CommandText = "DELETE FROM users WHERE id = 1" })
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
    /// EN: Ensures that triggers are not executed for a temporary table.
    /// PT: Garante que os gatilhos não sejam executados para uma tabela temporária.
    /// </summary>
    [Fact]
    public void TemporaryTable_ShouldNotExecuteTriggers()
    {
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);

        var temp = connection.AddTemporaryTable("#users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(temp, exactMatch: false);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var insert = new SqlServerCommandMock(connection)
        {
            CommandText = "INSERT INTO #users (id, name) VALUES (1, 'john')"
        };

        insert.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
