namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// EN: Contains trigger behavior tests for the SQLite strategy.
/// PT: Contém testes de comportamento de gatilhos para a estratégia SQLite.
/// </summary>
public sealed class SqliteTriggerStrategyTests
{
    /// <summary>
    /// EN: Ensures that an AFTER INSERT trigger is executed for a non-temporary table.
    /// PT: Garante que um gatilho AFTER INSERT seja executado para uma tabela não temporária.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new SqliteDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table, exactMatch: false);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }

    /// <summary>
    /// EN: Ensures that triggers are not executed for a temporary table.
    /// PT: Garante que os gatilhos não sejam executados para uma tabela temporária.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TemporaryTable_ShouldNotExecuteAfterInsertTrigger()
    {
        var db = new SqliteDbMock();
        using var connection = new SqliteConnectionMock(db);

        var table = connection.AddTemporaryTable("temp_users");
        table.AddColumn("id", DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table, exactMatch: false);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var cmd = new SqliteCommandMock(connection) { CommandText = "INSERT INTO temp_users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
