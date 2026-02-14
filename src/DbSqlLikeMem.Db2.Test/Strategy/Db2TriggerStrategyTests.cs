namespace DbSqlLikeMem.Db2.Test.Strategy;

/// <summary>
/// EN: Contains trigger behavior tests for the Db2 strategy.
/// PT: Contém testes de comportamento de gatilhos para a estratégia Db2.
/// </summary>
public sealed class Db2TriggerStrategyTests
{
    /// <summary>
    /// EN: Ensures that an AFTER INSERT trigger is executed for a non-temporary table.
    /// PT: Garante que um gatilho AFTER INSERT seja executado para uma tabela não temporária.
    /// </summary>
    [Fact]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new Db2DbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new Db2ConnectionMock(db);
        using var cmd = new Db2CommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }

    /// <summary>
    /// EN: Ensures that an AFTER INSERT trigger is not executed for a temporary table.
    /// PT: Garante que um gatilho AFTER INSERT não seja executado para uma tabela temporária.
    /// </summary>
    [Fact]
    public void TemporaryTable_ShouldNotExecuteAfterInsertTrigger()
    {
        var db = new Db2DbMock();
        using var connection = new Db2ConnectionMock(db);

        var table = connection.AddTemporaryTable("temp_users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var cmd = new Db2CommandMock(connection) { CommandText = "INSERT INTO temp_users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
