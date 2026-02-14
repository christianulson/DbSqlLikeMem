namespace DbSqlLikeMem.Oracle.Test.Strategy;

public sealed class OracleTriggerStrategyTests
{
    [Fact]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table, exactMatch: false);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }

    [Fact]
    public void TemporaryTable_ShouldNotExecuteAfterInsertTrigger()
    {
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);

        var table = connection.AddTemporaryTable("temp_users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table, exactMatch: false);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var cmd = new OracleCommandMock(connection) { CommandText = "INSERT INTO temp_users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
