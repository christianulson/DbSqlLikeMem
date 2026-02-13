namespace DbSqlLikeMem.Sqlite.Test.Strategy;

public sealed class SqliteTriggerStrategyTests
{
    [Fact]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new SqliteDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }

    [Fact]
    public void TemporaryTable_ShouldNotExecuteAfterInsertTrigger()
    {
        var db = new SqliteDbMock();
        using var connection = new SqliteConnectionMock(db);

        var table = connection.AddTemporaryTable("temp_users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var cmd = new SqliteCommandMock(connection) { CommandText = "INSERT INTO temp_users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
