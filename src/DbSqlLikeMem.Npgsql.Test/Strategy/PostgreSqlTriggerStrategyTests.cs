namespace DbSqlLikeMem.Npgsql.Test.Strategy;

public sealed class PostgreSqlTriggerStrategyTests
{
    [Fact]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }

    [Fact]
    public void TemporaryTable_ShouldNotExecuteAfterInsertTrigger()
    {
        var db = new NpgsqlDbMock();
        using var connection = new NpgsqlConnectionMock(db);

        var table = connection.AddTemporaryTable("temp_users");
        table.Columns["id"] = new(0, DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsType<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var cmd = new NpgsqlCommandMock(connection) { CommandText = "INSERT INTO temp_users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(0, calls);
    }
}
