namespace DbSqlLikeMem.Npgsql.Test.Strategy;

public sealed class PostgreSqlUpdateStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Fact]
    public void Update_SetNullableColumnToNull_ShouldWork()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["total"] = new ColumnDef(1, DbType.Decimal, true);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var updated = cmd.ExecuteNonQuery();

        Assert.Equal(1, updated);
        Assert.Null(users[0][1]);
    }

    [Fact]
    public void Update_SetNotNullableColumnToNull_ShouldThrow()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["total"] = new ColumnDef(1, DbType.Decimal, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var ex = Assert.Throws<NpgsqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("Coluna não aceita NULL", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
