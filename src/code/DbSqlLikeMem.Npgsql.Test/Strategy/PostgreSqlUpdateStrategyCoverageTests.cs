namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Covers update edge cases in the Npgsql mock.
/// PT-br: Cobre casos de borda de update no mock Npgsql.
/// </summary>
public sealed class PostgreSqlUpdateStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that setting a nullable column to NULL succeeds.
    /// PT-br: Verifica se definir uma coluna anulavel como NULL funciona.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNullableColumnToNull_ShouldWork()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
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

    /// <summary>
    /// EN: Verifies that setting a non-nullable column to NULL fails.
    /// PT-br: Verifica se definir uma coluna nao anulavel como NULL falha.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNotNullableColumnToNull_ShouldThrow()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, false, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var ex = Assert.Throws<NpgsqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.ColumnDoesNotAcceptNull(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
