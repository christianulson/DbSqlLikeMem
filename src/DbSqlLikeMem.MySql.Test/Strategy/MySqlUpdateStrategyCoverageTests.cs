namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class MySqlUpdateStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Update_SetNullableColumnToNull_ShouldWork behavior.
    /// PT: Testa o comportamento de Update_SetNullableColumnToNull_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNullableColumnToNull_ShouldWork()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var updated = cmd.ExecuteNonQuery();

        Assert.Equal(1, updated);
        Assert.Null(users[0][1]);
    }

    /// <summary>
    /// EN: Tests Update_SetNotNullableColumnToNull_ShouldThrow behavior.
    /// PT: Testa o comportamento de Update_SetNotNullableColumnToNull_ShouldThrow.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNotNullableColumnToNull_ShouldThrow()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, false, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var ex = Assert.Throws<MySqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.ColumnDoesNotAcceptNull(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
