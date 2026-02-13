namespace DbSqlLikeMem.Oracle.Test.Strategy;

/// <summary>
/// EN: Defines the class OracleUpdateStrategyCoverageTests.
/// PT: Define o(a) class OracleUpdateStrategyCoverageTests.
/// </summary>
public sealed class OracleUpdateStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Update_SetNullableColumnToNull_ShouldWork behavior.
    /// PT: Testa o comportamento de Update_SetNullableColumnToNull_ShouldWork.
    /// </summary>
    [Fact]
    public void Update_SetNullableColumnToNull_ShouldWork()
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["total"] = new ColumnDef(1, DbType.Decimal, true);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
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
    public void Update_SetNotNullableColumnToNull_ShouldThrow()
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["total"] = new ColumnDef(1, DbType.Decimal, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(DbSqlLikeMem.Resources.SqlExceptionMessages.ColumnDoesNotAcceptNull(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
