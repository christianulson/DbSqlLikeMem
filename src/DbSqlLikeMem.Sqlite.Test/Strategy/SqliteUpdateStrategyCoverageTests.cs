using FluentAssertions;

namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// EN: Covers update edge cases in the Sqlite mock.
/// PT: Cobre casos de borda de update no mock Sqlite.
/// </summary>
public sealed class SqliteUpdateStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that setting a nullable column to NULL succeeds.
    /// PT: Verifica se definir uma coluna anulavel como NULL funciona.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNullableColumnToNull_ShouldWork()
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        var updated = cmd.ExecuteNonQuery();

        updated.Should().Be(1);
        users[0][1].Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that setting a non-nullable column to NULL fails.
    /// PT: Verifica se definir uma coluna nao anulavel como NULL falha.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_SetNotNullableColumnToNull_ShouldThrow()
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, false, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "UPDATE users SET total = NULL WHERE id = 1"
        };

        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<SqliteMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.ColumnDoesNotAcceptNull());
    }
}
