using FluentAssertions;

namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Covers update edge cases in the MySql mock.
/// PT: Cobre casos de borda de update no mock MySql.
/// </summary>
public sealed class MySqlUpdateStrategyCoverageTests(
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

        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.ColumnDoesNotAcceptNull());
    }
}
