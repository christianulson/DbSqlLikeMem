namespace DbSqlLikeMem.Sqlite.Test.Strategy;
/// <summary>
/// EN: Covers single-row INSERT execution in the Sqlite mock.
/// PT: Cobre execucao de INSERT de uma linha no mock Sqlite.
/// </summary>
public sealed class SqliteInsertStrategyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that a single INSERT adds one row to the target table.
    /// PT: Verifica se um INSERT simples adiciona uma linha na tabela alvo.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertIntoTableShouldAddNewRow()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new SqliteConnectionMock(db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        var rowsAffected = command.ExecuteNonQuery();

        // Assert
        rowsAffected.Should().Be(1);
        table.Should().ContainSingle();
        table[0][0].Should().Be(1);
        table[0][1].Should().Be("John Doe");
    }
}
