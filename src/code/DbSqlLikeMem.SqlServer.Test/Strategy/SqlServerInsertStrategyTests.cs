namespace DbSqlLikeMem.SqlServer.Test.Strategy;
/// <summary>
/// EN: Covers single-row INSERT execution in the SqlServer mock.
/// PT-br: Cobre execucao de INSERT de uma linha no mock SqlServer.
/// </summary>
public sealed class SqlServerInsertStrategyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that a single INSERT adds one row to the target table.
    /// PT-br: Verifica se um INSERT simples adiciona uma linha na tabela alvo.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertIntoTableShouldAddNewRow()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        var rowsAffected = command.ExecuteNonQuery();

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }
}
