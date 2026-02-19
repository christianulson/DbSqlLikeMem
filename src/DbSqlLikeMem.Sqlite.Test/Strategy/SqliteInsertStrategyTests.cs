namespace DbSqlLikeMem.Sqlite.Test.Strategy;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteInsertStrategyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests InsertIntoTableShouldAddNewRow behavior.
    /// PT: Testa o comportamento de InsertIntoTableShouldAddNewRow.
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
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }
}
