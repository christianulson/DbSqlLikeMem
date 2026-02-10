namespace DbSqlLikeMem.MySql.Test.Strategy;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class MySqlInsertStrategyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests InsertIntoTableShouldAddNewRow behavior.
    /// PT: Testa o comportamento de InsertIntoTableShouldAddNewRow.
    /// </summary>
    [Fact]
    public void InsertIntoTableShouldAddNewRow()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);

        using var connection = new MySqlConnectionMock(db);
        using var command = new MySqlCommandMock(connection)
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
