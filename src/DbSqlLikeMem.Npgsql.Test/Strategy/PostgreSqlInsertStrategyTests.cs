namespace DbSqlLikeMem.Npgsql.Test.Strategy;
public sealed class PostgreSqlInsertStrategyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Fact]
    public void InsertIntoTableShouldAddNewRow()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);

        using var connection = new NpgsqlConnectionMock(db);
        using var command = new NpgsqlCommandMock(connection)
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