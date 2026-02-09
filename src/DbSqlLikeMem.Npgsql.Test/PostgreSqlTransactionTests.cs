namespace DbSqlLikeMem.Npgsql.Test;
public sealed class PostgreSqlTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        public int Id { get; set; }
        public required string Name { get; set; }
        public string? Email { get; set; }
    }

    [Fact]
    public void TransactionCommitShouldPersistData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new User { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        // Act
        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.CommitTransaction();

        // Assert
        Assert.Single(table);
        var insertedRow = table[0];
        Assert.Equal(user.Id, insertedRow[0]);
        Assert.Equal(user.Name, insertedRow[1]);
        Assert.Equal(user.Email, insertedRow[2]);
    }

    [Fact]
    public void TransactionRollbackShouldNotPersistData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new User { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        // Act
        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.RollbackTransaction();

        // Assert
        Assert.Empty(table);
    }
}