namespace DbSqlLikeMem.Db2.Dapper.Test;
/// <summary>
/// EN: Defines the class Db2TransactionTests.
/// PT: Define a classe Db2TransactionTests.
/// </summary>
public sealed class Db2TransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        /// <summary>
        /// EN: Gets or sets Id.
        /// PT: Obtém ou define Id.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Gets or sets Name.
        /// PT: Obtém ou define Name.
        /// </summary>
        public required string Name { get; set; }
        /// <summary>
        /// EN: Gets or sets Email.
        /// PT: Obtém ou define Email.
        /// </summary>
        public string? Email { get; set; }
    }

    /// <summary>
    /// EN: Tests TransactionCommitShouldPersistData behavior.
    /// PT: Testa o comportamento de TransactionCommitShouldPersistData.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Transaction")]
    public void TransactionCommitShouldPersistData()
    {
        // Arrange
        var db = new Db2DbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = new Db2ConnectionMock(db);
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

    /// <summary>
    /// EN: Tests TransactionRollbackShouldNotPersistData behavior.
    /// PT: Testa o comportamento de TransactionRollbackShouldNotPersistData.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Transaction")]
    public void TransactionRollbackShouldNotPersistData()
    {
        // Arrange
        var db = new Db2DbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = new Db2ConnectionMock(db);
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
