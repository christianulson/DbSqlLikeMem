namespace DbSqlLikeMem.Oracle.Test;
/// <summary>
/// EN: Defines the class OracleTransactionTests.
/// PT: Define o(a) class OracleTransactionTests.
/// </summary>
public sealed class OracleTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        /// <summary>
        /// EN: Provides details for Id.
        /// PT: Fornece detalhes de Id.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Provides details for Name.
        /// PT: Fornece detalhes de Name.
        /// </summary>
        public required string Name { get; set; }
        /// <summary>
        /// EN: Provides details for Email.
        /// PT: Fornece detalhes de Email.
        /// </summary>
        public string? Email { get; set; }
    }

    /// <summary>
    /// EN: Tests TransactionCommitShouldPersistData behavior.
    /// PT: Testa o comportamento de TransactionCommitShouldPersistData.
    /// </summary>
    [Fact]
    public void TransactionCommitShouldPersistData()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);

        using var connection = new OracleConnectionMock(db);
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
    public void TransactionRollbackShouldNotPersistData()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);

        using var connection = new OracleConnectionMock(db);
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
