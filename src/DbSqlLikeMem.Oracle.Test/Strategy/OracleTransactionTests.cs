namespace DbSqlLikeMem.Oracle.Test.Strategy;
public sealed class OracleTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests TransactionShouldCommit behavior.
    /// PT: Testa o comportamento de TransactionShouldCommit.
    /// </summary>
    [Fact]
    public void TransactionShouldCommit()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);

        using var connection = new OracleConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        ArgumentNullException.ThrowIfNull(transaction);
        using var command = new OracleCommandMock(
            connection,
            (OracleTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Commit();

        // Assert
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }

    /// <summary>
    /// EN: Tests TransactionShouldRollback behavior.
    /// PT: Testa o comportamento de TransactionShouldRollback.
    /// </summary>
    [Fact]
    public void TransactionShouldRollback()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);

        using var connection = new OracleConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        using var command = new OracleCommandMock(
            connection,
            (OracleTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Rollback();

        // Assert
        Assert.Empty(table);
    }
}
