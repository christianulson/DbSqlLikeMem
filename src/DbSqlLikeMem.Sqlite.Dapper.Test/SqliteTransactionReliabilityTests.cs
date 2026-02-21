namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class SqliteTransactionReliabilityTests
{
    /// <summary>
    /// EN: Ensures rolling back to a savepoint restores the intermediate state.
    /// PT: Garante que rollback para savepoint restaure o estado intermediário.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
    {
        var db = new SqliteDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();
        using var transaction = (SqliteTransactionMock)connection.BeginTransaction();

        connection.Execute("INSERT INTO Users (Id, Name) VALUES (1, 'John')", transaction: transaction);
        transaction.Save("sp_users");
        connection.Execute("INSERT INTO Users (Id, Name) VALUES (2, 'Mary')", transaction: transaction);

        transaction.Rollback("sp_users");
        transaction.Commit();

        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
    }

    /// <summary>
    /// EN: Ensures the simplified isolation model is deterministic and visible.
    /// PT: Garante que o modelo simplificado de isolamento seja determinístico e visível.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
    {
        var db = new SqliteDbMock();
        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, connection.CurrentIsolationLevel);
    }

    /// <summary>
    /// EN: Ensures savepoint release support follows provider compatibility rules.
    /// PT: Garante que o suporte a release de savepoint siga as regras de compatibilidade do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
    {
        var db = new SqliteDbMock();
        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using var transaction = (SqliteTransactionMock)connection.BeginTransaction();
        transaction.Save("sp_release");

        transaction.Release("sp_release");
    }

    /// <summary>
    /// EN: Ensures concurrent writes keep data consistent when thread safety is enabled.
    /// PT: Garante que escritas concorrentes mantenham dados consistentes com thread safety habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
    {
        var db = new SqliteDbMock { ThreadSafe = true };
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);

        Parallel.For(1, 41, id =>
        {
            using var connection = new SqliteConnectionMock(db);
            connection.Open();
            connection.Execute("INSERT INTO Users (Id, Name) VALUES (@Id, @Name)", new { Id = id, Name = $"Name{id}" });
        });

        Assert.Equal(40, table.Count);
        Assert.Equal(40, table.Select(row => (int)row[0]!).Distinct().Count());
    }
}
