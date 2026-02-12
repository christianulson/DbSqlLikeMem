namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class Db2TransactionReliabilityTests
{
    /// <summary>
    /// EN: Ensures rolling back to a savepoint restores the intermediate state.
    /// PT: Garante que rollback para savepoint restaure o estado intermediário.
    /// </summary>
    [Fact]
    public void SavepointRollbackShouldRestoreIntermediateState()
    {
        var db = new Db2DbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);

        using var connection = new Db2ConnectionMock(db);
        connection.Open();
        using var transaction = (Db2TransactionMock)connection.BeginTransaction();

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
    public void IsolationLevelShouldBeExposedDeterministically()
    {
        var db = new Db2DbMock();
        using var connection = new Db2ConnectionMock(db);
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
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
    {
        var db = new Db2DbMock();
        using var connection = new Db2ConnectionMock(db);
        connection.Open();

        using var transaction = (Db2TransactionMock)connection.BeginTransaction();
        transaction.Save("sp_release");

        if (true)
        {
            transaction.Release("sp_release");
            return;
        }

        Assert.Throws<NotSupportedException>(() => transaction.Release("sp_release"));
    }

    /// <summary>
    /// EN: Ensures concurrent writes keep data consistent when thread safety is enabled.
    /// PT: Garante que escritas concorrentes mantenham dados consistentes com thread safety habilitado.
    /// </summary>
    [Fact]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
    {
        var db = new Db2DbMock { ThreadSafe = true };
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);

        Parallel.For(1, 41, id =>
        {
            using var connection = new Db2ConnectionMock(db);
            connection.Open();
            connection.Execute("INSERT INTO Users (Id, Name) VALUES (@Id, @Name)", new { Id = id, Name = $"Name{id}" });
        });

        Assert.Equal(40, table.Count);
        Assert.Equal(40, table.Select(row => (int)row[0]!).Distinct().Count());
    }
}
