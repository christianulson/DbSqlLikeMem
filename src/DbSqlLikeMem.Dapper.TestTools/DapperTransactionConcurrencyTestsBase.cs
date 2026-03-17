using Dapper;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Defines shared transaction reliability assertions for Dapper provider tests.
/// PT: Define asserções compartilhadas de confiabilidade transacional para testes de provedores Dapper.
/// </summary>
public abstract class DapperTransactionConcurrencyTestsBase(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates a factory that opens connections against the same provider database instance.
    /// PT: Cria uma fábrica que abre conexões contra a mesma instância de banco do provedor.
    /// </summary>
    /// <param name="threadSafe">EN: Enables thread safety on provider database. PT: Habilita thread safety no banco do provedor.</param>
    /// <param name="version">EN: Provider version. PT: Versão do provedor.</param>
    /// <returns>EN: Open connection factory. PT: Fábrica de conexão aberta.</returns>
    protected abstract Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null);

    /// <summary>
    /// EN: Indicates whether provider is expected to support RELEASE SAVEPOINT.
    /// PT: Indica se o provedor deve suportar RELEASE SAVEPOINT.
    /// </summary>
    protected virtual bool SupportsReleaseSavepoint => true;

    /// <summary>
    /// EN: Verifies savepoint rollback restores intermediate state.
    /// PT: Verifica se rollback de savepoint restaura estado intermediário.
    /// </summary>
    protected void AssertSavepointRollbackRestoresIntermediateState()
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: false);
        using var connection = openConnection();
        connection.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(100))");

        using var transaction = connection.BeginTransaction();
        connection.Execute("INSERT INTO Users (Id, Name) VALUES (1, 'John')", transaction: transaction);
        connection.CreateSavepoint("sp_users");
        connection.Execute("INSERT INTO Users (Id, Name) VALUES (2, 'Mary')", transaction: transaction);

        connection.RollbackTransaction("sp_users");
        transaction.Commit();

        var ids = connection.Query<int>("SELECT Id FROM Users ORDER BY Id").ToList();
        Assert.Equal([1], ids);
    }

    /// <summary>
    /// EN: Verifies deterministic isolation level exposure.
    /// PT: Verifica exposição determinística do nível de isolamento.
    /// </summary>
    protected void AssertIsolationLevelExposedDeterministically()
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: false);
        using var connection = openConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, connection.CurrentIsolationLevel);
    }

    /// <summary>
    /// EN: Verifies savepoint release follows provider compatibility rules.
    /// PT: Verifica se release de savepoint segue regras de compatibilidade do provedor.
    /// </summary>
    protected void AssertReleaseSavepointCompatibilityIsProviderSpecific()
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: false);
        using var connection = openConnection();
        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp_release");

        if (SupportsReleaseSavepoint)
        {
            connection.ReleaseSavepoint("sp_release");
            return;
        }

        var ex = Assert.Throws<NotSupportedException>(() => connection.ReleaseSavepoint("sp_release"));
        Assert.Contains("RELEASE SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies concurrent inserts stay consistent when thread safety is enabled.
    /// PT: Verifica se inserts concorrentes permanecem consistentes com thread safety habilitado.
    /// </summary>
    protected void AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled()
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: true);
        using var setupConnection = openConnection();
        setupConnection.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(100))");

        Parallel.For(1, 41, id =>
        {
            using var connection = openConnection();
            connection.Execute("INSERT INTO Users (Id, Name) VALUES (@Id, @Name)", new { Id = id, Name = $"Name{id}" });
        });

        var ids = setupConnection.Query<int>("SELECT Id FROM Users").ToList();
        Assert.Equal(40, ids.Count);
        Assert.Equal(40, ids.Distinct().Count());
    }

    /// <summary>
    /// EN: Verifies that concurrent commit and rollback keep only committed changes.
    /// PT: Verifica se commit e rollback concorrentes mantêm apenas alterações confirmadas.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versão do provedor em teste.</param>
    protected void AssertConcurrentCommitAndRollbackKeepsExpectedState(int version)
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: true, version: version);
        using var first = openConnection();
        using var second = openConnection();

        first.Execute("CREATE TABLE tx_concurrency_state (id INT PRIMARY KEY, value INT)");
        first.Execute("CREATE TABLE tx_concurrency_audit (id INT PRIMARY KEY, source VARCHAR(20))");
        first.Execute("INSERT INTO tx_concurrency_state (id, value) VALUES (1, 0)");

        using var sync = new Barrier(2);
        using var rollbackCompleted = new ManualResetEventSlim(false);

        var commitTask = Task.Run(() =>
        {
            using var tx = first.BeginTransaction();
            sync.SignalAndWait();
            rollbackCompleted.Wait();
            first.Execute("UPDATE tx_concurrency_state SET value = value + 10 WHERE id = 1", transaction: tx);
            first.Execute("INSERT INTO tx_concurrency_audit (id, source) VALUES (1, 'commit')", transaction: tx);
            tx.Commit();
        });

        var rollbackTask = Task.Run(() =>
        {
            using var tx = second.BeginTransaction();
            second.Execute("UPDATE tx_concurrency_state SET value = value + 100 WHERE id = 1", transaction: tx);
            second.Execute("INSERT INTO tx_concurrency_audit (id, source) VALUES (2, 'rollback')", transaction: tx);
            sync.SignalAndWait();
            tx.Rollback();
            rollbackCompleted.Set();
        });

        Task.WaitAll(commitTask, rollbackTask);

        var finalValue = first.ExecuteScalar<int>("SELECT value FROM tx_concurrency_state WHERE id = 1");
        var auditRows = first.Query<(int id, string source)>("SELECT id, source FROM tx_concurrency_audit ORDER BY id").ToList();

        Assert.Equal(10, finalValue);
        Assert.Single(auditRows);
        Assert.Equal((1, "commit"), auditRows[0]);
    }

    /// <summary>
    /// EN: Verifies that concurrent commits persist combined deterministic writes.
    /// PT: Verifica se commits concorrentes persistem gravações combinadas de forma determinística.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versão do provedor em teste.</param>
    protected void AssertConcurrentCommitsPersistCombinedWrites(int version)
    {
        var openConnection = CreateOpenConnectionFactory(threadSafe: true, version: version);
        using var first = openConnection();
        using var second = openConnection();

        first.Execute("CREATE TABLE tx_concurrency_commit (id INT PRIMARY KEY, value INT)");
        first.Execute("INSERT INTO tx_concurrency_commit (id, value) VALUES (1, 0)");

        using var sync = new Barrier(2);

        var txFirstTask = Task.Run(() =>
        {
            using var tx = first.BeginTransaction();
            first.Execute("UPDATE tx_concurrency_commit SET value = value + 10 WHERE id = 1", transaction: tx);
            sync.SignalAndWait();
            tx.Commit();
        });

        var txSecondTask = Task.Run(() =>
        {
            using var tx = second.BeginTransaction();
            second.Execute("UPDATE tx_concurrency_commit SET value = value + 100 WHERE id = 1", transaction: tx);
            sync.SignalAndWait();
            tx.Commit();
        });

        Task.WaitAll(txFirstTask, txSecondTask);

        var finalValue = first.ExecuteScalar<int>("SELECT value FROM tx_concurrency_commit WHERE id = 1");
        Assert.Equal(110, finalValue);
    }
}

/// <summary>
/// EN: Shared provider-specific implementation for Dapper transaction reliability tests.
/// PT: Implementação compartilhada específica por provedor para testes Dapper de confiabilidade transacional.
/// </summary>
public abstract class ProviderDapperTransactionReliabilityTestsBase<TDb, TConnection>(
        ITestOutputHelper helper
    ) : DapperTransactionConcurrencyTestsBase(helper)
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
{
    /// <summary>
    /// EN: Creates the provider database mock with the desired version and thread-safety mode.
    /// PT: Cria o banco mock do provedor com a versão e o modo thread-safe desejados.
    /// </summary>
    protected abstract TDb CreateDb(int? version, bool threadSafe);

    /// <summary>
    /// EN: Creates the provider connection bound to the supplied database mock.
    /// PT: Cria a conexão do provedor associada ao banco mock informado.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <inheritdoc />
    protected sealed override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = CreateDb(version, threadSafe);
        return () =>
        {
            var connection = CreateConnection(db);
            connection.Open();
            return connection;
        };
    }
}
