using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.Transactions;

/// <summary>
/// EN: Provides shared transaction fidelity tests for commit, rollback, and savepoint workflows across mock and container runs.
/// PT: Fornece testes de fidelidade de transacao compartilhados para fluxos de commit, rollback e savepoint entre mock e container.
/// </summary>
public abstract class TransactionTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a transaction commit persists the inserted row for the current provider.
    /// PT: Verifica se o commit de uma transacao persiste a linha inserida para o provedor atual.
    /// </summary>
    [Fact]
    public void TransactionCommitTest()
        => RunTransactionCommitTest();

    /// <summary>
    /// EN: Verifies that a transaction rollback removes the inserted row for the current provider.
    /// PT: Verifica se o rollback de uma transacao remove a linha inserida para o provedor atual.
    /// </summary>
    [Fact]
    public void TransactionRollbackTest()
        => RunTransactionRollbackTest();

    /// <summary>
    /// EN: Verifies that creating a savepoint works for the current provider.
    /// PT: Verifica se a criacao de um savepoint funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void SavepointCreateTest()
        => RunSavepointCreateTest();

    /// <summary>
    /// EN: Verifies that rolling back to a savepoint keeps the expected row count for the current provider.
    /// PT: Verifica se o rollback para um savepoint mantem a contagem de linhas esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void RollbackToSavepointTest()
        => RunRollbackToSavepointTest();

    /// <summary>
    /// EN: Verifies that releasing a savepoint works for the current provider.
    /// PT: Verifica se a liberacao de um savepoint funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void ReleaseSavepointTest()
        => RunReleaseSavepointTest();

    /// <summary>
    /// EN: Verifies that nested savepoints keep the expected row count for the current provider.
    /// PT: Verifica se savepoints aninhados mantem a contagem de linhas esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void NestedSavepointFlowTest()
        => RunNestedSavepointFlowTest();

    private void RunTransactionCommitTest()
    {
        var uId = NewToken();
        var users = "Users";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunTransactionCommitScenario(connMock, users, uId);

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunTransactionCommitScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunTransactionRollbackTest()
    {
        var uId = NewToken();
        var users = "Users";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunTransactionRollbackScenario(connMock, users, uId);

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunTransactionRollbackScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunSavepointCreateTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        RunSavepointCreateScenario(connMock);

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunSavepointCreateScenario(connContainer);
        }
    }

    private void RunRollbackToSavepointTest()
    {
        var uId = NewToken();
        var users = "Users";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunRollbackToSavepointScenario(connMock, users, uId);

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunRollbackToSavepointScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunReleaseSavepointTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        if (SupportsReleaseSavepointWorkflow())
        {
            RunReleaseSavepointScenario(connMock);
        }
        else
        {
            FluentActions.Invoking(() => RunReleaseSavepointScenario(connMock)).Should().Throw<NotSupportedException>();
        }

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            if (SupportsReleaseSavepointWorkflow())
            {
                RunReleaseSavepointScenario(connContainer);
            }
            else
            {
                FluentActions.Invoking(() => RunReleaseSavepointScenario(connContainer)).Should().Throw<NotSupportedException>();
            }
        }
    }

    private void RunNestedSavepointFlowTest()
    {
        var uId = NewToken();
        var users = "Users";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunNestedSavepointFlowScenario(connMock, users, uId);

        if (IsTransactionContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunNestedSavepointFlowScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private int RunTransactionCommitScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new UsersScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        var usersTable = $"{users}_{uId}";
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunTransactionCommit(usersTable);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private int RunTransactionRollbackScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new UsersScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        var usersTable = $"{users}_{uId}";
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunTransactionRollback(usersTable);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private void RunSavepointCreateScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        serviceTest.CreateScenario();

        try
        {
            serviceTest.RunSavepointCreate();
        }
        finally
        {
            serviceTest.DropScenario();
        }
    }

    private int RunRollbackToSavepointScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new UsersScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        var usersTable = $"{users}_{uId}";
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunRollbackToSavepoint(usersTable);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private void RunReleaseSavepointScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        serviceTest.CreateScenario();

        try
        {
            serviceTest.RunReleaseSavepoint();
        }
        finally
        {
            serviceTest.DropScenario();
        }
    }

    private int RunNestedSavepointFlowScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new UsersScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        var usersTable = $"{users}_{uId}";
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunNestedSavepointFlow(usersTable);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    private bool SupportsReleaseSavepointWorkflow()
        => dialect.Provider is not ProviderId.SqlServer
            and not ProviderId.SqlAzure
            and not ProviderId.Oracle;
}
