using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared DML fidelity tests for insert workflows across mock and container runs.
/// PT: Fornece testes de fidelidade DML compartilhados para fluxos de insert entre mock e container.
/// </summary>
public abstract class InsertTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a single insert persists one row for the current provider.
    /// PT: Verifica se um insert unico persiste uma linha para o provedor atual.
    /// </summary>
    [Fact]
    public void InsertSingleTest()
        => RunInsertCountTest(1);

    /// <summary>
    /// EN: Verifies the parameter insert benchmark persists one row for the current provider.
    /// PT: Verifica se o benchmark de insert parametrizado persiste uma linha para o provedor atual.
    /// </summary>
    [Fact]
    public void ParameterInsertSingleTest()
        => RunParameterInsertSingleTest();

    /// <summary>
    /// EN: Verifies that ten sequential inserts persist ten rows for the current provider.
    /// PT: Verifica se dez inserts sequenciais persistem dez linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void InsertBatch10Test()
        => RunInsertCountTest(10);

    /// <summary>
    /// EN: Verifies that one hundred sequential inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts sequenciais persistem cem linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void InsertBatch100Test()
        => RunInsertCountTest(100);

    /// <summary>
    /// EN: Verifies that one hundred parallel inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts paralelos persistem cem linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void InsertBatch100ParallelTest()
        => RunInsertParallelCountTest(100);

    /// <summary>
    /// EN: Verifies that the parallel insert benchmark persists one hundred rows for the current provider.
    /// PT: Verifica se o benchmark de insert paralelo persiste cem linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void ParallelInsertTest()
        => RunInsertParallelCountTest(100);

    /// <summary>
    /// EN: Verifies that a single insert reports a valid affected-row count for the current provider.
    /// PT: Verifica se um insert unico retorna uma contagem valida de linhas afetadas para o provedor atual.
    /// </summary>
    [Fact]
    public void RowCountAfterInsertTest()
        => RunRowCountAfterInsertTest();

    /// <summary>
    /// EN: Verifies that inserts starting from a custom id persist the expected key range and row names for the current provider.
    /// PT: Verifica se inserts iniciando em um id customizado persistem a faixa de chaves e os nomes esperados para o provedor atual.
    /// </summary>
    [Fact]
    public void InsertCustomStartIdTest()
        => RunInsertCustomStartIdTest();

    private void RunInsertCountTest(int rowCount)
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunInsertCountScenario(connMock, users, uId, rowCount);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunInsertCountScenario(connContainer, users, uId, rowCount);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunParameterInsertSingleTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunParameterInsertSingleScenario(connMock, users, uId);
        resultMock.Should().Be(1);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunParameterInsertSingleScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunInsertParallelCountTest(int rowCount)
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunInsertParallelCountScenario(connMock, users, uId, rowCount, connectionMock);

        if (dialect.Provider is not ProviderId.Sqlite
            && IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunInsertParallelCountScenario(connContainer, users, uId, rowCount, () => connectionContainer(connectionString));
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunRowCountAfterInsertTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunRowCountAfterInsertScenario(connMock, users, uId);
        resultMock.Should().BeGreaterThan(0);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunRowCountAfterInsertScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunInsertCustomStartIdTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunInsertCustomStartIdScenario(connMock, users, uId);
        resultMock.firstName.Should().Be("User-10");
        resultMock.lastName.Should().Be("User-12");

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunInsertCustomStartIdScenario(connContainer, users, uId);
            resultMock.firstName.Should().Be(resultContainer.firstName);
            resultMock.lastName.Should().Be(resultContainer.lastName);
        }
    }

    private int RunInsertCountScenario<TConnection>(
        TConnection connection,
        string users,
        string uId,
        int rowCount)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new InsertUsersServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunTest(users, uId, rowCount);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private int RunParameterInsertSingleScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new InsertUsersServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunParameterInsertSingle(users, uId);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private int RunRowCountAfterInsertScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new InsertUsersServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunRowCountAfterInsert(users, uId);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private (string firstName, string lastName) RunInsertCustomStartIdScenario<TConnection>(
        TConnection connection,
        string users,
        string uId)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new InsertUsersServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            serviceTest.RunTest(users, uId, 3, 10, 3);
            var tableName = BuildScenarioTableName(users, uId);
            var firstName = Convert.ToString(serviceTest.ExecuteScalar(dialect.SelectUserNameById(tableName, 10))) ?? string.Empty;
            var lastName = Convert.ToString(serviceTest.ExecuteScalar(dialect.SelectUserNameById(tableName, 12))) ?? string.Empty;
            return (firstName, lastName);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private int RunInsertParallelCountScenario<TConnection>(
        TConnection connection,
        string users,
        string uId,
        int rowCount,
        Func<TConnection> connectionFactory)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new InsertUsersServiceTest<TConnection>(connection, testScenario, dialect, connectionFactory);
        serviceTest.CreateScenario(users, uId);

        try
        {
            return serviceTest.RunParallelTest(users, uId, rowCount);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    private static string BuildScenarioTableName(string users, string uId)
        => $"{users}_{uId}";
}
