using System.Text.Json;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.TestTools.Tests.Performance;

/// <summary>
/// EN: Provides shared performance fidelity tests for lifecycle, diagnostics, and fluent payload workflows.
/// PT: Fornece testes de fidelidade de performance compartilhados para fluxos de ciclo de vida, diagnostico e payload fluent.
/// </summary>
public abstract class PerformanceTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that the lifecycle helper reopens the shared connection for mock and container runs.
    /// PT: Verifica se o helper de ciclo de vida reabre a conexao compartilhada nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ConnectionReopenAfterCloseTest()
        => RunConnectionReopenAfterCloseTest();

    /// <summary>
    /// EN: Verifies that the volatile-data reset helper keeps the shared connection usable.
    /// PT: Verifica se o helper de reset de dados volateis mantem a conexao compartilhada utilizavel.
    /// </summary>
    [Fact]
    public void ResetVolatileDataTest()
        => RunResetVolatileDataTest();

    /// <summary>
    /// EN: Verifies that the full volatile-data reset helper keeps the shared connection usable.
    /// PT: Verifica se o helper de reset completo de dados volateis mantem a conexao compartilhada utilizavel.
    /// </summary>
    [Fact]
    public void ResetAllVolatileDataTest()
        => RunResetAllVolatileDataTest();

    /// <summary>
    /// EN: Verifies that the debug-trace JSON payload remains stable for mock and container runs.
    /// PT: Verifica se o payload JSON de debug trace permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void DebugTraceJsonTest()
        => RunDebugTraceJsonTest();

    /// <summary>
    /// EN: Verifies that the fluent schema payload remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de schema permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void FluentSchemaBuildTest()
        => RunFluentSchemaBuildTest();

    /// <summary>
    /// EN: Verifies that the fluent seed payload with one hundred rows remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de seed com cem linhas permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void FluentSeed100Test()
        => RunFluentSeed100Test();

    /// <summary>
    /// EN: Verifies that the fluent seed payload with one thousand rows remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de seed com mil linhas permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void FluentSeed1000Test()
        => RunFluentSeed1000Test();

    /// <summary>
    /// EN: Verifies that the fluent scenario payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de composicao de cenario fluent permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void FluentScenarioComposeTest()
        => RunFluentScenarioComposeTest();

    private void RunConnectionReopenAfterCloseTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        RunConnectionReopenAfterCloseScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunConnectionReopenAfterCloseScenario(connContainer);
        }
    }

    private void RunResetVolatileDataTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        RunResetVolatileDataScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunResetVolatileDataScenario(connContainer);
        }
    }

    private void RunResetAllVolatileDataTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        RunResetAllVolatileDataScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunResetAllVolatileDataScenario(connContainer);
        }
    }

    private void RunDebugTraceJsonTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunDebugTraceJsonScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunDebugTraceJsonScenario(connContainer);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunFluentSchemaBuildTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunFluentSchemaBuildScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunFluentSchemaBuildScenario(connContainer);
            Assert.Equal(SerializeValue(resultMock), SerializeValue(resultContainer));
        }
    }

    private void RunFluentSeed100Test()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunFluentSeed100Scenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunFluentSeed100Scenario(connContainer);
            Assert.Equal(SerializeValue(resultMock), SerializeValue(resultContainer));
        }
    }

    private void RunFluentSeed1000Test()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunFluentSeed1000Scenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunFluentSeed1000Scenario(connContainer);
            Assert.Equal(SerializeValue(resultMock), SerializeValue(resultContainer));
        }
    }

    private void RunFluentScenarioComposeTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunFluentScenarioComposeScenario(connMock);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunFluentScenarioComposeScenario(connContainer);
            Assert.Equal(SerializeValue(resultMock), SerializeValue(resultContainer));
        }
    }

    private void RunConnectionReopenAfterCloseScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunConnectionReopenAfterClose();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private void RunResetVolatileDataScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunResetVolatileData();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private void RunResetAllVolatileDataScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunResetAllVolatileData();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private string RunDebugTraceJsonScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new DebugTraceServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunDebugTraceJson();
    }

    private object RunFluentSchemaBuildScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new FluentServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunFluentSchemaBuild();
    }

    private object RunFluentSeed100Scenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new FluentServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunFluentSeed100();
    }

    private object RunFluentSeed1000Scenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new FluentServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunFluentSeed1000();
    }

    private object RunFluentScenarioComposeScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new FluentServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunFluentScenarioCompose();
    }

    private static string SerializeValue(object? value)
        => JsonSerializer.Serialize(value);
}
