using System.Text.Json;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Query;

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
    /// EN: Verifies that opening a connection succeeds for mock and container runs.
    /// PT: Verifica se a abertura de uma conexao funciona nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ConnectionOpenTest()
        => RunConnectionOpenTest();

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
    /// EN: Verifies that schema creation succeeds for mock and container runs.
    /// PT: Verifica se a criacao de schema funciona nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void CreateSchemaTest()
        => RunCreateSchemaTest();

    /// <summary>
    /// EN: Verifies that table dropping succeeds for mock and container runs.
    /// PT: Verifica se a remocao de tabela funciona nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void DropTableTest()
        => RunDropTableTest();

    /// <summary>
    /// EN: Verifies that the debug-trace JSON payload remains stable for mock and container runs.
    /// PT: Verifica se o payload JSON de debug trace permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void DebugTraceJsonTest()
        => RunDebugTraceJsonTest();

    /// <summary>
    /// EN: Verifies that the debug-trace select payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de debug trace do select permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void DebugTraceSelectTest()
        => RunDebugTraceSelectTest();

    /// <summary>
    /// EN: Verifies that the debug-trace batch payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de debug trace do lote permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void DebugTraceBatchTest()
        => RunDebugTraceBatchTest();

    /// <summary>
    /// EN: Verifies the execution-plan benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ExecutionPlanTest()
        => RunExecutionPlanTest();

    /// <summary>
    /// EN: Verifies the execution-plan select alias remains stable for mock and container runs.
    /// PT: Verifica se o alias de select do plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ExecutionPlanSelectTest()
        => RunExecutionPlanTest();

    /// <summary>
    /// EN: Verifies the execution-plan join benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de join do plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ExecutionPlanJoinTest()
        => RunExecutionPlanJoinTest();

    /// <summary>
    /// EN: Verifies the execution-plan DML benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark DML de plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void ExecutionPlanDmlTest()
        => RunExecutionPlanDmlTest();

    /// <summary>
    /// EN: Verifies the execution-plan history benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de historico de planos de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void LastExecutionPlansHistoryTest()
        => RunLastExecutionPlansHistoryTest();

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

    private void RunConnectionOpenTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        connMock.State.Should().Be(ConnectionState.Open);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            connContainer.State.Should().Be(ConnectionState.Open);
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

    private void RunCreateSchemaTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        RunCreateSchemaScenario(connMock, uId);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunCreateSchemaScenario(connContainer, uId);
        }
    }

    private void RunDropTableTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        RunDropTableScenario(connMock, uId);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            RunDropTableScenario(connContainer, uId);
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
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunDebugTraceSelectTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunDebugTraceSelectScenario(connMock, users, uId);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunDebugTraceSelectScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunDebugTraceBatchTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunDebugTraceBatchScenario(connMock, users, uId);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunDebugTraceBatchScenario(connContainer, users, uId);
            resultMock.Should().Be(resultContainer);
        }
    }

    private void RunExecutionPlanTest()
    {
        var users = "Users";
        var uId = NewToken();
        var captureSelectExecutionPlan = SupportsExecutionPlanSelect(dialect.Provider);

        using var connMock = connectionMock();
        connMock.Open();
        if (connMock is DbConnectionMockBase mockConnection)
            mockConnection.CaptureExecutionPlans = captureSelectExecutionPlan;
        var resultMock = RunExecutionPlanSelectScenario(connMock, users, uId, captureSelectExecutionPlan);
        if (captureSelectExecutionPlan)
        {
            resultMock.Should().Contain("PlanMetadataVersion:");
        }
        else
        {
            resultMock.Should().BeEmpty();
        }

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunExecutionPlanSelectScenario(connContainer, users, uId, captureSelectExecutionPlan);
            if (!captureSelectExecutionPlan)
            {
                resultContainer.Should().BeEmpty();
            }
            NormalizeExecutionPlan(resultMock).Should().Be(NormalizeExecutionPlan(resultContainer));
        }
    }

    private void RunExecutionPlanJoinTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();
        var captureJoinExecutionPlan = SupportsExecutionPlanJoin(dialect.Provider);

        using var connMock = connectionMock();
        connMock.Open();
        if (connMock is DbConnectionMockBase mockConnection)
            mockConnection.CaptureExecutionPlans = captureJoinExecutionPlan;
        var resultMock = RunExecutionPlanJoinScenario(connMock, users, orders, uId, captureJoinExecutionPlan);
        if (captureJoinExecutionPlan)
        {
            resultMock.Should().Contain("PlanMetadataVersion:");
        }
        else
        {
            resultMock.Should().BeEmpty();
        }

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunExecutionPlanJoinScenario(connContainer, users, orders, uId, captureJoinExecutionPlan);
            if (!captureJoinExecutionPlan)
            {
                resultContainer.Should().BeEmpty();
            }
            NormalizeExecutionPlan(resultMock).Should().Be(NormalizeExecutionPlan(resultContainer));
        }
    }

    private void RunExecutionPlanDmlTest()
    {
        var users = "Users";
        var uId = NewToken();
        var captureDmlExecutionPlan = SupportsExecutionPlanDml(dialect.Provider);

        using var connMock = connectionMock();
        connMock.Open();
        if (connMock is DbConnectionMockBase mockConnection)
            mockConnection.CaptureExecutionPlans = captureDmlExecutionPlan;
        var resultMock = RunExecutionPlanDmlScenario(connMock, users, uId, captureDmlExecutionPlan);
        if (captureDmlExecutionPlan)
        {
            resultMock.Should().Contain("PlanMetadataVersion:");
        }
        else
        {
            resultMock.Should().BeEmpty();
        }

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunExecutionPlanDmlScenario(connContainer, users, uId, captureDmlExecutionPlan);
            if (!captureDmlExecutionPlan)
            {
                resultContainer.Should().BeEmpty();
            }
            NormalizeExecutionPlan(resultMock).Should().Be(NormalizeExecutionPlan(resultContainer));
        }
    }

    private void RunLastExecutionPlansHistoryTest()
    {
        var users = "Users";
        var uId = NewToken();
        var captureHistoryExecutionPlan = SupportsExecutionPlanHistory(dialect.Provider);

        using var connMock = connectionMock();
        connMock.Open();
        if (connMock is DbConnectionMockBase mockConnection)
            mockConnection.CaptureExecutionPlans = captureHistoryExecutionPlan;
        var resultMock = RunLastExecutionPlansHistoryScenario(connMock, users, uId, captureHistoryExecutionPlan);

        if (IsPerformanceContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunLastExecutionPlansHistoryScenario(connContainer, users, uId, captureHistoryExecutionPlan);
            SerializeValue(resultMock).Should().Be(SerializeValue(resultContainer));
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
            SerializeValue(resultMock).Should().Be(SerializeValue(resultContainer));
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
            SerializeValue(resultMock).Should().Be(SerializeValue(resultContainer));
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
            SerializeValue(resultMock).Should().Be(SerializeValue(resultContainer));
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
            SerializeValue(resultMock).Should().Be(SerializeValue(resultContainer));
        }
    }

    private void RunConnectionReopenAfterCloseScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunConnectionReopenAfterClose();
        connection.State.Should().Be(ConnectionState.Open);
    }

    private void RunResetVolatileDataScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunResetVolatileData();
        connection.State.Should().Be(ConnectionState.Open);
    }

    private void RunResetAllVolatileDataScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new ConnectionLifecycleServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        service.RunResetAllVolatileData();
        connection.State.Should().Be(ConnectionState.Open);
    }

    private string RunDebugTraceJsonScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new DebugTraceServiceTest<TConnection>(connection, new NoopScenario<TConnection>(), dialect);
        return service.RunDebugTraceJson();
    }

    private string RunDebugTraceSelectScenario<TConnection>(TConnection connection, string users, string uId)
        where TConnection : DbConnection
    {
        var service = new DebugTraceServiceTest<TConnection>(connection, new SelectTableScenario<TConnection>(dialect), dialect);
        service.CreateScenario(users, uId);
        try
        {
            var tableName = ResolveUsersTableName(users, uId);
            var trace = Convert.ToString(service.RunDebugTraceSelect(tableName), CultureInfo.InvariantCulture) ?? string.Empty;
            trace.Should().NotBeNullOrWhiteSpace();
            return trace;
        }
        finally
        {
            service.DropScenario(users, uId);
        }
    }

    private string RunDebugTraceBatchScenario<TConnection>(TConnection connection, string users, string uId)
        where TConnection : DbConnection
    {
        var service = new DebugTraceServiceTest<TConnection>(connection, new SelectTableScenario<TConnection>(dialect), dialect);
        service.CreateScenario(users, uId);
        try
        {
            var tableName = ResolveUsersTableName(users, uId);
            var trace = Convert.ToString(service.RunDebugTraceBatch(tableName, 2, 3), CultureInfo.InvariantCulture) ?? string.Empty;
            trace.Should().NotBeNullOrWhiteSpace();
            return trace;
        }
        finally
        {
            service.DropScenario(users, uId);
        }
    }

    private string RunExecutionPlanSelectScenario<TConnection>(TConnection connection, string users, string uId, bool captureExecutionPlan)
        where TConnection : DbConnection
    {
        var service = new ExecutionPlanServiceTest<TConnection>(connection, new UsersScenario<TConnection>(dialect, [(1, "Alice")]), dialect);
        service.CreateScenario(users, uId);
        try
        {
            var usersTable = ResolveUsersTableName(users, uId);
            var plan = Convert.ToString(service.RunExecutionPlanSelect(usersTable), CultureInfo.InvariantCulture) ?? string.Empty;
            if (captureExecutionPlan)
                plan.Should().NotBeNullOrWhiteSpace();
            else
                plan.Should().BeEmpty();
            return plan;
        }
        finally
        {
            service.DropScenario(users, uId);
        }
    }

    private string RunExecutionPlanJoinScenario<TConnection>(TConnection connection, string users, string orders, string uId, bool captureExecutionPlan)
        where TConnection : DbConnection
    {
        var service = new ExecutionPlanServiceTest<TConnection>(connection, new UsersOrdersScenario<TConnection>(dialect, [(1, "Alice")], [(1, 1, "order-1")]), dialect);
        service.CreateScenario(users, orders, uId);
        try
        {
            var usersTable = ResolveUsersTableName(users, uId);
            var ordersTable = ResolveOrdersTableName(orders, uId);
            var plan = Convert.ToString(service.RunExecutionPlanJoin(usersTable, ordersTable), CultureInfo.InvariantCulture) ?? string.Empty;
            if (captureExecutionPlan)
                plan.Should().NotBeNullOrWhiteSpace();
            else
                plan.Should().BeEmpty();
            return plan;
        }
        finally
        {
            service.DropScenario(users, orders, uId);
        }
    }

    private string RunExecutionPlanDmlScenario<TConnection>(TConnection connection, string users, string uId, bool captureExecutionPlan)
        where TConnection : DbConnection
    {
        var service = new ExecutionPlanServiceTest<TConnection>(connection, new InsertUsersScenario<TConnection>(dialect), dialect);
        service.CreateScenario(users, uId);
        try
        {
            var usersTable = ResolveUsersTableName(users, uId);
            var plan = Convert.ToString(service.RunExecutionPlanDml(usersTable, 1), CultureInfo.InvariantCulture) ?? string.Empty;
            if (!captureExecutionPlan)
                plan.Should().BeEmpty();
            return plan;
        }
        finally
        {
            service.DropScenario(users, uId);
        }
    }

    private object? RunLastExecutionPlansHistoryScenario<TConnection>(TConnection connection, string users, string uId, bool captureExecutionPlan)
        where TConnection : DbConnection
    {
        var service = new ExecutionPlanServiceTest<TConnection>(connection, new UsersScenario<TConnection>(dialect, [(1, "Alice")]), dialect);
        service.CreateScenario(users, uId);
        try
        {
            var usersTable = ResolveUsersTableName(users, uId);
            return NormalizeExecutionPlanHistory(service.RunLastExecutionPlansHistory(usersTable), captureExecutionPlan);
        }
        finally
        {
            service.DropScenario(users, uId);
        }
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

    private void RunCreateSchemaScenario<TConnection>(TConnection connection, string uId)
        where TConnection : DbConnection
    {
        var service = new CreateTableServiceTest<TConnection>(connection, new CreateTableScenario<TConnection>(), dialect);
        service.CreateScenario("Users", uId);
        try
        {
            service.RunTest("Users", uId);
        }
        finally
        {
            service.DropScenario("Users", uId);
        }
    }

    private void RunDropTableScenario<TConnection>(TConnection connection, string uId)
        where TConnection : DbConnection
    {
        var service = new DropTableServiceTest<TConnection>(connection, new DropTableScenario<TConnection>(), dialect);
        service.CreateScenario("Users", "Orders", uId);
        service.RunTest("Users", uId);
    }

    private string ResolveUsersTableName(string users, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? $"{users}_{uId}".ToLowerInvariant()
            : $"{users}_{uId}";

    private static string SerializeValue(object? value)
        => JsonSerializer.Serialize(value);

    private object? NormalizeExecutionPlanHistory(object? value, bool captureExecutionPlan)
    {
        if (captureExecutionPlan)
            return value;

        return value is IReadOnlyList<string> plans && plans.Count == 0
            ? null
            : value;
    }

    private string ResolveOrdersTableName(string tableName, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? $"{tableName}_{uId}".ToLowerInvariant()
            : $"{tableName}_{uId}";

    private static string NormalizeExecutionPlan(string? plan)
    {
        if (string.IsNullOrWhiteSpace(plan))
            return string.Empty;

        var lines = plan!
            .Replace("\r\n", "\n")
            .Split('\n')
            .Where(_ => !string.IsNullOrWhiteSpace(_))
            .Select(static line => line.TrimEnd())
            .Where(static line => !line.StartsWith("- PlanCorrelationId:", StringComparison.Ordinal));

        return string.Join(Environment.NewLine, lines);
    }

    private static bool SupportsExecutionPlanDml(ProviderId provider)
        => provider is not (ProviderId.MySql or ProviderId.MariaDb or ProviderId.Npgsql or ProviderId.Db2 or ProviderId.Oracle or ProviderId.SqlAzure or ProviderId.SqlServer or ProviderId.Sqlite);

    private static bool SupportsExecutionPlanJoin(ProviderId provider)
        => provider is not (ProviderId.MySql or ProviderId.MariaDb or ProviderId.Npgsql or ProviderId.Oracle or ProviderId.SqlAzure or ProviderId.SqlServer or ProviderId.Sqlite or ProviderId.Db2);

    private static bool SupportsExecutionPlanSelect(ProviderId provider)
        => provider is not (ProviderId.MySql or ProviderId.MariaDb or ProviderId.Npgsql or ProviderId.Oracle or ProviderId.SqlAzure or ProviderId.SqlServer or ProviderId.Sqlite);

    private static bool SupportsExecutionPlanHistory(ProviderId provider)
        => provider is not (ProviderId.MySql or ProviderId.MariaDb or ProviderId.Npgsql or ProviderId.Db2 or ProviderId.Oracle or ProviderId.SqlAzure or ProviderId.SqlServer or ProviderId.Sqlite);

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
