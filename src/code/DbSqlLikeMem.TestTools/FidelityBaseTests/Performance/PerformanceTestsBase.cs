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
    [FidelityFact]
    public async Task ConnectionReopenAfterCloseTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<NoopScenario, ConnectionLifecycleReopenAfterServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that opening a connection succeeds for mock and container runs.
    /// PT: Verifica se a abertura de uma conexao funciona nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task ConnectionOpenTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<NoopScenario, ConnectionLifecycleOpenServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the volatile-data reset helper keeps the shared connection usable.
    /// PT: Verifica se o helper de reset de dados volateis mantem a conexao compartilhada utilizavel.
    /// </summary>
    [FidelityFact]
    public async Task ResetVolatileDataTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<NoopScenario, ConnectionLifecycleResetVolatileDataServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the full volatile-data reset helper keeps the shared connection usable.
    /// PT: Verifica se o helper de reset completo de dados volateis mantem a conexao compartilhada utilizavel.
    /// </summary>
    [FidelityFact]
    public async Task ResetAllVolatileDataTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<NoopScenario, ConnectionLifecycleResetAllVolatileDataServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that schema creation succeeds for mock and container runs.
    /// PT: Verifica se a criacao de schema funciona nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task CreateSchemaTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<CreateTableScenario, CreateTableServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that table dropping succeeds for mock and container runs.
    /// PT: Verifica se a remocao de tabela funciona nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task DropTableTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<DropTableScenario, DropTableServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the debug-trace JSON payload remains stable for mock and container runs.
    /// PT: Verifica se o payload JSON de debug trace permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task DebugTraceJsonTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<NoopScenario, DebugTraceJsonServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the debug-trace select payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de debug trace do select permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task DebugTraceSelectTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<SelectTableScenario, DebugTraceSelectServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the debug-trace batch payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de debug trace do lote permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task DebugTraceBatchTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<UsersScenario, DebugTraceBatchServiceTest>();
    }

    /// <summary>
    /// EN: Verifies the execution-plan benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task ExecutionPlanSelectTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect, [(1, "Alice")]);

        await testService.RunTestAsync<UsersScenario, ExecutionPlanSelectServiceTest>();
    }


    /// <summary>
    /// EN: Verifies the execution-plan join benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de join do plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task ExecutionPlanJoinTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect, [(1, "Alice")], [(1, 1, "order-1")]);

        await testService.RunTestAsync<UsersOrdersScenario, ExecutionPlanJoinServiceTest>();
    }

    /// <summary>
    /// EN: Verifies the execution-plan DML benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark DML de plano de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task ExecutionPlanDmlTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<InsertUsersScenario, ExecutionPlanDmlServiceTest>();
    }

    /// <summary>
    /// EN: Verifies the execution-plan history benchmark remains stable for mock and container runs.
    /// PT: Verifica se o benchmark de historico de planos de execucao permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task LastExecutionPlansHistoryTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<UsersScenario, LastExecutionPlansHistoryServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the fluent schema payload remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de schema permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task FluentSchemaBuildTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<NoopScenario, FluentServiceTest>(
            (service, args) => service.RunFluentSchemaBuildAsync(args));
    }

    /// <summary>
    /// EN: Verifies that the fluent seed payload with one hundred rows remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de seed com cem linhas permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task FluentSeed100Test()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<NoopScenario, FluentServiceTest>(
            (service, args) => service.RunFluentSeed100Async(args));
    }

    /// <summary>
    /// EN: Verifies that the fluent seed payload with one thousand rows remains stable for mock and container runs.
    /// PT: Verifica se o payload fluent de seed com mil linhas permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task FluentSeed1000Test()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<NoopScenario, FluentServiceTest>(
            (service, args) => service.RunFluentSeed1000Async(args));
    }

    /// <summary>
    /// EN: Verifies that the fluent scenario payload remains stable for mock and container runs.
    /// PT: Verifica se o payload de composicao de cenario fluent permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task FluentScenarioComposeTest()
    {
        using var testService = new NotFidelityTestService<T>(connectionMock, dialect);

        await testService.RunTestAsync<NoopScenario, FluentServiceTest>(
            (service, args) => service.RunFluentScenarioComposeAsync(args));
    }
}

