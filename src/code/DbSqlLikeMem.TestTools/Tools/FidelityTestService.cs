using System.Diagnostics;
using System.Text.Json;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: FidelityTestService is a generic class designed to facilitate the execution of fidelity tests across different database providers. It abstracts the testing logic, allowing for the comparison of results between a mock implementation and a real containerized database connection. The class is parameterized with two types, TCnn1 and TCnn2, representing the connection types for the mock and container scenarios, respectively. This design promotes code reuse and consistency in testing across various database providers and scenarios.
/// PT-br: FidelityTestService é uma classe genérica projetada para facilitar a execução de testes de fidelidade entre diferentes provedores de banco de dados. Ela abstrai a lógica de teste, permitindo a comparação de resultados entre uma implementação mock e uma conexão real em um ambiente containerizado. A classe é parametrizada com dois tipos, TCnn1 e TCnn2, representando os tipos de conexão para os cenários mock e container, respectivamente. Esse design promove a reutilização de código e a consistência nos testes entre vários provedores de banco de dados e cenários.
/// </summary>
/// <typeparam name="TCnn1"></typeparam>
/// <typeparam name="TCnn2"></typeparam>
public class FidelityTestService<TCnn1, TCnn2>
    : NotFidelityTestService<TCnn1>
    where TCnn1 : DbConnection
    where TCnn2 : DbConnection
{
    private readonly RepoService? repoContainer;
    private readonly TimeSpan temporalComparisonTolerance;

    /// <summary>
    /// EN: Initializes a new instance of the FidelityTestService class using the specified connection factory.
    /// PT-br: Inicializa uma nova instância da classe FidelityTestService usando a fábrica de conexões especificada.
    /// </summary>
    /// <param name="connectionMock">A delegate that returns an instance of TCnn1 to be used as the connection for the service. Cannot be null.</param>
    /// <param name="connectionContainer">A delegate that returns an instance of TCnn2 to be used as the connection for the containerized service. Cannot be null.</param>
    /// <param name="dialect">The SQL dialect to be used for the service. Cannot be null.</param>
    /// <param name="initialData">Initial data to be used for the test scenario. Can be null.</param>
    public FidelityTestService(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        params object?[][] initialData
        ) : this(
            connectionMock,
            connectionContainer,
            dialect,
            GetTemporalComparisonTolerance(dialect.Provider),
            initialData)
    {
    }

    /// <summary>
    /// EN: Initializes a new instance of the FidelityTestService class using a custom temporal comparison tolerance.
    /// PT-br: Inicializa uma nova instância da classe FidelityTestService usando uma tolerância temporal personalizada para comparação.
    /// </summary>
    /// <param name="connectionMock">EN: Delegate that creates the mock connection. PT-br: Delegado que cria a conexão mock.</param>
    /// <param name="connectionContainer">EN: Delegate that creates the container connection. PT-br: Delegado que cria a conexão do container.</param>
    /// <param name="dialect">EN: SQL dialect used by the service. PT-br: Dialeto SQL usado pelo serviço.</param>
    /// <param name="temporalComparisonTolerance">EN: Tolerance applied when comparing DateTime and DateTimeOffset values. PT-br: Tolerância aplicada na comparação de valores DateTime e DateTimeOffset.</param>
    /// <param name="initialData">EN: Initial scenario rows. PT-br: Linhas iniciais do cenário.</param>
    public FidelityTestService(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        TimeSpan temporalComparisonTolerance,
        params object?[][] initialData
        ) : base(connectionMock, dialect, initialData)
    {
        this.temporalComparisonTolerance = temporalComparisonTolerance;

        if (!TestEnv.RunContainerTests.Value
            || !ProviderConnectionStringResolver.TryResolve(dialect.Provider, out var connectionString))
            return;
        
        if (dialect.Provider == ProviderId.Sqlite && string.IsNullOrWhiteSpace(connectionString))
        {
            // Keep SQLite clones pointed at the same shared in-memory database for this test run.
            connectionString = $"Data Source=file:dbsqllikemem_{Guid.NewGuid():N}?mode=memory&cache=shared";
        }

        repoContainer = new RepoService(() => connectionContainer(connectionString), dialect);
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TServiceTest>(params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TServiceTest>(args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TServiceTest>(args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and returns the typed result after comparing mock and container runs.
    /// PT-br: Executa o cenario de teste especificado e retorna o resultado tipado apos comparar as execucoes mock e container.
    /// </summary>
    public override async Task<TResult> RunTestAsync<TScenario, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TServiceTest, TResult>(fnRunTest, args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TServiceTest, TResult>(fnRunTest, args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TScenario2, TServiceTest>(args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TScenario2, TServiceTest>(args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenarios and returns the typed result after comparing mock and container runs.
    /// PT-br: Executa os cenarios de teste especificados e retorna o resultado tipado apos comparar as execucoes mock e container.
    /// </summary>
    public override async Task<TResult> RunTestAsync<TScenario, TScenario2, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TScenario2, TServiceTest, TResult>(fnRunTest, args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TScenario2, TServiceTest, TResult>(fnRunTest, args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }


    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TServiceTest>(fnRunTest, args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TServiceTest>(fnRunTest, args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args)
    {
        var context = new FidelityTestContext();
        object? objResultContainer = null;
        var sw = Stopwatch.StartNew();
        long elapsedContainer = 0;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw Xunit.Sdk.SkipException.ForSkip($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured.");

            using var containerGate = CrossProcessProviderGate.Acquire(RepoMock.Dialect);
            await EnsureContainerConnectionAvailableAsync();
            objResultContainer = await Execute<TScenario, TScenario2, TServiceTest>(fnRunTest, args, repoContainer, InitialData, context);
            elapsedContainer = sw.ElapsedMilliseconds;
            sw.Restart();
        }

        var objResultMock = await Execute<TScenario, TScenario2, TServiceTest>(fnRunTest, args, RepoMock, InitialData, context);

        Trace();

        if (TestEnv.RunContainerTests.Value)
        {
            var elapsedMock = sw.ElapsedMilliseconds;
            Console.WriteLine($"CompareTime: {elapsedContainer} ms (container) / {elapsedMock} ms (mock), diff: {elapsedMock - elapsedContainer} ms");
            AssertEquivalentResults(objResultMock, objResultContainer);
        }

        return objResultMock;
    }

    private void AssertEquivalentResults(object? actual, object? expected)
        => actual.Should().BeEquivalentTo(expected, options => options
            .Using<DateTime>(context => context.Subject.Should().BeCloseTo(context.Expectation, temporalComparisonTolerance))
            .WhenTypeIs<DateTime>()
            .Using<DateTimeOffset>(context => context.Subject.Should().BeCloseTo(context.Expectation, temporalComparisonTolerance))
            .WhenTypeIs<DateTimeOffset>());

    private void Trace()
    {
        var cnn = (DbConnectionMockBase)RepoMock.Cnn;
        Console.WriteLine($"LastExecutionPlan: {cnn.LastExecutionPlan}");
        Console.WriteLine($"LastDebugTrace: {JsonSerializer.Serialize(cnn.LastDebugTrace)}");
    }

    private async Task EnsureContainerConnectionAvailableAsync()
    {
        if (repoContainer == null)
            return;

        try
        {
            await repoContainer.EnsureConnectionOpenAsync();
        }
        catch (DbException ex)
        {
            throw Xunit.Sdk.SkipException.ForSkip($"Container connection for provider {RepoMock.Dialect.Provider} is not available: {ex.Message}");
        }
        catch (InvalidOperationException ex)
        {
            throw Xunit.Sdk.SkipException.ForSkip($"Container connection for provider {RepoMock.Dialect.Provider} is not available: {ex.Message}");
        }
    }

    private static TimeSpan GetTemporalComparisonTolerance(ProviderId provider)
        => provider switch
        {
            ProviderId.Db2
            or ProviderId.MySql
            or ProviderId.MariaDb
            or ProviderId.Oracle => TimeSpan.FromSeconds(60),
            _ => TimeSpan.FromSeconds(10)
        };

    /// <summary>
    /// EN: Static helper method to run a fidelity test with the specified parameters, creating an instance of FidelityTestService and executing the provided test logic. This method abstracts the setup and execution of the test, allowing for a more concise and reusable way to run fidelity tests across different scenarios and service tests.
    /// PT-br: Método auxiliar estático para executar um teste de fidelidade com os parâmetros especificados, criando uma instância de FidelityTestService e executando a lógica de teste fornecida. Este método abstrai a configuração e execução do teste, permitindo uma maneira mais concisa e reutilizável de executar testes de fidelidade em diferentes cenários e testes de serviço.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="connectionMock"></param>
    /// <param name="connectionContainer"></param>
    /// <param name="dialect"></param>
    /// <param name="initialData"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public static async Task<object?> RunAsync<TScenario, TServiceTest>(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        object?[][] initialData,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        using var testService = new FidelityTestService<TCnn1, TCnn2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest>(args);
    }

    /// <summary>
    /// EN: Static helper method to run a fidelity test with the specified parameters, creating an instance of FidelityTestService and executing the provided test logic. This method abstracts the setup and execution of the test, allowing for a more concise and reusable way to run fidelity tests across different scenarios and service tests.
    /// PT-br: Método auxiliar estático para executar um teste de fidelidade com os parâmetros especificados, criando uma instância de FidelityTestService e executando a lógica de teste fornecida. Este método abstrai a configuração e execução do teste, permitindo uma maneira mais concisa e reutilizável de executar testes de fidelidade em diferentes cenários e testes de serviço.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="connectionMock"></param>
    /// <param name="connectionContainer"></param>
    /// <param name="dialect"></param>
    /// <param name="initialData"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public static async Task<object?> RunAsync<TScenario, TScenario2, TServiceTest>(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        object?[][] initialData,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        using var testService = new FidelityTestService<TCnn1, TCnn2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TScenario2, TServiceTest>(args);
    }

    /// <summary>
    /// EN: Static helper method to run a fidelity test with the specified parameters, creating an instance of FidelityTestService and executing the provided test logic. This method abstracts the setup and execution of the test, allowing for a more concise and reusable way to run fidelity tests across different scenarios and service tests.
    /// PT-br: Método auxiliar estático para executar um teste de fidelidade com os parâmetros especificados, criando uma instância de FidelityTestService e executando a lógica de teste fornecida. Este método abstrai a configuração e execução do teste, permitindo uma maneira mais concisa e reutilizável de executar testes de fidelidade em diferentes cenários e testes de serviço.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="connectionMock"></param>
    /// <param name="connectionContainer"></param>
    /// <param name="dialect"></param>
    /// <param name="initialData"></param>
    /// <param name="runTest"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public static async Task<object?> RunAsync<TScenario, TServiceTest>(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        object?[][] initialData,
        Func<TServiceTest, object[], Task<object?>> runTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<TCnn1, TCnn2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest>(runTest, args);
    }

    /// <summary>
    /// EN: Static helper method to run a fidelity test with the specified parameters, creating an instance of FidelityTestService and executing the provided test logic. This method abstracts the setup and execution of the test, allowing for a more concise and reusable way to run fidelity tests across different scenarios and service tests.
    /// PT-br: Método auxiliar estático para executar um teste de fidelidade com os parâmetros especificados, criando uma instância de FidelityTestService e executando a lógica de teste fornecida. Este método abstrai a configuração e execução do teste, permitindo uma maneira mais concisa e reutilizável de executar testes de fidelidade em diferentes cenários e testes de serviço.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="connectionMock"></param>
    /// <param name="connectionContainer"></param>
    /// <param name="dialect"></param>
    /// <param name="initialData"></param>
    /// <param name="runTest"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public static async Task<object?> RunAsync<TScenario, TScenario2, TServiceTest>(
        Func<TCnn1> connectionMock,
        Func<string, TCnn2> connectionContainer,
        ProviderSqlDialect dialect,
        object?[][] initialData,
        Func<TServiceTest, object[], Task<object?>> runTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<TCnn1, TCnn2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TScenario2, TServiceTest>(runTest, args);
    }

    #region Dispose 

    /// <summary>
    /// EN: Disposes the FidelityTestService instance, releasing all managed resources. If disposing is true, it disposes the Cnn2 connection if it exists. It always disposes the Cnn1 connection and sets the disposedValue flag to true to prevent multiple disposals.
    /// PT-br: Descarta a instância do FidelityTestService, liberando todos os recursos gerenciados. Se disposing for true, ele descarta a conexão Cnn2 se ela existir. Ele sempre descarta a conexão Cnn1 e define a flag disposedValue como true para evitar múltiplas liberações.
    /// </summary>
    /// <param name="disposing"></param>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            repoContainer?.Dispose();
        base.Dispose(disposing);
    }

    /// <summary>
    /// EN: Finalizer for the FidelityTestService class that ensures resources are released if Dispose is not called.
    /// PT-br: Finalizador para a classe FidelityTestService que garante que os recursos sejam liberados caso Dispose não seja chamado.
    /// </summary>
    ~FidelityTestService()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    #endregion
}
