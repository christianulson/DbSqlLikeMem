using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
#if NET462 || NETSTANDARD2_0
using ITuple = DbSqlLikeMem.Compatibility.ITuple;
#endif

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides mock-only fidelity test helpers for provider-specific scenarios.
/// PT-br: Fornece helpers de testes de fidelidade apenas com mock para cenarios especificos de provider.
/// </summary>
/// <typeparam name="TCnn1"></typeparam>
/// <summary>
/// EN: Creates a mock-only test service for the supplied connection factory.
/// PT-br: Cria um service de teste apenas com mock para a factory de conexoes informada.
/// </summary>
/// <param name="connectionMock">EN: Delegate that creates the mock connection. PT-br: Delegado que cria a conexao mock.</param>
/// <param name="dialect">EN: SQL dialect used by the service. PT-br: Dialeto SQL usado pelo service.</param>
/// <param name="initialData">EN: Initial scenario rows. PT-br: Linhas iniciais do cenario.</param>
public class NotFidelityTestService<TCnn1>(
    Func<TCnn1> connectionMock,
    ProviderSqlDialect dialect,
    params object?[][] initialData)
    : IDisposable
    where TCnn1 : DbConnection
{
    /// <summary>
    /// EN: Gets the last execution plan snapshot captured before scenario teardown.
    /// PT-br: Obtem o ultimo snapshot do plano de execucao capturado antes da limpeza do cenario.
    /// </summary>
    protected string? LastExecutionPlanSnapshot { get; private set; }

    /// <summary>
    /// EN: Gets the last debug trace snapshot captured before scenario teardown.
    /// PT-br: Obtem o ultimo snapshot do trace de debug capturado antes da limpeza do cenario.
    /// </summary>
    protected global::DbSqlLikeMem.QueryDebugTrace? LastDebugTraceSnapshot { get; private set; }

    /// <summary>
    /// EN: Gets the repository service instance used for executing database operations in the test scenarios. This property is initialized in the constructor with a connection factory and SQL dialect, and it provides the necessary methods for interacting with the database during tests.
    /// PT-br: Obtém a instância do serviço de repositório usado para executar operações de banco de dados nos cenários de teste. Esta propriedade é inicializada no construtor com uma fábrica de conexões e um dialeto SQL, e fornece os métodos necessários para interagir com o banco de dados durante os testes.
    /// </summary>
    protected RepoService RepoMock { get; } = new RepoService(connectionMock, dialect);

    /// <summary>
    /// EN: Gets the initial data to be used for the test scenarios. This property is an array of object arrays, allowing for flexible representation of various types of initial data that may be required by different test scenarios. It is initialized in the constructor and can be used to set up the necessary state for the tests.
    /// PT-br: Obtém os dados iniciais a serem usados nos cenários de teste. Esta propriedade é um array de arrays de objetos, permitindo uma representação flexível de vários tipos de dados iniciais que podem ser necessários para diferentes cenários de teste. Ela é inicializada no construtor e pode ser usada para configurar o estado necessário para os testes.
    /// </summary>
    protected object?[][] InitialData { get; } = CloneInitialData(initialData);

    /// <summary>
    /// EN: Executes the specified test scenario on the mock connection and returns the service result.
    /// PT-br: Executa o cenario de teste na conexao mock e retorna o resultado do service.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public virtual Task<object?> RunTestAsync<TScenario, TServiceTest>(
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest, IBaseServiceTest
    => Execute<TScenario, TServiceTest>(args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes the specified test scenario on the mock connection and returns a typed service result.
    /// PT-br: Executa o cenario de teste na conexao mock e retorna um resultado tipado do service.
    /// </summary>
    public virtual Task<TResult> RunTestAsync<TScenario, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    => Execute<TScenario, TServiceTest, TResult>(fnRunTest, args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes two test scenarios on the mock connection and returns the service result.
    /// PT-br: Executa dois cenarios de teste na conexao mock e retorna o resultado do service.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public virtual async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest, IBaseServiceTest
    => Execute<TScenario, TScenario2, TServiceTest>(args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes two test scenarios on the mock connection and returns a typed service result.
    /// PT-br: Executa dois cenarios de teste na conexao mock e retorna um resultado tipado do service.
    /// </summary>
    public virtual Task<TResult> RunTestAsync<TScenario, TScenario2, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    => Execute<TScenario, TScenario2, TServiceTest, TResult>(fnRunTest, args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes the specified test scenario on the mock connection and returns the service result.
    /// PT-br: Executa o cenario de teste na conexao mock e retorna o resultado do service.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public virtual Task<object?> RunTestAsync<TScenario, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    => Execute<TScenario, TServiceTest>(fnRunTest, args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public virtual async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args
    ) where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    => Execute<TScenario, TScenario2, TServiceTest>(fnRunTest, args, RepoMock, InitialData, new FidelityTestContext());

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected async Task<object?> Execute<TScenario, TServiceTest>(
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
    where TScenario : BaseScenario, ITestScenario
    where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        using var debugTraceCapture = repo.Cnn is DbConnectionMockBase cnn
            ? cnn.BeginDebugTraceCapture()
            : null;
        var scenarioData = CloneInitialData(initialData);
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            objResult = await serviceTest!.RunTestAsync(args);
        }
        finally
        {
            CaptureDiagnostics(repo);
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and returns the typed result produced by the service test.
    /// PT-br: Executa o cenario de teste especificado e retorna o resultado tipado produzido pelo teste de servico.
    /// </summary>
    protected async Task<TResult> Execute<TScenario, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
        where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var debugTraceCapture = repo.Cnn is DbConnectionMockBase cnn
            ? cnn.BeginDebugTraceCapture()
            : null;
        var scenarioData = CloneInitialData(initialData);
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            return await fnRunTest(serviceTest!, args);
        }
        finally
        {
            CaptureDiagnostics(repo);
            await testScenario!.DropScenarioAsync();
        }
    }


    /// <summary>
    /// EN: Executes the specified test scenarios and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenarios and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa os cenários de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias dos cenários e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected async Task<object?> Execute<TScenario, TScenario2, TServiceTest>(
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
        where TScenario : BaseScenario, ITestScenario
    where TScenario2 : BaseScenario, ITestScenario
    where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        using var debugTraceCapture = repo.Cnn is DbConnectionMockBase cnn
            ? cnn.BeginDebugTraceCapture()
            : null;
        var scenarioData = CloneInitialData(initialData);
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var testScenario2 = CreateScenarioInstance<TScenario2>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario2, nameof(testScenario2));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            await testScenario2!.CreateScenarioAsync();
            objResult = await serviceTest!.RunTestAsync(args);
        }
        finally
        {
            CaptureDiagnostics(repo);
            await testScenario2!.DropScenarioAsync();
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }

    /// <summary>
    /// EN: Executes the specified test scenarios on the mock connection and returns a typed service result.
    /// PT-br: Executa os cenarios de teste na conexao mock e retorna um resultado tipado do service.
    /// </summary>
    protected async Task<TResult> Execute<TScenario, TScenario2, TServiceTest, TResult>(
        Func<TServiceTest, object[], Task<TResult>> fnRunTest,
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
        where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var debugTraceCapture = repo.Cnn is DbConnectionMockBase cnn
            ? cnn.BeginDebugTraceCapture()
            : null;
        var scenarioData = CloneInitialData(initialData);
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var testScenario2 = CreateScenarioInstance<TScenario2>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario2, nameof(testScenario2));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            await testScenario2!.CreateScenarioAsync();
            return await fnRunTest(serviceTest!, args);
        }
        finally
        {
            CaptureDiagnostics(repo);
            await testScenario2!.DropScenarioAsync();
            await testScenario!.DropScenarioAsync();
        }
    }

    /// <summary>
    /// EN: Captures the diagnostics that should survive scenario teardown.
    /// PT-br: Captura os diagnostics que devem sobreviver a limpeza do cenario.
    /// </summary>
    /// <param name="repo">EN: Repository used by the completed test run. PT-br: Repositorio usado pela execucao concluida do teste.</param>
    protected virtual void CaptureDiagnostics(RepoService repo)
    {
        _ = repo;
        SetDiagnosticsSnapshot(null, null);
    }

    /// <summary>
    /// EN: Stores the diagnostics snapshot captured before scenario teardown.
    /// PT-br: Armazena o snapshot de diagnostics capturado antes da limpeza do cenario.
    /// </summary>
    /// <param name="executionPlan">EN: Last execution plan snapshot. PT-br: Ultimo snapshot do plano de execucao.</param>
    /// <param name="debugTrace">EN: Last debug trace snapshot. PT-br: Ultimo snapshot do trace de debug.</param>
    protected void SetDiagnosticsSnapshot(string? executionPlan, QueryDebugTrace? debugTrace)
    {
        LastExecutionPlanSnapshot = executionPlan;
        LastDebugTraceSnapshot = debugTrace;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="fnRunTest"></param>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected static async Task<object?> Execute<TScenario, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
    where TScenario : BaseScenario, ITestScenario
    where TServiceTest : BaseServiceTest
    {
        var scenarioData = CloneInitialData(initialData);
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            objResult = await fnRunTest(serviceTest!, args);
        }
        finally
        {
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }

    /// <summary>
    /// EN: Executes the specified test scenarios and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenarios and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT-br: Executa os cenários de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias dos cenários e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="fnRunTest"></param>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected static async Task<object?> Execute<TScenario, TScenario2, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
        where TScenario : BaseScenario, ITestScenario
    where TScenario2 : BaseScenario, ITestScenario
    where TServiceTest : BaseServiceTest
    {
        var scenarioData = CloneInitialData(initialData);
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var testScenario2 = CreateScenarioInstance<TScenario2>(repo, context, scenarioData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario2, nameof(testScenario2));
        var serviceTest = Activator.CreateInstance(typeof(TServiceTest), repo, context) as TServiceTest;
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceTest, nameof(serviceTest));
        try
        {
            await testScenario!.CreateScenarioAsync();
            await testScenario2!.CreateScenarioAsync();
            objResult = await fnRunTest(serviceTest!, args);
        }
        finally
        {
            await testScenario2!.DropScenarioAsync();
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }

    #region Dispose 

    private bool disposedValue;

    /// <summary>
    /// EN: Releases the mock repository connection used by the test service.
    /// PT-br: Libera a conexao mock do repositorio usada pelo service de teste.
    /// </summary>
    /// <param name="disposing">EN: True when managed resources should be released. PT-br: True quando os recursos gerenciados devem ser liberados.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            RepoMock.Dispose();

            disposedValue = true;
        }
    }

    /// <summary>
    /// EN: Ensures the mock repository connection is released if Dispose is not called.
    /// PT-br: Garante que a conexao mock do repositorio seja liberada se Dispose nao for chamado.
    /// </summary>
    ~NotFidelityTestService()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    /// <summary>
    /// EN: Releases the mock repository connection and suppresses finalization.
    /// PT-br: Libera a conexao mock do repositorio e suprime a finalizacao.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion

    private static TScenario CreateScenarioInstance<TScenario>(
        RepoService repo,
        FidelityTestContext context,
        object?[][] initialData
    )
        where TScenario : BaseScenario, ITestScenario
    {
        var type = typeof(TScenario);
        if (initialData.Length == 0
            && TryCreateScenarioWithOptionalDefaults<TScenario>(type, repo, context, out var directScenario))
        {
            return directScenario;
        }

        var ctors = type.GetConstructors();
        foreach (var ctor in ctors)
        {
            var ps = ctor.GetParameters();
            if (ps.Length == 2)
            {
                return (TScenario)ctor.Invoke([repo, context]);
            }
            if (ps.Length == 3)
            {
                if (IsObjectMatrixParameter(ps[2].ParameterType))
                    return (TScenario)ctor.Invoke([repo, context, initialData]);

                if (initialData.Length <= 1)
                {
                    var arr = TryConvertToArray(initialData, 0, typeof((int, string)[]));
                    if (arr != null)
                        return (TScenario)ctor.Invoke([repo, context, arr]);
                    arr = TryConvertToArray(initialData, 0, typeof((int, int, string)[]));
                    if (arr != null)
                        return (TScenario)ctor.Invoke([repo, context, arr]);
                }
            }
            if (ps.Length == 4 && initialData.Length >= 2)
            {
                var arr1 = TryConvertToArray(initialData, 0, typeof((int, string)[]));
                var arr2 = TryConvertToArray(initialData, 1, typeof((int, int, string)[]));
                if (arr1 != null && arr2 != null)
                    return (TScenario)ctor.Invoke([repo, context, arr1, arr2]);
            }
        }

        if (initialData.Length > 0
            && TryCreateScenarioWithOptionalDefaults<TScenario>(type, repo, context, out directScenario))
        {
            return directScenario;
        }
        throw new MissingMethodException($"Constructor on type '{type.Name}' not found.");
    }

    private static bool TryCreateScenarioWithOptionalDefaults<TScenario>(
        Type type,
        RepoService repo,
        FidelityTestContext context,
        [MaybeNullWhen(false)] out TScenario scenario)
        where TScenario : BaseScenario, ITestScenario
    {
        scenario = default;

        foreach (var ctor in type.GetConstructors())
        {
            var ps = ctor.GetParameters();
            if (ps.Length < 2)
                continue;

            if (ps[0].ParameterType != typeof(RepoService)
                || ps[1].ParameterType != typeof(FidelityTestContext))
            {
                continue;
            }

            var args = new object?[ps.Length];
            args[0] = repo;
            args[1] = context;
            for (var i = 2; i < ps.Length; i++)
                args[i] = Type.Missing;

            try
            {
                scenario = ctor.Invoke(args) as TScenario;
                if (scenario is not null)
                    return true;
            }
            catch
            {
                // Try the next constructor shape.
            }
        }

        return false;
    }

    private static bool IsObjectMatrixParameter(Type parameterType)
        => parameterType == typeof(object[][])
            || parameterType == typeof(object?[][]);

    private static object? TryConvertToArray(object?[][] initialData, int index, Type targetType)
    {
        if (index >= initialData.Length)
            return null;
        var data = initialData[index];
        if (data == null || data.Length == 0)
            return null;
        if (targetType == typeof((int, string)[]))
        {
            var arr = new (int, string)[data.Length];
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] is ValueTuple<int, string> vt)
                    arr[i] = vt;
                else if (data[i] is Tuple<int, string> t)
                    arr[i] = (t.Item1, t.Item2);
                else if (data[i] is object[] oa && oa.Length >= 2)
                    arr[i] = (Convert.ToInt32(oa[0]), oa[1]?.ToString() ?? "");
                else if (data[i] is ITuple it && it.Length == 2)
                    arr[i] = ((int)it[0]!, it[1]?.ToString() ?? "");
                else
                    return null;
            }
            return arr;
        }
        if (targetType == typeof((int, int, string)[]))
        {
            var arr = new (int, int, string)[data.Length];
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] is ValueTuple<int, int, string> vt)
                    arr[i] = vt;
                else if (data[i] is Tuple<int, int, string> t)
                    arr[i] = (t.Item1, t.Item2, t.Item3);
                else if (data[i] is object[] oa && oa.Length >= 3)
                    arr[i] = (Convert.ToInt32(oa[0]), Convert.ToInt32(oa[1]), oa[2]?.ToString() ?? "");
                else if (data[i] is ITuple it && it.Length == 3)
                    arr[i] = ((int)it[0]!, (int)it[1]!, it[2]?.ToString() ?? "");
                else
                    return null;
            }
            return arr;
        }
        return null;
    }

    private static object?[][] CloneInitialData(object?[][] initialData)
    {
        if (initialData.Length == 0)
            return [];

        var clone = new object?[initialData.Length][];
        for (var i = 0; i < initialData.Length; i++)
            clone[i] = CloneInitialDataRow(initialData[i]);

        return clone;
    }

    private static object?[] CloneInitialDataRow(object?[]? row)
    {
        if (row is null || row.Length == 0)
            return [];

        var clone = new object?[row.Length];
        for (var i = 0; i < row.Length; i++)
        {
            clone[i] = row[i] is object?[] nestedRow
                ? CloneInitialDataRow(nestedRow)
                : row[i];
        }

        return clone;
    }
}
