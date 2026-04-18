using System.Reflection;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: FidelityTestService is a generic class designed to facilitate the execution of fidelity tests across different database providers. It abstracts the testing logic, allowing for the comparison of results between a mock implementation and a real containerized database connection. The class is parameterized with two types, TCnn1 and TCnn2, representing the connection types for the mock and container scenarios, respectively. This design promotes code reuse and consistency in testing across various database providers and scenarios.
/// PT: FidelityTestService é uma classe genérica projetada para facilitar a execução de testes de fidelidade entre diferentes provedores de banco de dados. Ela abstrai a lógica de teste, permitindo a comparação de resultados entre uma implementação mock e uma conexão real em um ambiente containerizado. A classe é parametrizada com dois tipos, TCnn1 e TCnn2, representando os tipos de conexão para os cenários mock e container, respectivamente. Esse design promove a reutilização de código e a consistência nos testes entre vários provedores de banco de dados e cenários.
/// </summary>
/// <typeparam name="TCnn1"></typeparam>
/// <remarks>
/// EN: Initializes a new instance of the FidelityTestService class using the specified connection factory.
/// PT: Inicializa uma nova instância da classe FidelityTestService usando a fábrica de conexões especificada.
/// </remarks>
/// <param name="connectionMock">A delegate that returns an instance of TCnn1 to be used as the connection for the service. Cannot be null.</param>
/// <param name="dialect">The SQL dialect to be used for the service. Cannot be null.</param>
/// <param name="initialData">Initial data to be used for the test scenario. Can be null.</param>
public class NotFidelityTestService<TCnn1>(
    Func<TCnn1> connectionMock,
    ProviderSqlDialect dialect,
    params object?[][] initialData)
    : IDisposable
    where TCnn1 : DbConnection
{
    /// <summary>
    /// EN: Gets the repository service instance used for executing database operations in the test scenarios. This property is initialized in the constructor with a connection factory and SQL dialect, and it provides the necessary methods for interacting with the database during tests.
    /// PT: Obtém a instância do serviço de repositório usado para executar operações de banco de dados nos cenários de teste. Esta propriedade é inicializada no construtor com uma fábrica de conexões e um dialeto SQL, e fornece os métodos necessários para interagir com o banco de dados durante os testes.
    /// </summary>
    protected RepoService RepoMock { get; } = new RepoService(connectionMock, dialect);

    /// <summary>
    /// EN: Gets the initial data to be used for the test scenarios. This property is an array of object arrays, allowing for flexible representation of various types of initial data that may be required by different test scenarios. It is initialized in the constructor and can be used to set up the necessary state for the tests.
    /// PT: Obtém os dados iniciais a serem usados nos cenários de teste. Esta propriedade é um array de arrays de objetos, permitindo uma representação flexível de vários tipos de dados iniciais que podem ser necessários para diferentes cenários de teste. Ela é inicializada no construtor e pode ser usada para configurar o estado necessário para os testes.
    /// </summary>
    protected object?[][] InitialData { get; } = initialData;

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected static async Task<object?> Execute<TScenario, TServiceTest>(
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
    where TScenario : BaseScenario, ITestScenario
    where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, initialData);
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
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }


    /// <summary>
    /// EN: Executes the specified test scenarios and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenarios and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa os cenários de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias dos cenários e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <param name="args"></param>
    /// <param name="repo"></param>
    /// <param name="initialData"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    protected static async Task<object?> Execute<TScenario, TScenario2, TServiceTest>(
        object[] args,
        RepoService repo,
        object?[][] initialData,
        FidelityTestContext context
    )
        where TScenario : BaseScenario, ITestScenario
        where TScenario2 : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest, IBaseServiceTest
    {
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, initialData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var testScenario2 = CreateScenarioInstance<TScenario2>(repo, context, initialData);
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
            await testScenario2!.DropScenarioAsync();
            await testScenario!.DropScenarioAsync();
        }

        return objResult;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, initialData);
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
    /// PT: Executa os cenários de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias dos cenários e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
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
        object? objResult = null;
        var testScenario = CreateScenarioInstance<TScenario>(repo, context, initialData);
        ArgumentNullExceptionCompatible.ThrowIfNull(testScenario, nameof(testScenario));
        var testScenario2 = CreateScenarioInstance<TScenario2>(repo, context, initialData);
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
    /// EN: Disposes the NotFidelityTestService instance, releasing all managed resources. If disposing is true, it disposes the Cnn2 connection if it exists. It always disposes the Cnn1 connection and sets the disposedValue flag to true to prevent multiple disposals.
    /// PT: Descarta a instância do NotFidelityTestService, liberando todos os recursos gerenciados. Se disposing for true, ele descarta a conexão Cnn2 se ela existir. Ele sempre descarta a conexão Cnn1 e define a flag disposedValue como true para evitar múltiplas liberações.
    /// </summary>
    /// <param name="disposing"></param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            RepoMock.Dispose();

            disposedValue = true;
        }
    }

    /// <summary>
    /// EN: Finalizer for the NotFidelityTestService class that ensures resources are released if Dispose is not called.
    /// PT: Finalizador para a classe NotFidelityTestService que garante que os recursos sejam liberados caso Dispose não seja chamado.
    /// </summary>
    ~NotFidelityTestService()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    /// <summary>
    /// EN: Disposes the NotFidelityTestService instance, releasing all managed resources. It calls the Dispose method with disposing set to true and suppresses finalization to optimize garbage collection.
    /// PT: Descarta a instância do NotFidelityTestService, liberando todos os recursos gerenciados. Ele chama o método Dispose com disposing definido como true e suprime a finalização para otimizar a coleta de lixo.
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
        throw new MissingMethodException($"Constructor on type '{type.Name}' not found.");
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
            }
            return arr;
        }
        return null;
    }
}
