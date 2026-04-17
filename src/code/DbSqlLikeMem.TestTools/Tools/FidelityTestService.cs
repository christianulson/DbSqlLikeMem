namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: FidelityTestService is a generic class designed to facilitate the execution of fidelity tests across different database providers. It abstracts the testing logic, allowing for the comparison of results between a mock implementation and a real containerized database connection. The class is parameterized with two types, TCnn1 and TCnn2, representing the connection types for the mock and container scenarios, respectively. This design promotes code reuse and consistency in testing across various database providers and scenarios.
/// PT: FidelityTestService é uma classe genérica projetada para facilitar a execução de testes de fidelidade entre diferentes provedores de banco de dados. Ela abstrai a lógica de teste, permitindo a comparação de resultados entre uma implementação mock e uma conexão real em um ambiente containerizado. A classe é parametrizada com dois tipos, TCnn1 e TCnn2, representando os tipos de conexão para os cenários mock e container, respectivamente. Esse design promove a reutilização de código e a consistência nos testes entre vários provedores de banco de dados e cenários.
/// </summary>
/// <typeparam name="TCnn1"></typeparam>
/// <typeparam name="TCnn2"></typeparam>
public class FidelityTestService<TCnn1, TCnn2>
    : NotFidelityTestService<TCnn1>
    where TCnn1 : DbConnection
    where TCnn2 : DbConnection
{
    private readonly RepoService? repoContainer;

    /// <summary>
    /// EN: Initializes a new instance of the FidelityTestService class using the specified connection factory.
    /// PT: Inicializa uma nova instância da classe FidelityTestService usando a fábrica de conexões especificada.
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
        ) : base(connectionMock, dialect, initialData)
    {
        if (TestEnv.RunContainerTests.Value
            && ProviderConnectionStringResolver.TryResolve(dialect.Provider, out var connectionString))
            repoContainer = new RepoService(() => connectionContainer(connectionString), dialect);
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TServiceTest>(params object[] args)
    {
        object? objResultContainer = null;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw new InvalidOperationException($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured. Set the environment variable RUN_CONTAINER_TESTS to false or provide a valid connection string to run container tests.");
            objResultContainer = await Execute<TScenario, TServiceTest>(args, repoContainer, InitialData);
        }

        var objResultMock = await Execute<TScenario, TServiceTest>(args, RepoMock, InitialData);

        if (TestEnv.RunContainerTests.Value)
            objResultMock.Should().BeEquivalentTo(objResultContainer
                //TODO: Terminar essa parte, talvez seja necessário criar um equivalency step customizado para lidar com as diferenças de precisão de DateTime entre os provedores
                //, options => options
                //.Using(new DateTimePrecisionEquivalencyStep(RepoMock.Dialect))
                //.WhenTypeIs<DateTime>()
                );

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(params object[] args)
    {
        object? objResultContainer = null;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw new InvalidOperationException($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured. Set the environment variable RUN_CONTAINER_TESTS to false or provide a valid connection string to run container tests.");
            objResultContainer = await Execute<TScenario, TScenario2, TServiceTest>(args, repoContainer, InitialData);
        }

        var objResultMock = await Execute<TScenario, TScenario2, TServiceTest>(args, RepoMock, InitialData);

        if (TestEnv.RunContainerTests.Value)
            objResultMock.Should().BeEquivalentTo(objResultContainer
                //TODO: Terminar essa parte, talvez seja necessário criar um equivalency step customizado para lidar com as diferenças de precisão de DateTime entre os provedores
                //, options => options
                //.Using(new DateTimePrecisionEquivalencyStep(RepoMock.Dialect))
                //.WhenTypeIs<DateTime>()
                );

        return objResultMock;
    }


    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args)
    {
        object? objResultContainer = null;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw new InvalidOperationException($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured. Set the environment variable RUN_CONTAINER_TESTS to false or provide a valid connection string to run container tests.");
            objResultContainer = await Execute<TScenario, TServiceTest>(fnRunTest, args, repoContainer, InitialData);
        }

        var objResultMock = await Execute<TScenario, TServiceTest>(fnRunTest, args, RepoMock, InitialData);

        if (TestEnv.RunContainerTests.Value)
            objResultMock.Should().BeEquivalentTo(objResultContainer
                //TODO: Terminar essa parte, talvez seja necessário criar um equivalency step customizado para lidar com as diferenças de precisão de DateTime entre os provedores
                //, options => options
                //.Using(new DateTimePrecisionEquivalencyStep(RepoMock.Dialect))
                //.WhenTypeIs<DateTime>()
                );

        return objResultMock;
    }

    /// <summary>
    /// EN: Executes the specified test scenario and service test, comparing results between mock and container runs when applicable. The method creates instances of the scenario and service test, sets them up with the appropriate connections and dialect, and executes the test logic. If container tests are enabled, it compares the results from both runs using FluentAssertions, accounting for any provider-specific precision differences in DateTime values.
    /// PT: Executa o cenário de teste e o teste de serviço especificados, comparando os resultados entre as execuções mock e container quando aplicável. O método cria instâncias do cenário e do teste de serviço, configura-os com as conexões e dialeto apropriados, e executa a lógica do teste. Se os testes de container estiverem habilitados, ele compara os resultados de ambas as execuções usando FluentAssertions, levando em consideração quaisquer diferenças de precisão específicas do provedor em valores DateTime.
    /// </summary>
    /// <typeparam name="TScenario"></typeparam>
    /// <typeparam name="TScenario2"></typeparam>
    /// <typeparam name="TServiceTest"></typeparam>
    /// <returns></returns>
    public override async Task<object?> RunTestAsync<TScenario, TScenario2, TServiceTest>(
        Func<TServiceTest, object[], Task<object?>> fnRunTest,
        params object[] args)
    {
        object? objResultContainer = null;
        if (TestEnv.RunContainerTests.Value)
        {
            if (repoContainer == null)
                throw new InvalidOperationException($"Container connection string for provider {RepoMock.Dialect.Provider} is not configured. Set the environment variable RUN_CONTAINER_TESTS to false or provide a valid connection string to run container tests.");
            objResultContainer = await Execute<TScenario, TScenario2, TServiceTest>(fnRunTest, args, repoContainer, InitialData);
        }

        var objResultMock = await Execute<TScenario, TScenario2, TServiceTest>(fnRunTest, args, RepoMock, InitialData);

        if (TestEnv.RunContainerTests.Value)
            objResultMock.Should().BeEquivalentTo(objResultContainer
                //TODO: Terminar essa parte, talvez seja necessário criar um equivalency step customizado para lidar com as diferenças de precisão de DateTime entre os provedores
                //, options => options
                //.Using(new DateTimePrecisionEquivalencyStep(RepoMock.Dialect))
                //.WhenTypeIs<DateTime>()
                );

        return objResultMock;
    }

    #region Dispose 

    /// <summary>
    /// EN: Disposes the FidelityTestService instance, releasing all managed resources. If disposing is true, it disposes the Cnn2 connection if it exists. It always disposes the Cnn1 connection and sets the disposedValue flag to true to prevent multiple disposals.
    /// PT: Descarta a instância do FidelityTestService, liberando todos os recursos gerenciados. Se disposing for true, ele descarta a conexão Cnn2 se ela existir. Ele sempre descarta a conexão Cnn1 e define a flag disposedValue como true para evitar múltiplas liberações.
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
    /// PT: Finalizador para a classe FidelityTestService que garante que os recursos sejam liberados caso Dispose não seja chamado.
    /// </summary>
    ~FidelityTestService()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    #endregion
}
