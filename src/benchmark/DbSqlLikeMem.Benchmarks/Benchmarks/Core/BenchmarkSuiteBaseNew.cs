//namespace DbSqlLikeMem.Benchmarks.Benchmarks.Core;

//internal class BenchmarkSuiteBaseNew<TCnn1, TScenario>
//    : IDisposable
//    where TCnn1 : DbConnection
//    where TScenario : BaseScenario, ITestScenario
//{
//    /// <summary>
//    /// EN: Gets the initial data to be used for the test scenarios. This property is an array of object arrays, allowing for flexible representation of various types of initial data that may be required by different test scenarios. It is initialized in the constructor and can be used to set up the necessary state for the tests.
//    /// PT-br: Obtém os dados iniciais a serem usados nos cenários de teste. Esta propriedade é um array de arrays de objetos, permitindo uma representação flexível de vários tipos de dados iniciais que podem ser necessários para diferentes cenários de teste. Ela é inicializada no construtor e pode ser usada para configurar o estado necessário para os testes.
//    /// </summary>
//    protected object?[][] InitialData { get; private set; }

//    protected FidelityTestContext Context { get; private set; }

//    /// <summary>
//    /// EN: Gets the repository service instance used for executing database operations in the test scenarios. This property is initialized in the constructor with a connection factory and SQL dialect, and it provides the necessary methods for interacting with the database during tests.
//    /// PT-br: Obtém a instância do serviço de repositório usado para executar operações de banco de dados nos cenários de teste. Esta propriedade é inicializada no construtor com uma fábrica de conexões e um dialeto SQL, e fornece os métodos necessários para interagir com o banco de dados durante os testes.
//    /// </summary>
//    protected RepoService RepoMock { get; private set; }

//    /// <summary>
//    /// 
//    /// </summary>
//    protected TScenario Scenario { get; private set; }

//    /// <summary>
//    /// EN: Initializes a new instance of the BenchmarkSuiteBaseNew class with the specified connection factory, SQL dialect, and initial data. This constructor sets up the necessary components for the benchmark suite, including the repository service and the test scenario, using the provided parameters.
//    /// PT-br: Inicializa uma nova instância da classe BenchmarkSuiteBaseNew com a fábrica de conexões, dialeto SQL e dados iniciais especificados. Este construtor configura os componentes necessários para o conjunto de benchmarks, incluindo o serviço de repositório e o cenário de teste, usando os parâmetros fornecidos.
//    /// </summary>
//    /// <param name="connectionMock"></param>
//    /// <param name="dialect"></param>
//    /// <param name="initialData"></param>
//    protected BenchmarkSuiteBaseNew(
//        Func<TCnn1> connectionMock,
//        ProviderSqlDialect dialect,
//        object?[][] initialData)
//    {
//        InitialData = initialData;
//        Context = new FidelityTestContext();
//        RepoMock = new RepoService(connectionMock, dialect);
//        Scenario = (Activator.CreateInstance(typeof(TScenario), [RepoMock, Context, .. InitialData]) as TScenario)
//            ?? throw new InvalidOperationException($"Unable to create instance of {typeof(TScenario).Name}");
//    }

//    /// <summary>
//    /// EN: Prepares the benchmark session before the runs start.
//    /// PT-br: Prepara a sessao de benchmark antes do inicio das execucoes.
//    /// </summary>
//    [IterationSetup]
//    public Task IterationSetup()
//    => Scenario.CreateScenarioAsync();

//    /// <summary>
//    /// EN: Releases the benchmark session after the runs finish.
//    /// PT-br: Libera a sessao de benchmark depois que as execucoes terminam.
//    /// </summary>
//    [IterationCleanup]
//    public Task IterationCleanup()
//    => Scenario.DropScenarioAsync();

//    #region Dispose

//    private bool disposedValue;

//    protected virtual void Dispose(bool disposing)
//    {
//        if (!disposedValue)
//        {
//            if (disposing)
//            {
//                // TODO: dispose managed state (managed objects)
//            }

//            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
//            // TODO: set large fields to null
//            disposedValue = true;
//        }
//    }

//    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
//    ~BenchmarkSuiteBaseNew()
//    {
//        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
//        Dispose(disposing: false);
//    }

//    public void Dispose()
//    {
//        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
//        Dispose(disposing: true);
//        GC.SuppressFinalize(this);
//    }

//    #endregion
//}
