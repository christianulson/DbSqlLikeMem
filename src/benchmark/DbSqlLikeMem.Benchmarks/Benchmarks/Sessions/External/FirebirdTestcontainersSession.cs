using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DbSqlLikeMem.Firebird.TestTools;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs Firebird benchmarks against a Firebird container managed by Testcontainers.
/// PT-br: Executa benchmarks de Firebird contra um contêiner de Firebird gerenciado pelo Testcontainers.
/// </summary>
public sealed class FirebirdTestcontainersSession()
    : ExternalBenchmarkSessionBase(new FirebirdProviderSqlDialect(), BenchmarkEngine.Testcontainers)
{
    private const string Image = "firebirdsql/firebird:5.0.3-noble";
    private const string DatabasePath = "/var/lib/firebird/data/benchmark.fdb";
    private const ushort FirebirdPort = 3050;

    private IContainer? _container;

    /// <summary>
    /// EN: Starts the Firebird container and returns its connection string.
    /// PT-br: Inicia o contêiner de Firebird e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started Firebird container. PT-br: A string de conexão exposta pelo contêiner de Firebird iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new ContainerBuilder(Image)
            .WithPortBinding(FirebirdPort, true)
            .WithEnvironment("FIREBIRD_ROOT_PASSWORD", "masterkey")
            .WithEnvironment("FIREBIRD_USER", "benchmark")
            .WithEnvironment("FIREBIRD_PASSWORD", "benchmark")
            .WithEnvironment("FIREBIRD_DATABASE", DatabasePath)
            .WithEnvironment("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        var mappedPort = _container.GetMappedPublicPort(FirebirdPort);
        return $"User=benchmark;Password=benchmark;Database=127.0.0.1/{mappedPort}:{DatabasePath};Dialect=3;Charset=UTF8;Pooling=false;";
    }

    /// <summary>
    /// EN: Creates a native Firebird connection for the provided connection string.
    /// PT-br: Cria uma conexao nativa de Firebird para a string de conexao informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexao usada para criar a conexao do provedor.</param>
    /// <returns>EN: A native Firebird connection bound to the supplied connection string. PT-br: Uma conexao nativa de Firebird vinculada à string de conexao informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return FirebirdConnectionFactory.Create(connectionString);
    }

    /// <summary>
    /// EN: Executes the Firebird EXECUTE BLOCK benchmark that traps SQLSTATE 23000.
    /// PT-br: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    private void RunExecuteBlockSqlState23000()
    {
        using var runner = new NotFidelityTestService<DbConnection>(CreateConnection, Dialect);
        _ = runner.RunTestAsync<NoopScenario, FirebirdExecuteBlockSqlState23000ServiceTest>().GetAwaiter().GetResult();
    }

    /// <summary>
    /// EN: Dispatches the Firebird-specific benchmark feature before falling back to the shared implementation.
    /// PT-br: Encaminha o recurso de benchmark especifico do Firebird antes de delegar para a implementacao compartilhada.
    /// </summary>
    public override void Execute(BenchmarkFeatureId feature)
    {
        if (feature is BenchmarkFeatureId.JsonScalarRead
            or BenchmarkFeatureId.JsonPathRead
            or BenchmarkFeatureId.JsonInsertCast
            or BenchmarkFeatureId.ParameterProjection
            or BenchmarkFeatureId.ParameterTypeMatrix
            or BenchmarkFeatureId.TypedFieldStorageMatrix)
        {
            return;
        }

        if (feature == BenchmarkFeatureId.ExecuteBlockSqlState23000)
        {
            RunExecuteBlockSqlState23000();
            return;
        }

        base.Execute(feature);
    }

    /// <summary>
    /// EN: Disposes the Firebird container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de Firebird quando a sessao de benchmark termina.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
