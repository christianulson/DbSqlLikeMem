using Oracle.ManagedDataAccess.Client;
using Testcontainers.Oracle;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs Oracle benchmarks against an Oracle container managed by Testcontainers.
/// PT-br: Executa benchmarks de Oracle contra um contêiner de Oracle gerenciado pelo Testcontainers.
/// </summary>
public sealed class OracleTestcontainersSession()
    : ExternalBenchmarkSessionBase(new OracleDialect(), BenchmarkEngine.Testcontainers)
{
    private OracleContainer? _container;

    /// <summary>
    /// EN: Starts the Oracle container and returns its connection string.
    /// PT-br: Inicia o contêiner de Oracle e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started Oracle container. PT-br: A string de conexão exposta pelo contêiner de Oracle iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new OracleBuilder("gvenzl/oracle-free:23-slim-faststart")
            .WithUsername("bench")
            .WithPassword("bench")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return $"{_container.GetConnectionString()};Max Pool Size=200;Connection Timeout=120";
    }

    /// <summary>
    /// EN: Creates a native Oracle connection for the provided connection string.
    /// PT-br: Cria uma conexão nativa de Oracle para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A native Oracle connection bound to the supplied connection string. PT-br: Uma conexão nativa de Oracle vinculada à string de conexão informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new OracleConnection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the Oracle container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de Oracle quando a sessão de benchmark termina.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
