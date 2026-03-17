using MySqlConnector;
using Testcontainers.MySql;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs MySQL benchmarks against a MySQL container managed by Testcontainers.
/// PT-br: Executa benchmarks de MySQL contra um contêiner de MySQL gerenciado pelo Testcontainers.
/// </summary>
public sealed class MySqlTestcontainersSession()
    : ExternalBenchmarkSessionBase(new MySqlDialect(), BenchmarkEngine.Testcontainers)
{
    private MySqlContainer? _container;

    /// <summary>
    /// EN: Starts the MySQL container and returns its connection string.
    /// PT-br: Inicia o contêiner de MySQL e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started MySQL container. PT-br: A string de conexão exposta pelo contêiner de MySQL iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new MySqlBuilder("mysql:8.4")
            .WithDatabase("bench")
            .WithUsername("bench")
            .WithPassword("bench")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return _container.GetConnectionString();
    }

    /// <summary>
    /// EN: Creates a native MySQL connection for the provided connection string.
    /// PT-br: Cria uma conexão nativa de MySQL para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A native MySQL connection bound to the supplied connection string. PT-br: Uma conexão nativa de MySQL vinculada à string de conexão informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new MySqlConnection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the MySQL container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de MySQL quando a sessão de benchmark termina.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
