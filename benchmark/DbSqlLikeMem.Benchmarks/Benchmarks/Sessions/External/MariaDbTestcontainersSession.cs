using MySqlConnector;
using Testcontainers.MariaDb;
using DbSqlLikeMem.MariaDb.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs MariaDB benchmarks against a MariaDB container managed by Testcontainers.
/// PT-br: Executa benchmarks de MariaDB contra um contêiner de MariaDB gerenciado pelo Testcontainers.
/// </summary>
public sealed class MariaDbTestcontainersSession()
    : ExternalBenchmarkSessionBase(new MariaDbProviderSqlDialect(), BenchmarkEngine.Testcontainers)
{
    private MariaDbContainer? _container;

    /// <summary>
    /// EN: Starts the MariaDB container and returns its connection string.
    /// PT-br: Inicia o contêiner de MariaDB e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started MariaDB container. PT-br: A string de conexão exposta pelo contêiner de MariaDB iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new MariaDbBuilder("mariadb:11.0")
            .WithDatabase("bench")
            .WithUsername("bench")
            .WithPassword("bench")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return _container.GetConnectionString();
    }

    /// <summary>
    /// EN: Creates a native MySQL connection for the provided MariaDB connection string.
    /// PT-br: Cria uma conexao nativa MySQL para a string de conexao MariaDB informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexao usada para criar a conexao do provedor.</param>
    /// <returns>EN: A native MySQL connection bound to the supplied connection string. PT-br: Uma conexao nativa MySQL vinculada a string de conexao informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
        => new MySqlConnection(connectionString);

    /// <summary>
    /// EN: Disposes the MariaDB container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de MariaDB quando a sessão de benchmark termina.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
