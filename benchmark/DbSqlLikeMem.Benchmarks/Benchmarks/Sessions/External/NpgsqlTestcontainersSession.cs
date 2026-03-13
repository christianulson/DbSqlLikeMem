using Npgsql;
using Testcontainers.PostgreSql;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs PostgreSQL/Npgsql benchmarks against a PostgreSQL container managed by Testcontainers.
/// PT-br: Executa benchmarks de PostgreSQL/Npgsql contra um contêiner de PostgreSQL gerenciado pelo Testcontainers.
/// </summary>
public sealed class NpgsqlTestcontainersSession()
    : ExternalBenchmarkSessionBase(new NpgsqlDialect(), BenchmarkEngine.Testcontainers)
{
    private PostgreSqlContainer? _container;

    /// <summary>
    /// EN: Starts the PostgreSQL container and returns its connection string.
    /// PT-br: Inicia o contêiner de PostgreSQL e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started PostgreSQL container. PT-br: A string de conexão exposta pelo contêiner de PostgreSQL iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new PostgreSqlBuilder("postgres:17")
            .WithDatabase("bench")
            .WithUsername("bench")
            .WithPassword("bench")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return _container.GetConnectionString();
    }

    /// <summary>
    /// EN: Creates a native Npgsql connection for the provided connection string.
    /// PT-br: Cria uma conexão nativa Npgsql para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A native Npgsql connection bound to the supplied connection string. PT-br: Uma conexão nativa Npgsql vinculada à string de conexão informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new NpgsqlConnection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the PostgreSQL container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de PostgreSQL quando a sessão de benchmark termina.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
