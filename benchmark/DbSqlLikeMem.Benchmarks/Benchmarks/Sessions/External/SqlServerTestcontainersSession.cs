using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs SQL Server benchmarks against a SQL Server container managed by Testcontainers.
/// PT-br: Executa benchmarks de SQL Server contra um contêiner de SQL Server gerenciado pelo Testcontainers.
/// </summary>
public sealed class SqlServerTestcontainersSession()
    : ExternalBenchmarkSessionBase(new SqlServerDialect(), BenchmarkEngine.Testcontainers)
{
    private MsSqlContainer? _container;

    /// <summary>
    /// EN: Starts the SQL Server container and returns its connection string.
    /// PT-br: Inicia o contêiner de SQL Server e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started SQL Server container. PT-br: A string de conexão exposta pelo contêiner de SQL Server iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .WithPassword("Bench_strong_Password_123!")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return _container.GetConnectionString();
    }

    /// <summary>
    /// EN: Creates a native SQL Server connection for the provided connection string.
    /// PT-br: Cria uma conexão nativa de SQL Server para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A native SQL Server connection bound to the supplied connection string. PT-br: Uma conexão nativa de SQL Server vinculada à string de conexão informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new SqlConnection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the SQL Server container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de SQL Server quando a sessão de benchmark termina.
    /// </summary>
    public override void Dispose()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
