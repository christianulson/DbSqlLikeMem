using IBM.Data.Db2;
using Testcontainers.Db2;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs DB2 benchmarks against a DB2 container managed by Testcontainers.
/// PT-br: Executa benchmarks de DB2 contra um contêiner de DB2 gerenciado pelo Testcontainers.
/// </summary>
public sealed class Db2TestcontainersSession() : ExternalBenchmarkSessionBase(new Db2Dialect(), BenchmarkEngine.Testcontainers)
{
    private Db2Container? _container;

    /// <summary>
    /// EN: Starts the DB2 container and returns its connection string.
    /// PT-br: Inicia o contêiner de DB2 e retorna sua string de conexão.
    /// </summary>
    /// <returns>EN: The connection string exposed by the started DB2 container. PT-br: A string de conexão exposta pelo contêiner de DB2 iniciado.</returns>
    protected override string StartExternalRuntime()
    {
        _container = new Db2Builder("icr.io/db2_community/db2:12.1.0.0")
            .WithAcceptLicenseAgreement(true)
            .WithDatabase("BENCH")
            .WithUsername("db2inst1")
            .WithPassword("db2inst1")
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();
        return _container.GetConnectionString();
    }

    /// <summary>
    /// EN: Creates a native DB2 connection for the provided connection string.
    /// PT-br: Cria uma conexão nativa de DB2 para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A native DB2 connection bound to the supplied connection string. PT-br: Uma conexão nativa de DB2 vinculada à string de conexão informada.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new DB2Connection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the DB2 container when the benchmark session finishes.
    /// PT-br: Libera o contêiner de DB2 quando a sessão de benchmark termina.
    /// </summary>
    public override void Dispose()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
