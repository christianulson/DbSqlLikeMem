namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Base class for benchmark sessions that provision and benchmark an external runtime such as a Testcontainers database.
/// PT-br: Classe base para sessões de benchmark que provisionam e medem um runtime externo, como um banco em Testcontainers.
/// </summary>
/// <param name="dialect">EN: The provider-specific SQL dialect used to generate benchmark commands. PT-br: O dialeto SQL específico do provedor usado para gerar os comandos de benchmark.</param>
/// <param name="engine">EN: The benchmark engine that identifies the runtime behind the session. PT-br: O mecanismo de benchmark que identifica o runtime por trás da sessão.</param>
public abstract class ExternalBenchmarkSessionBase(
    ProviderSqlDialect dialect,
    BenchmarkEngine engine
    ) : BenchmarkSessionBase(dialect, engine)
{
    /// <summary>
    /// EN: Gets the connection string produced by the external runtime startup step.
    /// PT-br: Obtém a string de conexão produzida pela etapa de inicialização do runtime externo.
    /// </summary>
    protected string ConnectionString { get; private set; } = string.Empty;

    private bool OwnsRuntime { get; set; }

    /// <summary>
    /// EN: Starts the external runtime and captures its connection string for subsequent benchmark connections.
    /// PT-br: Inicia o runtime externo e captura sua string de conexão para as conexões usadas posteriormente nos benchmarks.
    /// </summary>
    public override void Initialize()
    {
        var configuredConnection = TryGetConfiguredConnectionString();
        if (!string.IsNullOrWhiteSpace(configuredConnection.ConnectionString))
        {
            ConnectionString = configuredConnection.ConnectionString;
            OwnsRuntime = false;
            Console.WriteLine($"[benchmarks] Using pre-provisioned {Dialect.DisplayName} runtime from environment variable '{configuredConnection.SourceName}'.");
            EnsureExternalRuntimeIsReady(ConnectionString);
            return;
        }

        ConnectionString = StartExternalRuntime();
        OwnsRuntime = true;
        EnsureExternalRuntimeIsReady(ConnectionString);
    }

    /// <summary>
    /// EN: Creates a provider-specific connection using the current external runtime connection string.
    /// PT-br: Cria uma conexão específica do provedor usando a string de conexão atual do runtime externo.
    /// </summary>
    /// <returns>EN: A provider-specific connection bound to the external runtime. PT-br: Uma conexão específica do provedor vinculada ao runtime externo.</returns>
    protected override DbConnection CreateConnection()
    {
        return CreateProviderConnection(ConnectionString);
    }

    /// <summary>
    /// EN: Releases the owned runtime resources when the session itself was responsible for provisioning them.
    /// PT-br: Libera os recursos do runtime quando a própria sessão foi responsável por provisioná-los.
    /// </summary>
    public override void Dispose()
    {
        if (OwnsRuntime)
        {
            DisposeOwnedRuntime();
        }
    }

    /// <summary>
    /// EN: Starts the external runtime and returns the connection string that should be used by the session.
    /// PT-br: Inicia o runtime externo e retorna a string de conexão que deve ser usada pela sessão.
    /// </summary>
    /// <returns>EN: The connection string that should be used to reach the started runtime. PT-br: A string de conexão que deve ser usada para acessar o runtime iniciado.</returns>
    protected abstract string StartExternalRuntime();

    /// <summary>
    /// EN: Creates the provider-specific connection wrapper for the supplied connection string.
    /// PT-br: Cria o wrapper de conexão específico do provedor para a string de conexão informada.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used to create the provider connection. PT-br: A string de conexão usada para criar a conexão do provedor.</param>
    /// <returns>EN: A provider-specific connection created from the supplied connection string. PT-br: Uma conexão específica do provedor criada a partir da string de conexão informada.</returns>
    protected abstract DbConnection CreateProviderConnection(string connectionString);

    /// <summary>
    /// EN: Returns the set of environment variables that may contain a pre-provisioned connection string for the current provider.
    /// PT-br: Retorna o conjunto de variáveis de ambiente que podem conter uma string de conexão pré-provisionada para o provedor atual.
    /// </summary>
    protected virtual IReadOnlyList<string> GetConfiguredConnectionStringEnvironmentVariables()
    {
        return Provider switch
        {
            BenchmarkProviderId.MySql => new[]
            {
                "DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING",
                "MYSQL_CONNECTION_STRING"
            },
            BenchmarkProviderId.SqlServer => new[]
            {
                "DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING",
                "SQLSERVER_CONNECTION_STRING"
            },
            BenchmarkProviderId.Oracle => new[]
            {
                "DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING",
                "ORACLE_CONNECTION_STRING"
            },
            BenchmarkProviderId.Npgsql => new[]
            {
                "DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING",
                "DBSQLLIKEMEM_BENCH_POSTGRES_CONNECTION_STRING",
                "NPGSQL_CONNECTION_STRING",
                "POSTGRES_CONNECTION_STRING"
            },
            BenchmarkProviderId.Db2 => new[]
            {
                "DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING",
                "DB2_CONNECTION_STRING"
            },
            _ => Array.Empty<string>()
        };
    }

    /// <summary>
    /// EN: Gives derived sessions a hook to wait until a configured or started external runtime is accepting client connections.
    /// PT-br: Oferece às sessões derivadas um ponto de extensão para aguardar até que um runtime externo configurado ou iniciado esteja aceitando conexões de cliente.
    /// </summary>
    /// <param name="connectionString">EN: The connection string used for readiness validation. PT-br: A string de conexão usada na validação de prontidão.</param>
    protected virtual void EnsureExternalRuntimeIsReady(string connectionString)
    {
        var deadline = DateTime.UtcNow.AddMinutes(2);
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var connection = CreateProviderConnection(connectionString);
                connection.Open();
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
        }

        throw new TimeoutException(
            $"The external runtime for provider '{Dialect.DisplayName}' did not become ready before the timeout expired.",
            lastError);
    }

    /// <summary>
    /// EN: Releases the resources of a runtime owned by the current session.
    /// PT-br: Libera os recursos de um runtime pertencente à sessão atual.
    /// </summary>
    protected virtual void DisposeOwnedRuntime()
    {
    }

    private (string ConnectionString, string SourceName) TryGetConfiguredConnectionString()
    {
        foreach (var variableName in GetConfiguredConnectionStringEnvironmentVariables())
        {
            var value = Environment.GetEnvironmentVariable(variableName);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return (value, variableName);
            }
        }

        return (string.Empty, string.Empty);
    }
}
