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

    /// <summary>
    /// EN: Starts the external runtime and captures its connection string for subsequent benchmark connections.
    /// PT-br: Inicia o runtime externo e captura sua string de conexão para as conexões usadas posteriormente nos benchmarks.
    /// </summary>
    public override void Initialize()
    {
        ConnectionString = StartExternalRuntime();
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
}
