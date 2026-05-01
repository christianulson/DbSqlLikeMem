namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class ExternalBenchmarkSessionBase
{
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
        base.Dispose();
        if (OwnsRuntime)
        {
            DisposeOwnedRuntime();
        }
    }

    /// <summary>
    /// EN: Skips the stored procedure benchmark for external runtimes that do not expose the mock procedure registry.
    /// PT-br: Ignora o benchmark de procedimento armazenado para runtimes externos que nao expõem o registro mock de procedimentos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StoredProcedureCall)]
    protected override void RunStoredProcedureCall()
    {
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
#pragma warning disable CA1031 // Do not catch general exception types
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
#pragma warning restore CA1031 // Do not catch general exception types
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
}
