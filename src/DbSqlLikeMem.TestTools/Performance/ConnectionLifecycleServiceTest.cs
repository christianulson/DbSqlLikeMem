namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes connection-lifecycle benchmark workflows and validates the observed provider behavior.
/// PT: Executa fluxos de benchmark de ciclo de vida da conexao e valida o comportamento observado do provedor.
/// </summary>
public class ConnectionLifecycleServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : PerformanceServiceBase<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Invokes the provider reset helper when it is available.
    /// PT: Invoca o helper de reset do provedor quando ele esta disponivel.
    /// </summary>
    public void RunResetVolatileData()
    {
        TryInvokeIfExists(Connection, "ResetVolatileData");
        GC.KeepAlive(Connection.State);
    }

    /// <summary>
    /// EN: Invokes the provider full-reset helper when it is available.
    /// PT: Invoca o helper de reset completo do provedor quando ele esta disponivel.
    /// </summary>
    public void RunResetAllVolatileData()
    {
        if (!TryInvokeIfExists(Connection, "ResetAllVolatileData"))
        {
            TryInvokeIfExists(Connection, "ResetVolatileData");
        }

        GC.KeepAlive(Connection.State);
    }

    /// <summary>
    /// EN: Closes and reopens the shared connection for the lifecycle benchmark.
    /// PT: Fecha e reabre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    public void RunConnectionReopenAfterClose()
    {
        Connection.Close();
        Connection.Open();
        GC.KeepAlive(Connection.State);
    }
}
