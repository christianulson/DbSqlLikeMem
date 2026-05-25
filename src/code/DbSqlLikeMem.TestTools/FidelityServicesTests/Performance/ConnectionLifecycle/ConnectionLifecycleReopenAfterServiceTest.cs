namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the connection reopen after close workflow for the lifecycle benchmark and validates the observed provider behavior.
/// PT-br: Executa o fluxo de reabertura da conexao apos fechamento para o benchmark de ciclo de vida e valida o comportamento observado do provedor.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class ConnectionLifecycleReopenAfterServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Closes and reopens the shared connection for the lifecycle benchmark.
    /// PT-br: Fecha e reabre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    public Task<object?> RunTestAsync(params object[] args)
        => Task.FromResult<object?>(RunConnectionReopenAfterClose());

    /// <summary>
    /// EN: Closes and reopens the shared connection for the lifecycle benchmark.
    /// PT-br: Fecha e reabre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    public int RunConnectionReopenAfterClose()
    {
        Repo.Cnn.Close();
        Repo.Cnn.Open();
        GC.KeepAlive(Repo.Cnn.State);
        return 1;
    }
}
