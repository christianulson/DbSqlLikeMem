namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the connection reopen workflow for the lifecycle benchmark and validates the observed provider behavior.
/// PT: Executa o fluxo de reabertura da conexao para o benchmark de ciclo de vida e valida o comportamento observado do provedor.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class ConnectionLifecycleOpenServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Opens the shared connection for the lifecycle benchmark.
    /// PT: Abre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    public Task<object?> RunTestAsync(params object[] args)
    {
        Repo.Cnn.Open();
        Repo.Cnn.State.Should().Be(ConnectionState.Open);
        GC.KeepAlive(Repo.Cnn.State);
        return Task.FromResult<object?>(1);
    }
}
