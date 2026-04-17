namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes connection-lifecycle benchmark workflows and validates the observed provider behavior.
/// PT: Executa fluxos de benchmark de ciclo de vida da conexao e valida o comportamento observado do provedor.
/// </summary>
public class ConnectionLifecycleResetAllVolatileDataServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Invokes the provider full-reset helper when it is available.
    /// PT: Invoca o helper de reset completo do provedor quando ele esta disponivel.
    /// </summary>
    public Task<object?> RunTestAsync(params object[] args)
    {
        if (!TryInvokeIfExists(Repo.Cnn, "ResetAllVolatileData"))
            TryInvokeIfExists(Repo.Cnn, "ResetVolatileData");

        Repo.Cnn.State.Should().Be(ConnectionState.Open);
        GC.KeepAlive(Repo.Cnn.State);
        return Task.FromResult<object?>(1);
    }
}
