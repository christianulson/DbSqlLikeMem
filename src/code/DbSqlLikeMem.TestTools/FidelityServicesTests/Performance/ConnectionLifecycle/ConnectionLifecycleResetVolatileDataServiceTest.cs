namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the connection reset volatile data workflow for the lifecycle benchmark and validates the observed provider behavior.
/// PT: Executa o fluxo de reset de dados volateis da conexao para o benchmark de ciclo de vida e valida o comportamento observado do provedor.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class ConnectionLifecycleResetVolatileDataServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Invokes the provider reset volatile data helper for the lifecycle benchmark.
    /// PT: Invoca o helper de reset de dados volateis do provedor para o benchmark de ciclo de vida.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunTestAsync(params object[] args)
        => Task.FromResult<object?>(RunResetVolatileData());

    /// <summary>
    /// EN: Invokes the provider reset volatile data helper for the lifecycle benchmark.
    /// PT: Invoca o helper de reset de dados volateis do provedor para o benchmark de ciclo de vida.
    /// </summary>
    public int RunResetVolatileData()
    {
        if (Repo.Cnn.State == ConnectionState.Open)
            Repo.Cnn.Close();

        Repo.Cnn.Open();
        TryInvokeIfExists(Repo.Cnn, "ResetVolatileData");
        Repo.Cnn.State.Should().Be(ConnectionState.Open);
        GC.KeepAlive(Repo.Cnn.State);
        return 1;
    }
}
