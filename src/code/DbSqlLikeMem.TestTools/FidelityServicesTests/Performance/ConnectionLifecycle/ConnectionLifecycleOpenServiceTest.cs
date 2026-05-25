namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the connection reopen workflow for the lifecycle benchmark and validates the observed provider behavior.
/// PT-br: Executa o fluxo de reabertura da conexao para o benchmark de ciclo de vida e valida o comportamento observado do provedor.
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
    /// PT-br: Abre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT-br: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunTestAsync(params object[] args)
        => Task.FromResult<object?>(RunConnectionOpen());

    /// <summary>
    /// EN: Opens the shared connection for the lifecycle benchmark.
    /// PT-br: Abre a conexao compartilhada para o benchmark de ciclo de vida.
    /// </summary>
    public int RunConnectionOpen()
    {
        if (Repo.Cnn.State == ConnectionState.Open)
            Repo.Cnn.Close();

        Repo.Cnn.Open();
        Repo.Cnn.State.Should().Be(ConnectionState.Open);
        GC.KeepAlive(Repo.Cnn.State);
        return 1;
    }
}
