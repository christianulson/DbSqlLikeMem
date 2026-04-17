namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanJoinServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a join and reads the provider execution plan diagnostic.
    /// PT: Executa uma junção e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = await Repo.ExecuteScalarAsync(Repo.Dialect.CountJoinForUser(Context, 1));
        var plan = TryReadDiagnosticValue(Repo.Cnn, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }
}
