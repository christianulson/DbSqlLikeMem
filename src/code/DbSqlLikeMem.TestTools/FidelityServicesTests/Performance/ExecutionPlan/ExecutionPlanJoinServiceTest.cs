namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT-br: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanJoinServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a join and reads the provider execution plan diagnostic.
    /// PT-br: Executa uma junção e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the join query. PT-br: Id principal opcional do usuario para a consulta de join.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;

        _ = await Repo.ExecuteScalarAsync(Repo.Dialect.CountJoinForUser(Context, userId));
        var plan = TryReadDiagnosticValue(Repo.Cnn, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }
}
