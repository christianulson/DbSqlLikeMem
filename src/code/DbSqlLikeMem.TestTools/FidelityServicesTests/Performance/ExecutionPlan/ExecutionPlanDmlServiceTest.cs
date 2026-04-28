namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanDmlServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    private int _nextInsertId = 1;

    /// <summary>
    /// EN: Executes an insert and reads the provider execution plan diagnostic.
    /// PT: Executa um insert e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    /// <param name="args">EN: Optional insert id for the benchmark flow. PT: Id de insert opcional para o fluxo do benchmark.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : _nextInsertId++;
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, "Alice"));
        var plan = TryReadDiagnosticValue(Repo.Cnn, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }
}
