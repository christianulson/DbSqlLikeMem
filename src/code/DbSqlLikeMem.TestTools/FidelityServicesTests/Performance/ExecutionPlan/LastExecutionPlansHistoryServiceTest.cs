namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class LastExecutionPlansHistoryServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes several statements and reads the provider execution-plan history.
    /// PT: Executa varias instrucoes e lê o historico de planos de execucao do provedor.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1));
        _ = await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName));
        var plans = TryReadDiagnosticValue(Repo.Cnn, "LastExecutionPlans");
        GC.KeepAlive(plans);
        return plans;
    }
}
