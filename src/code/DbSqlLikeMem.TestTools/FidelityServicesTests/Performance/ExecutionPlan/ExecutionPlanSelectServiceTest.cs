namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanSelectServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the select execution-plan benchmark alias.
    /// PT: Executa o alias do benchmark de plano de execucao para select.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        using (var command = Repo.Cnn.CreateCommand())
        {
            command.CommandText = Repo.Dialect.SelectUserNameById(Context, 1);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
            }
        }

        var plan = TryReadDiagnosticValue(Repo.Cnn, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }
}
