namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT-br: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanSelectServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the select execution-plan benchmark alias.
    /// PT-br: Executa o alias do benchmark de plano de execucao para select.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the query. PT-br: Id principal opcional do usuario para a consulta.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;

        using (var command = Repo.Cnn.CreateCommand())
        {
            command.CommandText = Repo.Dialect.SelectUserNameById(Context, userId);
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
