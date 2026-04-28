namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes debug-trace benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de rastreamento de debug e valida os diagnosticos observados do provedor.
/// </summary>
public class DebugTraceSelectServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a select and reads the provider debug SQL trace when available.
    /// PT: Executa um select e lê o rastreamento SQL de debug do provedor quando disponivel.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the query. PT: Id principal opcional do usuario para a consulta.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;

        _ = await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, userId));
        var trace = TryReadDiagnosticValue(Repo.Cnn, "DebugSql") ?? Repo.Dialect.SelectUserNameById(Context, userId);
        GC.KeepAlive(trace);
        return trace;
    }
}
