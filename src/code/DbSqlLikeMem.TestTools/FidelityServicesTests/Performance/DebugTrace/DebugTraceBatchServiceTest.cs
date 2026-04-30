namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes debug-trace benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de rastreamento de debug e valida os diagnosticos observados do provedor.
/// </summary>
public class DebugTraceBatchServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a batch and reads the provider debug SQL batch trace when available.
    /// PT: Executa um lote e lê o rastreamento do lote SQL de debug do provedor quando disponivel.
    /// </summary>
    /// <param name="args">EN: Optional insert ids for the batch rows. PT: IDs opcionais de insert para as linhas do lote.</param>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunDebugTraceBatchAsync(args);

    /// <summary>
    /// EN: Executes a batch and reads the provider debug SQL batch trace when available.
    /// PT: Executa um lote e lê o rastreamento do lote SQL de debug do provedor quando disponivel.
    /// </summary>
    /// <param name="args">EN: Optional insert ids for the batch rows. PT: IDs opcionais de insert para as linhas do lote.</param>
    public async Task<string> RunDebugTraceBatchAsync(params object[] args)
    {
        var id1 = args.Length > 0 ? (int)args[0] : 1;
        var id2 = args.Length > 1 ? (int)args[1] : 2;
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id1, "Alice"));
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id2, "Bob"));
            var trace = Convert.ToString(TryReadDiagnosticValue(Repo.Cnn, "DebugSqlBatch")) ?? (Repo.Dialect.InsertUser(Context, id1, "Alice") + ";" + Repo.Dialect.InsertUser(Context, id2, "Bob"));
            GC.KeepAlive(trace);
            return trace;
        }
        finally
        {
            try
            {
                await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, id2));
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, id1));
            }
            catch
        {
            // Ignore cleanup failures during benchmark teardown.
        }
    }
}
}
