namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes debug-trace benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de rastreamento de debug e valida os diagnosticos observados do provedor.
/// </summary>
public class DebugTraceJsonServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Serializes a JSON payload used by the debug-trace benchmark.
    /// PT: Serializa um payload JSON usado pelo benchmark de debug trace.
    /// </summary>
    public Task<object?> RunTestAsync(params object[] args)
    {
        var json = RunDebugTraceJson(
            Repo.Dialect.DisplayName,
            "TestTools",
            new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        GC.KeepAlive(json);
        return Task.FromResult<object?>(json);
    }

    /// <summary>
    /// EN: Serializes the debug-trace payload without requiring a database connection.
    /// PT: Serializa o payload de debug trace sem exigir uma conexao de banco de dados.
    /// </summary>
    public static string RunDebugTraceJson(
        string providerDisplayName,
        string engineName,
        DateTime? timestamp = null)
    {
        var payload = new Dictionary<string, object?>
        {
            ["provider"] = providerDisplayName,
            ["engine"] = engineName,
            ["timestamp"] = timestamp ?? DateTime.UtcNow
        };

        return System.Text.Json.JsonSerializer.Serialize(payload);
    }
}
