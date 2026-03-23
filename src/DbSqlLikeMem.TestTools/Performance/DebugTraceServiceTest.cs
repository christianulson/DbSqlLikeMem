namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes debug-trace benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de rastreamento de debug e valida os diagnosticos observados do provedor.
/// </summary>
public class DebugTraceServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : PerformanceServiceBase<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Executes a select and reads the provider debug SQL trace when available.
    /// PT: Executa um select e lê o rastreamento SQL de debug do provedor quando disponivel.
    /// </summary>
    public object? RunDebugTraceSelect(params object[] pars)
    {
        var users = (string)pars[0];
        _ = ExecuteScalar(Dialect.SelectUserNameById(users, 1));
        var trace = TryReadDiagnosticValue(Connection, "DebugSql") ?? Dialect.SelectUserNameById(users, 1);
        GC.KeepAlive(trace);
        return trace;
    }

    /// <summary>
    /// EN: Executes a batch and reads the provider debug SQL batch trace when available.
    /// PT: Executa um lote e lê o rastreamento do lote SQL de debug do provedor quando disponivel.
    /// </summary>
    public object? RunDebugTraceBatch(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"));
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"));
        var trace = TryReadDiagnosticValue(Connection, "DebugSqlBatch") ?? (Dialect.InsertUser(users, 1, "Alice") + ";" + Dialect.InsertUser(users, 2, "Bob"));
        GC.KeepAlive(trace);
        return trace;
    }

    /// <summary>
    /// EN: Serializes a JSON payload used by the debug-trace benchmark.
    /// PT: Serializa um payload JSON usado pelo benchmark de debug trace.
    /// </summary>
    public string RunDebugTraceJson()
    {
        var json = RunDebugTraceJson(
            Dialect.DisplayName,
            "TestTools",
            new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        GC.KeepAlive(json);
        return json;
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
