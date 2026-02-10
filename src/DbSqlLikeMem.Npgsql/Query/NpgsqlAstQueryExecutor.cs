namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// Auto-generated summary.
/// </summary>
public static class NpgsqlAstQueryExecutorRegister
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(NpgsqlDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
            NpgsqlDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new NpgsqlAstQueryExecutor((NpgsqlConnectionMock)cnn, pars));
    }
}

/// <summary>
/// Executor do PostgreSQL (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class NpgsqlAstQueryExecutor(
    NpgsqlConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    protected override SqlExpr MapJsonAccess(JsonAccessExpr ja)
    {
        var pathExpr = ja.Path;
        if (pathExpr is LiteralExpr lit && lit.Value is string s)
        {
            var converted = ConvertPostgresJsonPath(s);
            if (converted is not null)
                pathExpr = new LiteralExpr(converted);
        }

        var normalized = new JsonAccessExpr(ja.Target, pathExpr, ja.Unquote);
        return base.MapJsonAccess(normalized);
    }

    private static string? ConvertPostgresJsonPath(string raw)
    {
        var trimmed = raw.Trim();
        if (!trimmed.StartsWith("{", StringComparison.Ordinal) || !trimmed.EndsWith("}", StringComparison.Ordinal))
            return null;

        var inner = trimmed[1..^1];
        if (string.IsNullOrWhiteSpace(inner))
            return null;

        var parts = inner
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.Trim('"'))
            .Where(p => p.Length > 0)
            .ToArray();

        return parts.Length == 0 ? null : "$." + string.Join('.', parts);
    }
}
