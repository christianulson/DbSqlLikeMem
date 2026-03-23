namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Registers the Npgsql AST executor with the shared factory.
/// PT: Registra o executor AST de Npgsql na factory compartilhada.
/// </summary>
public static class NpgsqlAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the Npgsql AST executor for the PostgreSQL dialect.
    /// PT: Registra o executor AST de Npgsql para o dialeto PostgreSQL.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            NpgsqlDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new NpgsqlAstQueryExecutor((NpgsqlConnectionMock)cnn, pars));
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
    /// EN: Maps PostgreSQL JSON path access into the executor-specific JSON access function.
    /// PT: Mapeia o acesso a caminho JSON do PostgreSQL para a funcao de acesso JSON especifica do executor.
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

        return new FunctionCallExpr(
            ja.Unquote ? "__JSON_ACCESS_TEXT" : "__JSON_ACCESS_JSON",
            [ja.Target, pathExpr]);
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
            .Split(',').Select(_ => _.Trim()).Where(_ => !string.IsNullOrWhiteSpace(_))
            .Select(p => p.Trim('"'))
            .Where(p => p.Length > 0)
            .ToArray();

        return parts.Length == 0 ? null : "$." + string.Join(".", parts);
    }
}
