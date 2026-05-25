namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Registers the Npgsql AST executor with the shared factory.
/// PT-br: Registra o executor AST de Npgsql na factory compartilhada.
/// </summary>
public static class NpgsqlAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the Npgsql AST executor for the PostgreSQL dialect.
    /// PT-br: Registra o executor AST de Npgsql para o dialeto PostgreSQL.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            NpgsqlDialect.DialectName,
            ctx => new NpgsqlAstQueryExecutor(ctx));
}

/// <summary>
/// Executor do PostgreSQL (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class NpgsqlAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
    /// <summary>
    /// EN: Maps PostgreSQL JSON path access into the executor-specific JSON access function.
    /// PT-br: Mapeia o acesso a caminho JSON do PostgreSQL para a funcao de acesso JSON especifica do executor.
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
        if (string.IsNullOrWhiteSpace(trimmed))
            return null;

        if (trimmed.StartsWith("$", StringComparison.Ordinal)
            || trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return trimmed;

        if (trimmed.StartsWith("{", StringComparison.Ordinal) && trimmed.EndsWith("}", StringComparison.Ordinal))
        {
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

        if (int.TryParse(trimmed, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var index)
            && index >= 0)
            return $"$[{index}]";

        if (IsSimpleJsonPropertyName(trimmed))
            return "$." + trimmed;

        return "$.\"" + trimmed.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    }

    private static bool IsSimpleJsonPropertyName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        if (!(char.IsLetter(value[0]) || value[0] == '_'))
            return false;

        for (var i = 1; i < value.Length; i++)
        {
            var ch = value[i];
            if (!(char.IsLetterOrDigit(ch) || ch == '_'))
                return false;
        }

        return true;
    }
}
