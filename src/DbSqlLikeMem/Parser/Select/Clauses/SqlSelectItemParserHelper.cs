namespace DbSqlLikeMem;

internal static class SqlSelectItemParserHelper
{
    internal static List<SqlSelectItem> ParseSelectItemsWithValidation(
        IReadOnlyList<string> raws,
        DbMock db,
        ISqlDialect dialect,
        Func<string, bool>? customFunctionSupported = null)
    {
        return [.. raws.Select(raw =>
        {
            // Fail fast on known-invalid patterns before any splitting/normalization.
            // Example: COUNT(DISTINCT DISTINCT id)
            if (Regex.IsMatch(
                    raw,
                    @"\bDISTINCT\s+DISTINCT\b",
                    RegexOptions.IgnoreCase))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            var (expr, alias) = SqlAliasParserHelper.SplitTrailingAsAliasTopLevel(raw, dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException("Empty SELECT item.");

            // Fail fast: duplicated DISTINCT inside function calls like COUNT(DISTINCT DISTINCT id)
            // (the expression parser also checks, but this guard prevents corpus regressions when
            // select-item splitting/reconstruction changes token boundaries).
            if (Regex.IsMatch(
                    expr,
                    @"\bDISTINCT\s+DISTINCT\b",
                    RegexOptions.IgnoreCase))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            // Validate select item expressions. This is what makes corpus tests catch
            // typos like "SELEC" inside subqueries, invalid EXISTS(), duplicated DISTINCT, etc.
            _ = SqlExpressionParser.ParseScalar(expr, db, dialect, null, customFunctionSupported);
            return new SqlSelectItem(expr, alias);
        })];
    }
}
