namespace DbSqlLikeMem;

internal static class SqlSelectItemParserHelper
{
    private static readonly Regex _duplicateDistinct = new(@"\bDISTINCT\s+DISTINCT\b", RegexOptions.IgnoreCase);

    internal static List<SqlSelectItem> ParseSelectItemsWithValidation(
        IReadOnlyList<string> raws,
        DbMock db,
        ISqlDialect dialect,
        Func<string, bool>? customFunctionSupported = null)
    {
        return [.. raws.Select(raw =>
        {
            var normalizedRaw = raw.AsSpan().Trim().ToString();

            // Fail fast on known-invalid patterns before any splitting/normalization.
            // Example: COUNT(DISTINCT DISTINCT id)
            if (_duplicateDistinct.IsMatch(normalizedRaw))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            var (expr, alias) = SqlAliasParserHelper.SplitTrailingAsAliasTopLevel(normalizedRaw.AsSpan(), dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException("Empty SELECT item.");

            // Fail fast: duplicated DISTINCT inside function calls like COUNT(DISTINCT DISTINCT id)
            // (the expression parser also checks, but this guard prevents corpus regressions when
            // select-item splitting/reconstruction changes token boundaries).
            if (_duplicateDistinct.IsMatch(expr))
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
