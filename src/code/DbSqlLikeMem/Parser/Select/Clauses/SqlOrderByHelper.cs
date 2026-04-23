namespace DbSqlLikeMem;

internal static class SqlOrderByHelper
{
    internal static List<SqlOrderByItem> TryParseOrderBy(
        SqlQueryParserContext ctx,
        Func<bool, IReadOnlyList<string>> readRawItems)
    {
        var list = new List<SqlOrderByItem>();
        if (!ctx.IsWord(SqlConst.ORDER))
            return list;

        ctx.Consume();
        if (!ctx.IsWord(SqlConst.BY))
            throw new InvalidOperationException("ORDER requires BY.");

        ctx.Consume();

        var raws = readRawItems(ctx.AllowInsertSelectSuffixBoundary);
        foreach (var r in raws)
        {
            list.Add(ParseOrderByItem(r.AsSpan(), ctx.Dialect, ctx.NotSupported));
        }

        return list;
    }

    internal static List<SqlOrderByItem> TryParseOrderBy(
        ISqlDialect dialect,
        Func<SqlToken> peek,
        Action consume,
        Func<SqlToken, string, bool> isWord,
        Func<SqlToken, string, bool> isSymbol,
        Func<bool, IReadOnlyList<string>> readRawItems,
        bool allowInsertSelectSuffixBoundary)
    {
        var list = new List<SqlOrderByItem>();
        if (!isWord(peek(), SqlConst.ORDER))
            return list;

        consume();
        if (!isWord(peek(), SqlConst.BY))
            throw new InvalidOperationException("ORDER requires BY.");

        consume();

        var raws = readRawItems(allowInsertSelectSuffixBoundary);
        foreach (var r in raws)
        {
            list.Add(ParseOrderByItem(r.AsSpan(), dialect, message => SqlUnsupported.NotSupported(dialect, message)));
        }

        return list;
    }

    private static SqlOrderByItem ParseOrderByItem(
        ReadOnlySpan<char> raw,
        ISqlDialect dialect,
        Func<string, Exception> notSupported)
    {
        var normalized = raw.Trim();
        bool? nullsFirst = null;

        if (normalized.EndsWith(" NULLS FIRST", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsOrderByNullsModifier)
                throw notSupported("ORDER BY ... NULLS FIRST");
            nullsFirst = true;
            normalized = TrimSuffix(normalized, 12);
        }
        else if (normalized.EndsWith(" NULLS LAST", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsOrderByNullsModifier)
                throw notSupported("ORDER BY ... NULLS LAST");
            nullsFirst = false;
            normalized = TrimSuffix(normalized, 11);
        }

        var desc = normalized.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
        var value = desc
            ? TrimSuffix(normalized, 5)
            : normalized.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase)
                ? TrimSuffix(normalized, 4)
                : normalized;

        return new SqlOrderByItem(value.ToString(), desc, nullsFirst);
    }

    private static ReadOnlySpan<char> TrimSuffix(ReadOnlySpan<char> value, int suffixLength)
        => suffixLength <= 0 || suffixLength > value.Length ? value : value[..^suffixLength].Trim();
}
