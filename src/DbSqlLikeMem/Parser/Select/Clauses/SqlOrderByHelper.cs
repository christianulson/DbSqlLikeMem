namespace DbSqlLikeMem;

internal static class SqlOrderByHelper
{
    internal static List<SqlOrderByItem> TryParseOrderBy(
        SqlQueryParserContext ctx,
        Func<bool, IReadOnlyList<string>> readRawItems)
    {
        var list = new List<SqlOrderByItem>();
        if (!ctx.IsWord( SqlConst.ORDER))
            return list;

        ctx.Consume();
        if (!ctx.IsWord( SqlConst.BY))
            throw new InvalidOperationException("ORDER requires BY.");

        ctx.Consume();

        var raws = readRawItems(ctx.AllowInsertSelectSuffixBoundary);
        foreach (var r in raws)
        {
            var raw = r.Trim();
            bool? nullsFirst = null;

            if (raw.EndsWith(" NULLS FIRST", StringComparison.OrdinalIgnoreCase))
            {
                if (!ctx.Dialect.SupportsOrderByNullsModifier)
                    throw ctx.NotSupported("ORDER BY ... NULLS FIRST");
                nullsFirst = true;
                raw = raw[..^12].Trim();
            }
            else if (raw.EndsWith(" NULLS LAST", StringComparison.OrdinalIgnoreCase))
            {
                if (!ctx.Dialect.SupportsOrderByNullsModifier)
                    throw ctx.NotSupported("ORDER BY ... NULLS LAST");
                nullsFirst = false;
                raw = raw[..^11].Trim();
            }

            var desc = raw.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
            var val = desc
                ? raw[..^5].Trim()
                : (raw.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase)
                    ? raw[..^4].Trim()
                    : raw);
            list.Add(new SqlOrderByItem(val, desc, nullsFirst));
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
            var raw = r.Trim();
            bool? nullsFirst = null;

            if (raw.EndsWith(" NULLS FIRST", StringComparison.OrdinalIgnoreCase))
            {
                if (!dialect.SupportsOrderByNullsModifier)
                    throw SqlUnsupported.NotSupported(dialect, "ORDER BY ... NULLS FIRST");
                nullsFirst = true;
                raw = raw[..^12].Trim();
            }
            else if (raw.EndsWith(" NULLS LAST", StringComparison.OrdinalIgnoreCase))
            {
                if (!dialect.SupportsOrderByNullsModifier)
                    throw SqlUnsupported.NotSupported(dialect, "ORDER BY ... NULLS LAST");
                nullsFirst = false;
                raw = raw[..^11].Trim();
            }

            var desc = raw.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
            var val = desc
                ? raw[..^5].Trim()
                : (raw.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase)
                    ? raw[..^4].Trim()
                    : raw);
            list.Add(new SqlOrderByItem(val, desc, nullsFirst));
        }

        return list;
    }
}
