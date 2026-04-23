namespace DbSqlLikeMem;

internal static class SqlPivotHelper
{
    internal static SqlTableSource TryParsePivot(
        this SqlQueryParserContext ctx,
        SqlTableSource source)
    {
        if (!IsWord(ctx.Peek(), SqlConst.PIVOT))
            return source;

        if (!ctx.Dialect.SupportsPivotClause)
            throw SqlUnsupported.NotSupported(ctx.Dialect, SqlConst.PIVOT);

        ctx.Consume();
        var raw = ctx.ReadBalancedParenRawTokens();
        var spec = ParsePivotSpec(raw);

        var pivotAlias = ctx.ReadOptionalAlias();
        return source with
        {
            Alias = pivotAlias ?? source.Alias,
            Pivot = spec
        };
    }

    internal static SqlTableSource TryParsePivot(
        SqlTableSource source,
        ISqlDialect dialect,
        Func<SqlToken> peek,
        Action consume,
        Func<string> readBalancedParenRawTokens,
        Func<string?> readOptionalAlias)
    {
        if (!IsWord(peek(), SqlConst.PIVOT))
            return source;

        if (!dialect.SupportsPivotClause)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.PIVOT);

        consume();
        var raw = readBalancedParenRawTokens();
        var spec = ParsePivotSpec(raw);

        var pivotAlias = readOptionalAlias();
        return source with
        {
            Alias = pivotAlias ?? source.Alias,
            Pivot = spec
        };
    }

    internal static SqlTableSource TryParseUnpivot(
        this SqlQueryParserContext ctx,
        SqlTableSource source)
    {
        if (!IsWord(ctx.Peek(), SqlConst.UNPIVOT))
            return source;

        if (!ctx.Dialect.SupportsUnpivotClause)
            throw SqlUnsupported.NotSupported(ctx.Dialect, SqlConst.UNPIVOT);

        ctx.Consume();
        var raw = ctx.ReadBalancedParenRawTokens();
        var spec = ParseUnpivotSpec(raw);

        var unpivotAlias = ctx.ReadOptionalAlias();
        return source with
        {
            Alias = unpivotAlias ?? source.Alias,
            Unpivot = spec
        };
    }

    internal static SqlTableSource TryParseUnpivot(
        SqlTableSource source,
        ISqlDialect dialect,
        Func<SqlToken> peek,
        Action consume,
        Func<string> readBalancedParenRawTokens,
        Func<string?> readOptionalAlias)
    {
        if (!IsWord(peek(), SqlConst.UNPIVOT))
            return source;

        if (!dialect.SupportsUnpivotClause)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.UNPIVOT);

        consume();
        var raw = readBalancedParenRawTokens();
        var spec = ParseUnpivotSpec(raw);

        var unpivotAlias = readOptionalAlias();
        return source with
        {
            Alias = unpivotAlias ?? source.Alias,
            Unpivot = spec
        };
    }

    private static SqlPivotSpec ParsePivotSpec(string raw)
    {
        var m = Regex.Match(
            raw,
            @"^\s*(?<agg>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<arg>[^\)]+?)\s*\)\s+FOR\s+(?<for>[A-Za-z_][A-Za-z0-9_\.]*)\s+IN\s*\((?<in>.+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!m.Success)
            throw new InvalidOperationException("invalid: unsupported PIVOT syntax");

        var aggregateFunction = m.Groups["agg"].Value.Trim();
        var aggregateArgRaw = m.Groups["arg"].Value.Trim();
        var forColumnRaw = m.Groups["for"].Value.Trim();
        var inListRaw = m.Groups["in"].Value.Trim();

        var inItems = new List<SqlPivotInItem>();
        foreach (var itemRaw in SplitPivotInItems(inListRaw.AsSpan()))
        {
            var item = itemRaw.Trim();
            if (item.Length == 0)
                continue;

            var im = Regex.Match(item, @"^(?<val>.+?)(?:\s+AS\s+(?<alias>[A-Za-z_][A-Za-z0-9_]*))?$", RegexOptions.IgnoreCase);
            if (!im.Success)
                throw new InvalidOperationException("invalid: unsupported PIVOT IN item");

            var valueRaw = im.Groups["val"].Value.Trim();
            var alias = im.Groups["alias"].Success
                ? im.Groups["alias"].Value.Trim()
                : valueRaw.Trim('\'', '"').Replace('.', '_');

            if (string.IsNullOrWhiteSpace(alias))
                throw new InvalidOperationException("invalid: PIVOT IN item alias");

            inItems.Add(new SqlPivotInItem(valueRaw, alias));
        }

        if (inItems.Count == 0)
            throw new InvalidOperationException("invalid: PIVOT IN list is empty");

        return new SqlPivotSpec(aggregateFunction, aggregateArgRaw, forColumnRaw, inItems);
    }

    private static SqlUnpivotSpec ParseUnpivotSpec(string raw)
    {
        const string identifierPattern = @"(?:\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)";

        var match = Regex.Match(
            raw,
            $@"^\s*(?<value>{identifierPattern})\s+FOR\s+(?<name>{identifierPattern})\s+IN\s*\((?<in>.+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

        if (!match.Success)
            throw new InvalidOperationException("invalid: unsupported UNPIVOT syntax");

        var valueColumnName = match.Groups["value"].Value.NormalizeName();
        var nameColumnName = match.Groups["name"].Value.NormalizeName();
        var inListRaw = match.Groups["in"].Value.Trim();

        var inItems = new List<SqlUnpivotInItem>();
        foreach (var itemRaw in SplitPivotInItems(inListRaw.AsSpan()))
        {
            var item = itemRaw.Trim();
            if (item.Length == 0)
                continue;

            if (!Regex.IsMatch(item, $"^{identifierPattern}$", RegexOptions.CultureInvariant))
                throw new InvalidOperationException("invalid: unsupported UNPIVOT IN item");

            var normalized = item.NormalizeName();
            inItems.Add(new SqlUnpivotInItem(normalized, normalized));
        }

        if (inItems.Count == 0)
            throw new InvalidOperationException("invalid: UNPIVOT IN list is empty");

        return new SqlUnpivotSpec(valueColumnName, nameColumnName, inItems);
    }

    private static IEnumerable<string> SplitPivotInItems(ReadOnlySpan<char> raw)
    {
        var list = new List<string>();
        var depth = 0;
        var start = 0;

        for (var i = 0; i < raw.Length; i++)
        {
            var ch = raw[i];
            if (ch == '(')
                depth++;
            else if (ch == ')')
                depth--;

            if (ch == ',' && depth == 0)
            {
                list.Add(TrimToString(raw.Slice(start, i - start)));
                start = i + 1;
                continue;
            }
        }

        if (start <= raw.Length)
            list.Add(TrimToString(raw[start..]));

        return list;
    }

    private static string TrimToString(ReadOnlySpan<char> value)
    {
        var start = 0;
        var end = value.Length - 1;

        while (start <= end && char.IsWhiteSpace(value[start]))
            start++;

        while (end >= start && char.IsWhiteSpace(value[end]))
            end--;

        return end < start ? string.Empty : value.Slice(start, end - start + 1).ToString();
    }

    private static bool IsWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);
}
