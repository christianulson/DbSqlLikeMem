namespace DbSqlLikeMem;

internal static class SqlForJsonClauseHelper
{
    internal static SqlForJsonClause? TryParseForJsonClause(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.FOR) || !ctx.IsWord(1, "JSON"))
            return null;

        if (!ctx.Dialect.SupportsForJsonClause)
            throw SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.FOR_JSON);

        ctx.Consume(); // FOR
        ctx.Consume(); // JSON

        SqlForJsonMode mode;
        if (ctx.IsWord(SqlConst.PATH))
        {
            mode = SqlForJsonMode.Path;
            ctx.Consume();
        }
        else if (ctx.IsWord(SqlConst.AUTO))
        {
            mode = SqlForJsonMode.Auto;
            ctx.Consume();
        }
        else
        {
            throw new InvalidOperationException("FOR JSON requires PATH or AUTO mode.");
        }

        string? rootName = null;
        var includeNullValues = false;
        var withoutArrayWrapper = false;

        while (ctx.IsSymbol(","))
        {
            ctx.Consume();

            if (ctx.IsWord(SqlConst.ROOT))
            {
                if (rootName is not null)
                    throw new InvalidOperationException("FOR JSON ROOT option cannot be specified more than once.");

                ctx.Consume();
                if (!ctx.IsSymbol("("))
                    throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

                var rootArgRaw = ctx.ReadBalancedParenRawTokens().Trim();
                rootName = ParseForJsonRootName(rootArgRaw);
                continue;
            }

            if (ctx.IsWord(SqlConst.INCLUDE_NULL_VALUES))
            {
                if (includeNullValues)
                    throw new InvalidOperationException("FOR JSON INCLUDE_NULL_VALUES option cannot be specified more than once.");

                includeNullValues = true;
                ctx.Consume();
                continue;
            }

            if (ctx.IsWord(SqlConst.WITHOUT_ARRAY_WRAPPER))
            {
                if (withoutArrayWrapper)
                    throw new InvalidOperationException("FOR JSON WITHOUT_ARRAY_WRAPPER option cannot be specified more than once.");

                withoutArrayWrapper = true;
                ctx.Consume();
                continue;
            }

            throw new InvalidOperationException($"FOR JSON option '{ctx.Peek().Text}' is not supported in the mock.");
        }

        return new SqlForJsonClause(mode, rootName, includeNullValues, withoutArrayWrapper);
    }

    internal static SqlForJsonClause? TryParseForJsonClause(
        SqlQueryParserContext ctx,
        Func<string> readBalancedParenRawTokens)
    {
        if (!ctx.IsWord(SqlConst.FOR) || !ctx.IsWord(1, "JSON"))
            return null;

        if (!ctx.Dialect.SupportsForJsonClause)
            throw SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.FOR_JSON);

        ctx.Consume(); // FOR
        ctx.Consume(); // JSON

        SqlForJsonMode mode;
        if (ctx.IsWord(SqlConst.PATH))
        {
            mode = SqlForJsonMode.Path;
            ctx.Consume();
        }
        else if (ctx.IsWord(SqlConst.AUTO))
        {
            mode = SqlForJsonMode.Auto;
            ctx.Consume();
        }
        else
        {
            throw new InvalidOperationException("FOR JSON requires PATH or AUTO mode.");
        }

        string? rootName = null;
        var includeNullValues = false;
        var withoutArrayWrapper = false;

        while (ctx.IsSymbol(","))
        {
            ctx.Consume();

            if (ctx.IsWord(SqlConst.ROOT))
            {
                if (rootName is not null)
                    throw new InvalidOperationException("FOR JSON ROOT option cannot be specified more than once.");

                ctx.Consume();
                if (!ctx.IsSymbol("("))
                    throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

                var rootArgRaw = readBalancedParenRawTokens().Trim();
                rootName = ParseForJsonRootName(rootArgRaw);
                continue;
            }

            if (ctx.IsWord(SqlConst.INCLUDE_NULL_VALUES))
            {
                if (includeNullValues)
                    throw new InvalidOperationException("FOR JSON INCLUDE_NULL_VALUES option cannot be specified more than once.");

                includeNullValues = true;
                ctx.Consume();
                continue;
            }

            if (ctx.IsWord(SqlConst.WITHOUT_ARRAY_WRAPPER))
            {
                if (withoutArrayWrapper)
                    throw new InvalidOperationException("FOR JSON WITHOUT_ARRAY_WRAPPER option cannot be specified more than once.");

                withoutArrayWrapper = true;
                ctx.Consume();
                continue;
            }

            throw new InvalidOperationException($"FOR JSON option '{ctx.Peek().Text}' is not supported in the mock.");
        }

        return new SqlForJsonClause(mode, rootName, includeNullValues, withoutArrayWrapper);
    }

    private static string ParseForJsonRootName(string raw)
    {
        var trimmed = raw.Trim();
        if (trimmed.Length == 0)
            throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

        if (!Regex.IsMatch(trimmed, @"^N?'(?:''|[^'])*'$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

        return SqlOpenJsonHelper.UnquoteSqlStringLiteral(trimmed);
    }
}
