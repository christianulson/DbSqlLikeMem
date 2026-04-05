namespace DbSqlLikeMem;

internal static class SqlExecuteBlockParserHelper
{
    internal static SqlExecuteBlockQuery ParseExecuteBlock(
        this SqlQueryParserContext ctx)
    {
        if (!string.Equals(ctx.Dialect.Name, "firebird", StringComparison.OrdinalIgnoreCase))
            throw ctx.NotSupported("EXECUTE BLOCK");

        ctx.Consume(); // EXECUTE
        if (!ctx.IsWord(SqlConst.BLOCK))
            throw new InvalidOperationException("EXECUTE BLOCK requires BLOCK keyword.");

        ctx.Consume(); // BLOCK

        IReadOnlyList<ProcParam> inputParameters = [];
        IReadOnlyList<ProcParam> returnParameters = [];

        if (ctx.IsSymbol("("))
        {
            var rawParameterList = ctx.ReadBalancedParenRawTokens();
            inputParameters = ctx.ParseExecuteBlockParameters(rawParameterList, forceOptional: false);
        }

        if (ctx.IsWord(SqlConst.RETURNS))
        {
            ctx.Consume();
            if (ctx.IsSymbol("("))
            {
                var rawParameterList = ctx.ReadBalancedParenRawTokens();
                returnParameters = ctx.ParseExecuteBlockParameters(rawParameterList, forceOptional: true);
            }
        }

        if (ctx.IsWord(SqlConst.AS))
            ctx.Consume();

        if (!ctx.IsWord(SqlConst.BEGIN))
            throw new InvalidOperationException("EXECUTE BLOCK requires a BEGIN ... END body.");

        var bodySql = ctx.ReadBlockBodyRawSql();
        ctx.EnsureStatementEnd("EXECUTE BLOCK");

        return new SqlExecuteBlockQuery
        {
            InputParameters = inputParameters,
            ReturnParameters = returnParameters,
            BodySql = bodySql
        };
    }

    private static string ReadBlockBodyRawSql(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.BEGIN))
            throw new InvalidOperationException("EXECUTE BLOCK requires BEGIN.");

        var buf = new List<SqlToken>();
        var depth = 0;
        var terminatedByExit = false;

        while (!ctx.IsEnd())
        {
            var token = ctx.Peek();
            ctx.Consume();

            if (SqlQueryParserContext.IsWord(token, SqlConst.BEGIN))
            {
                depth++;
                if (depth > 1)
                    buf.Add(token);
                continue;
            }

            if (SqlQueryParserContext.IsWord(token, SqlConst.END))
            {
                depth--;
                if (depth == 0)
                    break;

                if (depth < 0)
                    throw new InvalidOperationException("EXECUTE BLOCK body has an unexpected END.");

                if (!terminatedByExit)
                    buf.Add(token);
                continue;
            }

            if (SqlQueryParserContext.IsWord(token, SqlConst.EXIT))
            {
                terminatedByExit = true;
                continue;
            }

            if (SqlQueryParserContext.IsWord(token, SqlConst.SUSPEND))
                continue;

            if (depth >= 1 && !terminatedByExit)
                buf.Add(token);
        }

        if (depth != 0)
            throw new InvalidOperationException("EXECUTE BLOCK body was not closed correctly.");

        return TokensToSql(buf);
    }

    private static string TokensToSql(List<SqlToken> tokens)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;
        foreach (var token in tokens)
        {
            if (prev is not null && prev.Value.Kind != SqlTokenKind.Symbol && token.Kind != SqlTokenKind.Symbol)
                sb.Append(' ');

            sb.Append(token.Text);
            prev = token;
        }

        return sb.ToString();
    }
}
