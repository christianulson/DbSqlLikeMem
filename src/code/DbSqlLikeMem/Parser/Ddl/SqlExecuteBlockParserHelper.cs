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
            var text = token.Kind switch
            {
                SqlTokenKind.String => $"'{EscapeStringLiteral(token.Text)}'",
                _ => token.Text
            };

            if (sb.Length > 0 && NeedsSpace(prev, token))
                sb.Append(' ');

            sb.Append(text);
            prev = token;
        }

        return sb.ToString().Trim();

        static string EscapeStringLiteral(string value)
            => value.Replace("'", "''");

        static bool IsWordLike(SqlToken tok)
            => tok.Kind is SqlTokenKind.Identifier
            or SqlTokenKind.Keyword
            or SqlTokenKind.Number
            or SqlTokenKind.Parameter
            or SqlTokenKind.String;

        static bool NeedsSpace(SqlToken? p, SqlToken c)
        {
            if (p is null)
                return false;

            if (c.Kind == SqlTokenKind.Symbol && (c.Text is "." or ")" or "," or ";"))
                return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is "." or "("))
                return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is ")" or ","))
                return IsWordLike(c) || c.Kind == SqlTokenKind.Number || c.Kind == SqlTokenKind.String;
            if (p.Value.Kind == SqlTokenKind.Symbol && p.Value.Text == ";")
                return false;
            if (IsWordLike(p.Value) && IsWordLike(c))
                return true;
            if ((p.Value.Kind == SqlTokenKind.Operator && c.Kind != SqlTokenKind.Symbol)
                || (c.Kind == SqlTokenKind.Operator && p.Value.Kind != SqlTokenKind.Symbol))
                return true;

            return true;
        }
    }
}
