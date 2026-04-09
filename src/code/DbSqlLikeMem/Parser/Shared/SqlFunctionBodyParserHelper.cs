namespace DbSqlLikeMem;

internal static class SqlFunctionBodyParserHelper
{
    internal static string ParseFunctionReturnTypeSql(
        this SqlQueryParserContext ctx,
        params string[] stopWords)
    {
        var returnTypeTokens = new List<SqlToken>();
        while (!ctx.IsEnd() && !stopWords.Any(ctx.IsWord))
            returnTypeTokens.Add(ctx.Consume());

        var returnTypeSql = ctx.TokensToSql(returnTypeTokens).Trim();
        if (string.IsNullOrWhiteSpace(returnTypeSql))
            throw new InvalidOperationException("CREATE FUNCTION requires a scalar return type.");

        if (returnTypeSql.Equals(SqlConst.TABLE, StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only scalar return types in the mock.");

        return returnTypeSql;
    }

    internal static SqlExpr ParseFunctionReturnBody(
        this SqlQueryParserContext ctx,
        bool allowBeginEndBlock,
        bool requireBeginEndBlock)
    {
        var hasBeginEndBlock = false;
        if (allowBeginEndBlock && ctx.IsWord(SqlConst.BEGIN))
        {
            ctx.Consume();
            hasBeginEndBlock = true;
        }
        else if (requireBeginEndBlock)
        {
            throw new InvalidOperationException("CREATE FUNCTION body requires BEGIN ... END in this syntax subset.");
        }

        if (!ctx.IsWord(SqlConst.RETURN))
            throw new InvalidOperationException("CREATE FUNCTION requires RETURN in the body.");

        ctx.Consume();

        var bodyTokens = new List<SqlToken>();
        while (!ctx.IsEnd())
        {
            if (hasBeginEndBlock && ctx.IsWord(SqlConst.END))
                break;

            if (ctx.Peek().Kind == SqlTokenKind.Symbol && ctx.Peek().Text == ";")
            {
                if (hasBeginEndBlock)
                {
                    ctx.Consume();
                    break;
                }

                break;
            }

            bodyTokens.Add(ctx.Consume());
        }

        var bodySql = ctx.TokensToSql(bodyTokens).Trim();
        if (string.IsNullOrWhiteSpace(bodySql))
            throw new InvalidOperationException("CREATE FUNCTION requires a scalar expression after RETURN.");

        if (hasBeginEndBlock && !SqlQueryParserContext.IsWord(ctx.Consume(), SqlConst.END))
            throw new InvalidOperationException("CREATE FUNCTION body requires END.");

        return ctx.ParseScalar(bodySql);
    }

    internal static string ParseQuotedFunctionBodySql(
        this SqlQueryParserContext ctx)
    {
        var token = ctx.Consume();
        if (token.Kind != SqlTokenKind.String)
            throw new InvalidOperationException("CREATE FUNCTION requires a quoted SQL body after AS in this syntax subset.");

        return token.Text;
    }

    internal static SqlExpr ParsePostgreSqlSqlFunctionBody(
        this SqlQueryParserContext ctx,
        string bodySql)
    {
        var trimmed = bodySql.Trim();
        if (trimmed.EndsWith(";", StringComparison.Ordinal))
            trimmed = trimmed[..^1].TrimEnd();

        if (!trimmed.StartsWith(SqlConst.SELECT, StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only PostgreSQL LANGUAGE SQL bodies with a single SELECT <expr> statement in the mock.");

        trimmed = trimmed[6..].TrimStart();
        if (string.IsNullOrWhiteSpace(trimmed))
            throw new InvalidOperationException("CREATE FUNCTION PostgreSQL body requires a scalar expression after SELECT.");

        return ctx.ParseScalar(trimmed);
    }

    internal static SqlCreateFunctionQuery BuildCreateFunctionQuery(
        this SqlQueryParserContext ctx,
        SqlTableSource function,
        IReadOnlyList<DbFunctionParameterDef> parameters,
        string returnTypeSql,
        SqlExpr body,
        bool orReplace,
        string statementEnd)
    {
        ctx.EnsureStatementEnd(statementEnd);
        if (function.Name is null)
            throw new InvalidOperationException("CREATE FUNCTION requires a function name.");

        return new SqlCreateFunctionQuery
        {
            Table = function,
            OrReplace = orReplace,
            Definition = DbFunctionDef.CreateUserDefined(function.Name, returnTypeSql, parameters, body)
        };
    }
}
