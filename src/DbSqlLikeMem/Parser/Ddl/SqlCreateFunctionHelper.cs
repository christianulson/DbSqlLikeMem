namespace DbSqlLikeMem;

internal static class SqlCreateFunctionHelper
{
    internal static SqlCreateFunctionQuery ParseCreateFunction(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (orReplace && !ctx.Dialect.SupportsCreateOrReplaceFunctionDdl)
            throw ctx.NotSupported("CREATE OR REPLACE FUNCTION");

        if (!ctx.Dialect.SupportsFunctionDdl)
            throw ctx.NotSupported("CREATE FUNCTION");

        if (!ctx.IsWord(SqlConst.FUNCTION))
            throw new InvalidOperationException("CREATE FUNCTION requires FUNCTION keyword.");

        ctx.Consume(); // FUNCTION

        var functionNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(functionNameToken)
            || SqlQueryParserContext.IsSymbol(functionNameToken, ";"))
            throw new InvalidOperationException("CREATE FUNCTION requires a function name.");

        var function = ctx.ParseQualifiedObjectName();
        var rawParameterList = ctx.IsSymbol("(")
            ? ReadBalancedParenRawTokens(ctx).Trim()
            : null;
        var parameters = ctx.ParseFunctionParameters(
            rawParameterList,
            allowMissingParameterList: ctx.Dialect.SupportsOracleCreateFunctionDdl);

        if (ctx.Dialect.SupportsOracleCreateFunctionDdl)
            return ctx.ParseOracleCreateFunction(function, parameters, orReplace);

        if (ctx.Dialect.SupportsPostgreSqlCreateFunctionDdl)
            return ctx.ParsePostgreSqlCreateFunction(function, parameters, orReplace);

        if (ctx.Dialect.SupportsInlineReturnCreateFunctionDdl)
        {
            return ctx.ParseInlineReturnCreateFunction(function, parameters, orReplace);
        }

        return ctx.ParseSqlServerStyleCreateFunction(function, parameters, orReplace);
    }

    private static SqlCreateFunctionQuery ParseSqlServerStyleCreateFunction(
        this SqlQueryParserContext ctx,
        SqlTableSource function,
        IReadOnlyList<DbFunctionParameterDef> parameters,
        bool orReplace)
    {
        ctx.ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ctx.ParseFunctionReturnTypeSql(SqlConst.AS);
        ctx.ExpectWord(SqlConst.AS);
        var body = ctx.ParseFunctionReturnBody(
            allowBeginEndBlock: true,
            requireBeginEndBlock: false);
        return ctx.BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace, "CREATE FUNCTION");
    }

    private static SqlCreateFunctionQuery ParseInlineReturnCreateFunction(
        this SqlQueryParserContext ctx,
        SqlTableSource function,
        IReadOnlyList<DbFunctionParameterDef> parameters,
        bool orReplace)
    {
        ctx.ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ctx.ParseFunctionReturnTypeSql(SqlConst.RETURN, SqlConst.BEGIN);
        var body = ctx.ParseFunctionReturnBody(
            allowBeginEndBlock: true,
            requireBeginEndBlock: false);
        return ctx.BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace, "CREATE FUNCTION");
    }

    private static SqlCreateFunctionQuery ParseOracleCreateFunction(
        this SqlQueryParserContext ctx,
        SqlTableSource function,
        IReadOnlyList<DbFunctionParameterDef> parameters,
        bool orReplace)
    {
        ctx.ExpectWord(SqlConst.RETURN);
        var returnTypeSql = ctx.ParseFunctionReturnTypeSql(SqlConst.IS, SqlConst.AS);

        if (!ctx.IsWord(SqlConst.IS) && !ctx.IsWord(SqlConst.AS))
            throw new InvalidOperationException("CREATE FUNCTION in Oracle syntax requires IS or AS before the body.");

        ctx.Consume();
        var body = ctx.ParseFunctionReturnBody(
            allowBeginEndBlock: true,
            requireBeginEndBlock: true);
        return ctx.BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace, "CREATE FUNCTION");
    }

    private static SqlCreateFunctionQuery ParsePostgreSqlCreateFunction(
        this SqlQueryParserContext ctx,
        SqlTableSource function,
        IReadOnlyList<DbFunctionParameterDef> parameters,
        bool orReplace)
    {
        ctx.ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ctx.ParseFunctionReturnTypeSql(SqlConst.AS, SqlConst.LANGUAGE);

        string? bodySql = null;
        string? language = null;

        if (ctx.IsWord(SqlConst.AS))
        {
            ctx.Consume();
            bodySql = ctx.ParseQuotedFunctionBodySql();
        }

        if (ctx.IsWord(SqlConst.LANGUAGE))
        {
            ctx.Consume();
            language = ctx.ExpectIdentifier();
        }

        if (bodySql is null && ctx.IsWord(SqlConst.AS))
        {
            ctx.Consume();
            bodySql = ctx.ParseQuotedFunctionBodySql();
        }

        if (bodySql is null)
            throw new InvalidOperationException("CREATE FUNCTION in PostgreSQL syntax requires AS '<body>'.");

        if (!string.Equals(language, "SQL", StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only PostgreSQL LANGUAGE SQL bodies in the mock.");

        var body = ctx.ParsePostgreSqlSqlFunctionBody(bodySql);
        return ctx.BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace, "CREATE FUNCTION");
    }

    private static string ReadBalancedParenRawTokens(
        this SqlQueryParserContext ctx)
    {
        if (!((ctx.Peek().Kind == SqlTokenKind.Symbol) && ctx.Peek().Text == "("))
            throw new InvalidOperationException("Expected '('");

        ctx.Consume();
        var buf = new List<SqlToken>();
        var depth = 1;
        while (true)
        {
            var token = ctx.Peek();
            if (token.Kind == SqlTokenKind.EndOfFile)
                throw new InvalidOperationException("CREATE FUNCTION parameter list was not closed correctly.");

            ctx.Consume();
            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
                depth++;
            else if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                depth--;
                if (depth == 0)
                    break;
            }

            buf.Add(token);
        }

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
