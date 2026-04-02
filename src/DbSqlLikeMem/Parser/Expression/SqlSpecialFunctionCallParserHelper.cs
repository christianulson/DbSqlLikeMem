namespace DbSqlLikeMem;

internal static class SqlSpecialFunctionCallParserHelper
{
    internal static bool TryParseSpecialCall(
        this SqlExpressionParserContext ctx,
        string name,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        expr = default!;

        if (name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseCast(parseExpression, out expr);

        if (name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseTryCast(parseExpression, out expr);

        if (name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseTryConvert(parseExpression, out expr);

        if (name.Equals("PARSE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseParseLike(name, parseExpression, out expr);

        if (name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("JSON_TABLE is a table function and cannot be used as a scalar expression.");
        }

        return false;
    }

    private static bool TryParseCast(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;
        var inner = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord(SqlConst.AS))
            throw ctx.Error("CAST requires AS", ctx.Peek());

        ctx.Consume(); // AS

        var typeToks = new List<SqlToken>();
        var depth = 0;
        while (true)
        {
            var t = ctx.Peek();
            if (t.Kind == SqlTokenKind.EndOfFile)
                throw ctx.Error("CAST type not closed", t);

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                depth++;

            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0)
                    break;
                depth--;
            }

            typeToks.Add(ctx.Consume());
        }

        ExpectSymbol(ctx, ")");

        var typeSql = string.Join(" ", typeToks.Select(ctx.TokenToSql)).Trim();
        expr = new CallExpr("CAST", [inner, new RawSqlExpr(typeSql)])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseTryCast(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;
        var inner = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord(SqlConst.AS))
            throw ctx.Error("TRY_CAST requires AS", ctx.Peek());

        ctx.Consume(); // AS

        var typeToks = new List<SqlToken>();
        var depth = 0;
        while (true)
        {
            var t = ctx.Peek();
            if (t.Kind == SqlTokenKind.EndOfFile)
                throw ctx.Error("TRY_CAST type not closed", t);

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                depth++;

            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0)
                    break;
                depth--;
            }

            typeToks.Add(ctx.Consume());
        }

        ExpectSymbol(ctx, ")");
        var typeSql = string.Join(" ", typeToks.Select(ctx.TokenToSql)).Trim();
        expr = new CallExpr("TRY_CAST", [inner, new RawSqlExpr(typeSql)])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseTryConvert(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;

        var typeToks = new List<SqlToken>();
        var depth = 0;
        while (true)
        {
            var t = ctx.Peek();
            if (t.Kind == SqlTokenKind.EndOfFile)
                throw ctx.Error("TRY_CONVERT type not closed", t);

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                depth++;

            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0)
                    throw ctx.Error("TRY_CONVERT requires an expression argument", t);
                depth--;
            }

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "," && depth == 0)
                break;

            typeToks.Add(ctx.Consume());
        }

        if (typeToks.Count == 0)
            throw ctx.Error("TRY_CONVERT requires a target type", ctx.Peek());

        ExpectSymbol(ctx, ",");

        var inner = parseExpression(0);
        var convertArgs = new List<SqlExpr>
        {
            inner,
            new RawSqlExpr(string.Join(" ", typeToks.Select(ctx.TokenToSql)).Trim())
        };

        if (ctx.IsSymbol(","))
        {
            ctx.Consume();
            convertArgs.Add(parseExpression(0));
        }

        ExpectSymbol(ctx, ")");
        expr = new CallExpr("TRY_CONVERT", [.. convertArgs])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseParseLike(
        this SqlExpressionParserContext ctx,
        string name,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;
        var functionName = name.ToUpperInvariant();
        var inner = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord(SqlConst.AS))
            throw ctx.Error($"{functionName} requires AS", ctx.Peek());
        ctx.Consume();

        var typeToks = new List<SqlToken>();
        while (true)
        {
            var t = ctx.Peek();
            if (t.Kind == SqlTokenKind.EndOfFile)
                throw ctx.Error($"{functionName} type not closed", t);

            if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.USING) || SqlExpressionParserContext.IsSymbol(t, ")"))
                break;

            typeToks.Add(ctx.Consume());
        }

        if (typeToks.Count == 0)
            throw ctx.Error($"{functionName} requires a target type", ctx.Peek());

        var parseArgs = new List<SqlExpr>
        {
            inner,
            new RawSqlExpr(string.Join(" ", typeToks.Select(ctx.TokenToSql)).Trim())
        };

        if (ctx.IsKeywordOrIdentifierWord(SqlConst.USING))
        {
            ctx.Consume();
            parseArgs.Add(parseExpression(0));
        }

        ExpectSymbol(ctx, ")");
        expr = new CallExpr(functionName, [.. parseArgs])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static void ExpectSymbol(
        SqlExpressionParserContext ctx,
        string symbol)
    {
        if (!ctx.IsSymbol(symbol))
            throw ctx.Error($"Expected '{symbol}'", ctx.Peek());

        ctx.Consume();
    }
}
