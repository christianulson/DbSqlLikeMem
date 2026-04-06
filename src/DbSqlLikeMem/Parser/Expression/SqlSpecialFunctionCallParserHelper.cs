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

        if (name.Equals("OVERLAY", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseOverlay(parseExpression, out expr);

        if (name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseTryCast(parseExpression, out expr);

        if (name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseTryConvert(parseExpression, out expr);

        if (name.Equals("PARSE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase))
            return ctx.TryParseParseLike(name, parseExpression, out expr);

        if (name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
            && (IsFirebirdDialect(ctx.Dialect)
                || (AutoDialectFactory.IsAutoDialect(ctx.Dialect) && LooksLikeFirebirdDateAddSyntax(ctx))))
            return ctx.TryParseFirebirdDateAdd(parseExpression, out expr);

        if (name.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
            && (IsFirebirdDialect(ctx.Dialect)
                || (AutoDialectFactory.IsAutoDialect(ctx.Dialect) && LooksLikeFirebirdSubstringSyntax(ctx))))
            return ctx.TryParseFirebirdSubstring(parseExpression, out expr);

        if (name.Equals("HASH", StringComparison.OrdinalIgnoreCase)
            && IsFirebirdDialectOrAuto(ctx.Dialect))
            return ctx.TryParseFirebirdHash(parseExpression, out expr);

        if (name.Equals("CRYPT_HASH", StringComparison.OrdinalIgnoreCase)
            && IsFirebirdDialectOrAuto(ctx.Dialect))
            return ctx.TryParseFirebirdCryptHash(parseExpression, out expr);

        if (name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("JSON_TABLE is a table function and cannot be used as a scalar expression.");
        }

        return false;
    }

    private static bool IsFirebirdDialectOrAuto(ISqlDialect dialect)
        => dialect.Name.Equals("firebird", StringComparison.OrdinalIgnoreCase)
           || AutoDialectFactory.IsAutoDialect(dialect);

    private static bool IsFirebirdDialect(ISqlDialect dialect)
        => dialect.Name.Equals("firebird", StringComparison.OrdinalIgnoreCase);

    private static bool LooksLikeFirebirdDateAddSyntax(SqlExpressionParserContext ctx)
    {
        var startIndex = ctx.IsSymbol("(") ? ctx.Index + 1 : ctx.Index;
        var depth = 0;
        var caseDepth = 0;

        for (var i = startIndex; i < ctx.Toks.Count; i++)
        {
            var token = ctx.Toks[i];
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            if (depth == 0)
            {
                if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, "CASE"))
                {
                    caseDepth++;
                }
                else if (caseDepth > 0 && SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.END))
                {
                    caseDepth--;
                }

                if (caseDepth == 0)
                {
                    if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.TO))
                        return true;

                    if (token.Kind == SqlTokenKind.Symbol && token.Text == ",")
                        return false;

                    if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
                        return false;
                }
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
                depth++;
            else if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
                depth = Math.Max(0, depth - 1);
        }

        return false;
    }

    private static bool LooksLikeFirebirdSubstringSyntax(SqlExpressionParserContext ctx)
    {
        var startIndex = ctx.IsSymbol("(") ? ctx.Index + 1 : ctx.Index;
        var depth = 0;
        var caseDepth = 0;

        for (var i = startIndex; i < ctx.Toks.Count; i++)
        {
            var token = ctx.Toks[i];
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            if (depth == 0)
            {
                if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, "CASE"))
                {
                    caseDepth++;
                }
                else if (caseDepth > 0 && SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.END))
                {
                    caseDepth--;
                }

                if (caseDepth == 0)
                {
                    if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, "FROM"))
                        return true;

                    if (token.Kind == SqlTokenKind.Symbol && token.Text == ",")
                        return false;

                    if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
                        return false;
                }
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
                depth++;
            else if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
                depth = Math.Max(0, depth - 1);
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

    private static bool TryParseOverlay(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;
        var source = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord("PLACING"))
            throw ctx.Error("OVERLAY requires PLACING", ctx.Peek());
        ctx.Consume();

        var replacement = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord("FROM"))
            throw ctx.Error("OVERLAY requires FROM", ctx.Peek());
        ctx.Consume();

        var position = parseExpression(0);
        var args = new List<SqlExpr> { source, replacement, position };

        if (ctx.IsKeywordOrIdentifierWord(SqlConst.FOR))
        {
            ctx.Consume();
            args.Add(parseExpression(0));
        }

        ExpectSymbol(ctx, ")");
        expr = new CallExpr("OVERLAY", [.. args])
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

    private static bool TryParseFirebirdDateAdd(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;

        var amountAndUnitTokens = ctx.ReadTokensUntilTopLevelStop("TO");
        if (amountAndUnitTokens.Count < 2)
            throw ctx.Error("DATEADD requires an amount and a unit before TO", ctx.Peek());

        if (!ctx.IsKeywordOrIdentifierWord("TO"))
            throw ctx.Error("DATEADD requires TO", ctx.Peek());

        var unitToken = amountAndUnitTokens[^1];
        if (unitToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw ctx.Error("DATEADD requires a unit before TO", unitToken);

        var amountTokens = amountAndUnitTokens.Take(amountAndUnitTokens.Count - 1).ToArray();
        if (amountTokens.Length == 0)
            throw ctx.Error("DATEADD requires an amount before the unit", unitToken);

        ctx.Consume(); // TO

        var amountSql = string.Join(" ", amountTokens.Select(ctx.TokenToSql)).Trim();
        var amountExpr = SqlExpressionParser.ParseScalar(
            amountSql,
            ctx.Db,
            ctx.Dialect,
            ctx.Parameters,
            ctx.CustomFunctionSupported);

        var dateExpr = parseExpression(0);
        ExpectSymbol(ctx, ")");

        expr = new CallExpr("DATEADD", [new RawSqlExpr(unitToken.Text), amountExpr, dateExpr])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseFirebirdSubstring(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;

        var sourceTokens = ctx.ReadTokensUntilTopLevelStop("FROM");
        if (sourceTokens.Count == 0)
            throw ctx.Error("SUBSTRING requires a source expression", ctx.Peek());

        if (!ctx.IsKeywordOrIdentifierWord("FROM"))
            throw ctx.Error("SUBSTRING requires FROM", ctx.Peek());

        ctx.Consume(); // FROM

        var positionTokens = ctx.ReadTokensUntilTopLevelStop(SqlConst.FOR);
        if (positionTokens.Count == 0)
            throw ctx.Error("SUBSTRING requires a position after FROM", ctx.Peek());

        var sourceExpr = SqlExpressionParser.ParseScalar(
            ctx.TokensToSql(sourceTokens).Trim(),
            ctx.Db,
            ctx.Dialect,
            ctx.Parameters,
            ctx.CustomFunctionSupported);
        var positionExpr = SqlExpressionParser.ParseScalar(
            ctx.TokensToSql(positionTokens).Trim(),
            ctx.Db,
            ctx.Dialect,
            ctx.Parameters,
            ctx.CustomFunctionSupported);

        var args = new List<SqlExpr> { sourceExpr, positionExpr };
        if (ctx.IsKeywordOrIdentifierWord(SqlConst.FOR))
        {
            ctx.Consume(); // FOR
            args.Add(parseExpression(0));
        }

        ExpectSymbol(ctx, ")");

        expr = new CallExpr("SUBSTRING", [.. args])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseFirebirdHash(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;

        var valueExpr = parseExpression(0);
        var args = new List<SqlExpr> { valueExpr };

        if (ctx.IsKeywordOrIdentifierWord("USING"))
        {
            ctx.Consume(); // USING

            var algorithmToken = ctx.Peek();
            if (algorithmToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                throw ctx.Error("HASH requires a hash algorithm", algorithmToken);

            ctx.Consume();
            args.Add(new RawSqlExpr(algorithmToken.Text));
        }

        ExpectSymbol(ctx, ")");

        expr = new CallExpr("HASH", [.. args])
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static bool TryParseFirebirdCryptHash(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        out CallExpr expr)
    {
        expr = default!;

        var valueExpr = parseExpression(0);

        if (!ctx.IsKeywordOrIdentifierWord("USING"))
            throw ctx.Error("CRYPT_HASH requires USING", ctx.Peek());

        ctx.Consume(); // USING

        var algorithmToken = ctx.Peek();
        if (algorithmToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw ctx.Error("CRYPT_HASH requires a hash algorithm", algorithmToken);

        ctx.Consume();
        ExpectSymbol(ctx, ")");

        expr = new CallExpr("CRYPT_HASH", [valueExpr, new RawSqlExpr(algorithmToken.Text)])
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
