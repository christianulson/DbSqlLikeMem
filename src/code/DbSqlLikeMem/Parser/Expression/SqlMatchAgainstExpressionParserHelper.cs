namespace DbSqlLikeMem;

internal static class SqlMatchAgainstExpressionParserHelper
{
    internal static bool TryParseMatchAgainstInfix(
        this SqlExpressionParserContext ctx,
        CallExpr call,
        Func<IReadOnlyList<SqlToken>, SqlToken, string, SqlExpr> parseStandaloneExpression,
        out SqlExpr expr)
    {
        expr = default!;
        if (!call.Name.Equals("MATCH", StringComparison.OrdinalIgnoreCase))
            return false;

        if (!ctx.IsKeywordOrIdentifierWord("AGAINST"))
            return false;

        if (!ctx.Dialect.SupportsMatchAgainstPredicate)
            throw ctx.NotSupported("MATCH ... AGAINST full-text predicate");

        var againstToken = ctx.Consume(); // AGAINST
        if (ctx.Peek().Kind != SqlTokenKind.Symbol || ctx.Peek().Text != "(")
            throw ctx.Error("Expected '(' after MATCH ... AGAINST.", ctx.Peek());

        ctx.Consume(); // (

        var payloadTokens = ReadTokensUntilMatchingParen(ctx);
        if (ctx.Peek().Kind != SqlTokenKind.Symbol || ctx.Peek().Text != ")")
            throw ctx.Error("AGAINST clause was not closed for MATCH(...).", againstToken);
        ctx.Consume(); // )

        var (queryTokens, modeTokens) = SplitMatchAgainstPayload(payloadTokens);
        if (queryTokens.Count == 0)
            throw ctx.Error("MATCH ... AGAINST requires a search expression.", againstToken);

        var queryExpr = parseStandaloneExpression(queryTokens, againstToken, "MATCH ... AGAINST search expression");

        var args = new List<SqlExpr>
        {
            new RowExpr(call.Args),
            queryExpr
        };

        if (modeTokens.Count > 0)
        {
            var modeSql = ctx.ParseAndValidateMatchAgainstMode(modeTokens, againstToken);
            args.Add(new RawSqlExpr(modeSql));
        }

        expr = new CallExpr("MATCH_AGAINST", args)
            .BindScalarFunctionDefinition(ctx.Dialect);
        return true;
    }

    private static IReadOnlyList<SqlToken> ReadTokensUntilMatchingParen(
        this SqlExpressionParserContext ctx)
    {
        var depth = 0;
        var tokens = new List<SqlToken>();

        while (true)
        {
            var token = ctx.Peek();
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                tokens.Add(ctx.Consume());
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                if (depth == 0)
                    break;

                depth--;
                tokens.Add(ctx.Consume());
                continue;
            }

            tokens.Add(ctx.Consume());
        }

        return tokens;
    }

    private static string ParseAndValidateMatchAgainstMode(
        this SqlExpressionParserContext ctx,
        IReadOnlyList<SqlToken> modeTokens,
        SqlToken contextToken)
    {
        if (modeTokens.Count == 0)
            return string.Empty;

        if (WordsEqual(modeTokens, SqlConst.IN, "BOOLEAN", "MODE")
            || WordsEqual(modeTokens, SqlConst.IN, "NATURAL", SqlConst.LANGUAGE, "MODE")
            || WordsEqual(modeTokens, SqlConst.IN, "NATURAL", SqlConst.LANGUAGE, "MODE", SqlConst.WITH, "QUERY", "EXPANSION")
            || WordsEqual(modeTokens, SqlConst.WITH, "QUERY", "EXPANSION"))
            return string.Join(" ", modeTokens.Select(TokenToSql)).Trim();

        throw ctx.Error(
            "Unsupported AGAINST mode. Supported forms: IN BOOLEAN MODE, IN NATURAL LANGUAGE MODE, IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION, WITH QUERY EXPANSION.",
            contextToken);
    }

    private static bool WordsEqual(IReadOnlyList<SqlToken> actual, params string[] expected)
    {
        if (actual.Count != expected.Length)
            return false;

        for (var i = 0; i < expected.Length; i++)
        {
            if (!actual[i].Text.Equals(expected[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static (IReadOnlyList<SqlToken> QueryTokens, IReadOnlyList<SqlToken> ModeTokens) SplitMatchAgainstPayload(
        IReadOnlyList<SqlToken> payloadTokens)
    {
        if (payloadTokens.Count == 0)
            return ([], []);

        var depth = 0;
        var splitAt = -1;
        for (var i = 0; i < payloadTokens.Count; i++)
        {
            var token = payloadTokens[i];
            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.IN)
                && i + 1 < payloadTokens.Count
                && (SqlExpressionParserContext.IsKeywordOrIdentifierWord(payloadTokens[i + 1], "BOOLEAN")
                    || SqlExpressionParserContext.IsKeywordOrIdentifierWord(payloadTokens[i + 1], "NATURAL")
                    || SqlExpressionParserContext.IsKeywordOrIdentifierWord(payloadTokens[i + 1], "QUERY")))
            {
                splitAt = i;
                break;
            }

            if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.WITH)
                && i + 1 < payloadTokens.Count
                && SqlExpressionParserContext.IsKeywordOrIdentifierWord(payloadTokens[i + 1], "QUERY"))
            {
                splitAt = i;
                break;
            }
        }

        if (splitAt < 0)
            return (payloadTokens, []);

        return (
            payloadTokens.Take(splitAt).ToArray(),
            payloadTokens.Skip(splitAt).ToArray());
    }

    private static string TokenToSql(SqlToken token)
        => token.Text;
}
