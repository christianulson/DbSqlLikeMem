namespace DbSqlLikeMem;

internal static class SqlInsertValuesParserHelper
{
    internal static void ParseInsertValuesRows(
        this SqlQueryParserContext ctx,
        List<List<string>> valuesRaw,
        List<List<SqlExpr?>> valuesExpr)
    {
        while (true)
        {
            if (ctx.IsEnd() || ctx.IsSymbol(";"))
            {
                if (valuesRaw.Count == 0)
                    throw new InvalidOperationException("INSERT VALUES requires at least one row.");

                return;
            }

            if (ctx.IsSymbol(","))
                throw new InvalidOperationException("INSERT VALUES has an unexpected comma before row (found ',').");

            if (!ctx.IsSymbol("("))
            {
                if (valuesRaw.Count == 0)
                    throw new InvalidOperationException("Invalid INSERT statement: expected VALUES row tuple.");

                throw new InvalidOperationException("INSERT VALUES must separate row tuples with commas.");
            }

            var rawBlock = ctx.ReadBalancedParenRawTokens();
            var rowValuesRaw = SqlRawCommaSplitterHelper.SplitRawByComma(rawBlock);

            if (rowValuesRaw.Any(v => string.IsNullOrWhiteSpace(v)))
                throw new InvalidOperationException("INSERT VALUES row has an empty expression between commas.");

            var rowValues = rowValuesRaw;
            var rowNumber = valuesRaw.Count + 1;

            valuesRaw.Add([.. rowValues.Select(raw => NormalizeInsertValueRaw(raw, ctx.Dialect))]);
            valuesExpr.Add(ParseInsertValuesRowExpressions(rowValues, rowNumber, ctx.ParseScalar));

            if (ctx.IsSymbol(","))
            {
                ctx.Consume();

                if (ctx.IsEnd() || ctx.IsSymbol(";"))
                    throw new InvalidOperationException("INSERT VALUES has a trailing comma without row tuple.");

                continue;
            }

            if (ctx.IsSymbol("("))
                throw new InvalidOperationException("INSERT VALUES must separate row tuples with commas.");

            return;
        }
    }

    private static List<SqlExpr?> ParseInsertValuesRowExpressions(
        IReadOnlyList<string> rowValues,
        int rowNumber,
        Func<string, SqlExpr> parseScalar)
    {
        var parsed = new List<SqlExpr?>(rowValues.Count);

        for (var exprIndex = 0; exprIndex < rowValues.Count; exprIndex++)
        {
            var raw = rowValues[exprIndex];
            try
            {
                parsed.Add(parseScalar(raw));
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException(
                    $"INSERT VALUES row {rowNumber} expression {exprIndex + 1} is invalid.",
                    ex);
            }
        }

        return parsed;
    }

    private static string NormalizeInsertValueRaw(string raw, ISqlDialect dialect)
    {
        raw = raw.Trim();
        if (string.IsNullOrWhiteSpace(raw))
            return raw;

        var tokens = new SqlTokenizer(raw, dialect).Tokenize();
        if (tokens.Count == 2
            && tokens[0].Kind == SqlTokenKind.String
            && tokens[1].Kind == SqlTokenKind.EndOfFile)
        {
            return tokens[0].Text;
        }

        return raw;
    }
}
