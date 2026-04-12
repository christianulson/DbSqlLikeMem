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

            var rowValueCount = rowValuesRaw.Count;
            for (var i = 0; i < rowValueCount; i++)
            {
                if (string.IsNullOrWhiteSpace(rowValuesRaw[i]))
                    throw new InvalidOperationException("INSERT VALUES row has an empty expression between commas.");
            }

            var rowValues = rowValuesRaw;
            var rowNumber = valuesRaw.Count + 1;

            var normalizedValues = new List<string>(rowValueCount);
            var parsedValues = new List<SqlExpr?>(rowValueCount);
            for (var i = 0; i < rowValueCount; i++)
            {
                var raw = rowValues[i];
                normalizedValues.Add(SqlSimpleValueParserHelper.NormalizeSimpleSqlValueRawTrimmed(raw, ctx.Dialect));
                parsedValues.Add(ParseInsertValueExpression(raw, ctx.Dialect, rowNumber, i + 1, ctx.ParseScalar));
            }

            valuesRaw.Add(normalizedValues);
            valuesExpr.Add(parsedValues);

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

    private static SqlExpr ParseInsertValueExpression(
        string raw,
        ISqlDialect dialect,
        int rowNumber,
        int exprNumber,
        Func<string, SqlExpr> parseScalar)
    {
        if (HasDanglingTrailingOperator(raw))
            throw new InvalidOperationException(
                $"INSERT VALUES row {rowNumber} expression {exprNumber} is invalid.");

        if (SqlSimpleValueParserHelper.TryParseSimpleSqlValueExpressionTrimmed(raw, dialect, out var simpleExpr))
            return simpleExpr;

        try
        {
            return parseScalar(raw);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                $"INSERT VALUES row {rowNumber} expression {exprNumber} is invalid.",
                ex);
        }
    }

    private static bool HasDanglingTrailingOperator(string raw)
    {
        var trimmed = raw.TrimEnd();
        if (trimmed.Length == 0)
            return true;

        return trimmed[^1] is '+' or '-' or '*' or '/' or '%' or '&' or '|' or '^' or '=' or '<' or '>' or '!';
    }

}
