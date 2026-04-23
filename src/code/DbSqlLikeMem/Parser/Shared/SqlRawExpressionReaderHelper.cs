namespace DbSqlLikeMem;

internal static class SqlRawExpressionReaderHelper
{
    internal static List<string> ParseReturningItemsRaw(
        this SqlQueryParserContext ctx,
        Func<string> readRawExpressionUntilCommaOrTerminator)
    {
        var items = new List<string>();

        while (true)
        {
            if (ctx.IsEnd() || ctx.IsSymbol(";"))
            {
                if (items.Count == 0)
                    throw new InvalidOperationException(
                        $"RETURNING requires at least one expression (found '{ctx.DescribeFoundToken()}').");
                break;
            }

            if (ctx.IsSymbol(","))
                throw new InvalidOperationException(
                    $"RETURNING has an unexpected comma before expression (found '{ctx.DescribeFoundToken()}').");

            var raw = readRawExpressionUntilCommaOrTerminator().Trim();
            if (string.IsNullOrWhiteSpace(raw))
                throw new InvalidOperationException(
                    $"RETURNING requires at least one expression (found '{ctx.DescribeFoundToken()}').");

            items.Add(raw);

            if (ctx.IsSymbol(","))
            {
                ctx.Consume();

                if (ctx.IsEnd() || ctx.IsSymbol(";"))
                    throw new InvalidOperationException(
                        $"RETURNING has a trailing comma without expression (found '{ctx.DescribeFoundToken()}').");

                continue;
            }

            break;
        }

        return items;
    }

    internal static string ReadRawExpressionUntilCommaOrRightParen(
        this SqlQueryParserContext ctx)
    {
        var depth = 0;
        var startPos = -1;
        var lastEndPos = -1;

        while (!ctx.IsEnd())
        {
            var t = ctx.Peek();
            if (startPos < 0)
                startPos = t.Position;

            if (depth == 0 && SqlQueryParserContext.IsSymbol(t, ";"))
                throw new InvalidOperationException("ON CONFLICT target was not closed correctly (found '<end-of-statement>').");

            if (depth == 0 && (SqlQueryParserContext.IsSymbol(t, ",") || SqlQueryParserContext.IsSymbol(t, ")")))
                break;

            if (SqlQueryParserContext.IsSymbol(t, "("))
                depth++;
            else if (SqlQueryParserContext.IsSymbol(t, ")"))
            {
                if (depth == 0)
                    throw new InvalidOperationException("ON CONFLICT target has unbalanced parentheses in expression.");
                depth--;
            }

            _ = ctx.Consume();
            lastEndPos = ctx.IsEnd() ? ctx.RawSql.Length : ctx.Peek().Position;
        }

        if (depth != 0)
            throw new InvalidOperationException("ON CONFLICT target has unbalanced parentheses in expression.");

        if (startPos < 0 || lastEndPos < startPos)
            return string.Empty;

        return SqlQueryParserContext.NormalizeClauseText(ctx.RawSql.AsSpan(startPos, lastEndPos - startPos));
    }

    internal static string ReadRawExpressionUntilCommaOrTerminator(
        this SqlQueryParserContext ctx)
    {
        var depth = 0;
        var startPos = -1;
        var lastEndPos = -1;

        while (!ctx.IsEnd())
        {
            var t = ctx.Peek();
            if (startPos < 0)
                startPos = t.Position;

            if (depth == 0 && (SqlQueryParserContext.IsSymbol(t, ",") || SqlQueryParserContext.IsSymbol(t, ";")))
                break;

            if (SqlQueryParserContext.IsSymbol(t, "("))
                depth++;
            else if (SqlQueryParserContext.IsSymbol(t, ")"))
            {
                if (depth == 0)
                    throw ctx.NotSupported("RETURNING has unbalanced parentheses in expression.");
                depth--;
            }

            _ = ctx.Consume();
            lastEndPos = ctx.IsEnd() ? ctx.RawSql.Length : ctx.Peek().Position;
        }

        if (depth != 0)
            throw ctx.NotSupported("RETURNING has unbalanced parentheses in expression.");

        if (startPos < 0 || lastEndPos < startPos)
            return string.Empty;

        return SqlQueryParserContext.NormalizeClauseText(ctx.RawSql.AsSpan(startPos, lastEndPos - startPos));
    }
}
