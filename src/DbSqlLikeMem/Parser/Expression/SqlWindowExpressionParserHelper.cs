namespace DbSqlLikeMem;

internal static class SqlWindowExpressionParserHelper
{
    internal static void EnsureWindowFunctionSupport(
        this SqlExpressionParserContext ctx,
        string functionName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        if (!ctx.Dialect.SupportsWindowFunctions
            || !ctx.Dialect.TryGetWindowFunctionDefinition(functionName, out _))
            throw ctx.NotSupported($"window functions ({functionName})");
    }

    internal static void EnsureWindowFunctionArguments(
        this SqlExpressionParserContext ctx,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        SqlToken contextToken)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        var argCount = args.Count;
        if (argCount < 0)
            throw ctx.Error("Invalid window function argument count.", contextToken);

        if (!ctx.Dialect.TryGetWindowFunctionDefinition(functionName, out var definition)
            || definition is null)
            return;

        if (definition.MinArguments == definition.MaxArguments && argCount != definition.MinArguments)
        {
            var message = definition.MinArguments == 0
                ? $"Window function '{functionName}' does not accept arguments."
                : $"Window function '{functionName}' requires exactly {definition.MinArguments} argument{(definition.MinArguments == 1 ? "" : "s")}.";
            throw ctx.Error(message, contextToken);
        }

        if (!definition.AllowsArgumentCount(argCount))
            throw ctx.Error($"Window function '{functionName}' requires between {definition.MinArguments} and {definition.MaxArguments} arguments.", contextToken);

        ctx.EnsureWindowFunctionArgumentLiteralRanges(functionName, args, contextToken);
    }

    internal static void EnsureWindowSpecSupport(
        this SqlExpressionParserContext ctx,
        string functionName,
        WindowSpec spec,
        SqlToken contextToken)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        ArgumentNullExceptionCompatible.ThrowIfNull(spec, nameof(spec));

        if (ctx.Dialect.TryGetWindowFunctionDefinition(functionName, out var definition)
            && definition is not null
            && definition.RequiresOrderBy
            && spec.OrderBy.Count == 0)
            throw ctx.Error($"Window function '{functionName}' requires ORDER BY in OVER clause.", contextToken);

        if (spec.Frame is not null)
            ctx.EnsureWindowFrameSemanticRange(spec.Frame, contextToken);
    }

    internal static WindowSpec ParseWindowSpec(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression,
        Func<IReadOnlyList<SqlExpr>> parseExprListUntilOrderOrParenClose
       )
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        ExpectSymbol(ctx, "(");

        var parts = new List<SqlExpr>();
        var order = new List<WindowOrderItem>();
        WindowFrameSpec? frame = null;

        if (ctx.IsKeywordOrIdentifierWord(SqlConst.PARTITION))
        {
            ctx.Consume(); // PARTITION
            if (!ctx.IsKeywordOrIdentifierWord(SqlConst.BY))
                throw ctx.Error("Esperava BY após PARTITION");
            ctx.Consume(); // BY

            parts.AddRange(parseExprListUntilOrderOrParenClose());
        }

        if (ctx.IsKeywordOrIdentifierWord(SqlConst.ORDER))
        {
            ctx.Consume(); // ORDER
            if (!ctx.IsKeywordOrIdentifierWord(SqlConst.BY))
                throw ctx.Error("Esperava BY após ORDER");
            ctx.Consume(); // BY

            while (true)
            {
                var e = parseExpression(0);

                var desc = false;
                if (ctx.IsKeywordOrIdentifierWord("DESC"))
                {
                    ctx.Consume();
                    desc = true;
                }
                else if (ctx.IsKeywordOrIdentifierWord("ASC"))
                {
                    ctx.Consume();
                }

                order.Add(new WindowOrderItem(e, desc));

                if (!ctx.IsSymbol(","))
                    break;
                ctx.Consume();
            }
        }

        if (ctx.IsKeywordOrIdentifierWord(SqlConst.ROWS)
            || ctx.IsKeywordOrIdentifierWord("RANGE")
            || ctx.IsKeywordOrIdentifierWord("GROUPS"))
        {
            frame = ctx.ParseWindowFrameClause(parseExpression);
        }

        ExpectSymbol(ctx, ")");
        return new WindowSpec(parts, order, frame);
    }

    private static void EnsureWindowFunctionArgumentLiteralRanges(
        this SqlExpressionParserContext ctx,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        SqlToken contextToken)
    {
        if (functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            && args.Count >= 1
            && TryReadIntegralLiteral(args[0], out var ntileBuckets)
            && ntileBuckets <= 0)
        {
            throw ctx.Error("Window function requires a positive integer literal.", contextToken);
        }

        if ((functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase))
            && args.Count >= 2
            && TryReadIntegralLiteral(args[1], out var lagLeadOffset)
            && lagLeadOffset < 0)
        {
            throw ctx.Error("Window function offset must be non-negative.", contextToken);
        }

        if (functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase)
            && args.Count >= 2
            && TryReadIntegralLiteral(args[1], out var nthIndex)
            && nthIndex <= 0)
        {
            throw ctx.Error("Window function position must be greater than zero.", contextToken);
        }
    }

    private static WindowFrameSpec ParseWindowFrameClause(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression)
    {
        var unit = ctx.ParseWindowFrameUnit();

        WindowFrameBound start;
        WindowFrameBound end;
        if (ctx.IsKeywordOrIdentifierWord(SqlConst.BETWEEN))
        {
            ctx.Consume(); // BETWEEN
            start = ctx.ParseWindowFrameBound(parseExpression);
            if (!ctx.IsKeywordOrIdentifierWord(SqlConst.AND))
                throw ctx.Error("Expected AND in window frame clause.");
            ctx.Consume(); // AND
            end = ctx.ParseWindowFrameBound(parseExpression);
        }
        else
        {
            start = ctx.ParseWindowFrameBound(parseExpression);
            end = new WindowFrameBound(WindowFrameBoundKind.CurrentRow, null);
        }

        return new WindowFrameSpec(unit, start, end);
    }

    private static WindowFrameUnit ParseWindowFrameUnit(
        this SqlExpressionParserContext ctx)
    {
        if (ctx.IsKeywordOrIdentifierWord(SqlConst.ROWS))
        {
            ctx.Consume();
            return WindowFrameUnit.Rows;
        }

        if (ctx.IsKeywordOrIdentifierWord("RANGE"))
        {
            ctx.Consume();
            return WindowFrameUnit.Range;
        }

        if (ctx.IsKeywordOrIdentifierWord("GROUPS"))
        {
            ctx.Consume();
            return WindowFrameUnit.Groups;
        }

        throw ctx.Error("Expected ROWS, RANGE or GROUPS in window frame clause.");
    }

    private static WindowFrameBound ParseWindowFrameBound(
        this SqlExpressionParserContext ctx,
        Func<int, SqlExpr> parseExpression)
    {
        if (ctx.IsKeywordOrIdentifierWord("UNBOUNDED"))
        {
            ctx.Consume();
            if (ctx.IsKeywordOrIdentifierWord("PRECEDING"))
            {
                ctx.Consume();
                return new WindowFrameBound(WindowFrameBoundKind.UnboundedPreceding, null);
            }

            if (ctx.IsKeywordOrIdentifierWord("FOLLOWING"))
            {
                ctx.Consume();
                return new WindowFrameBound(WindowFrameBoundKind.UnboundedFollowing, null);
            }

            throw ctx.Error("Expected PRECEDING or FOLLOWING after UNBOUNDED in window frame clause.");
        }

        if (ctx.IsKeywordOrIdentifierWord("CURRENT"))
        {
            ctx.Consume();
            if (!ctx.IsKeywordOrIdentifierWord(SqlConst.ROW))
                throw ctx.Error("Expected ROW after CURRENT in window frame clause.");
            ctx.Consume();
            return new WindowFrameBound(WindowFrameBoundKind.CurrentRow, null);
        }

        var boundExpr = parseExpression(0);
        if (!TryReadIntegralLiteral(boundExpr, out var offset) || offset < 0 || offset > int.MaxValue)
            throw ctx.Error("Expected a non-negative integer literal in window frame bound.");

        if (ctx.IsKeywordOrIdentifierWord("PRECEDING"))
        {
            ctx.Consume();
            return new WindowFrameBound(WindowFrameBoundKind.Preceding, (int)offset);
        }

        if (ctx.IsKeywordOrIdentifierWord("FOLLOWING"))
        {
            ctx.Consume();
            return new WindowFrameBound(WindowFrameBoundKind.Following, (int)offset);
        }

        throw ctx.Error("Expected PRECEDING or FOLLOWING in window frame bound.");
    }

    private static void EnsureWindowFrameSemanticRange(
        this SqlExpressionParserContext ctx,
        WindowFrameSpec frame,
        SqlToken contextToken)
    {
        var startRank = GetWindowFrameBoundRank(frame.Start);
        var endRank = GetWindowFrameBoundRank(frame.End);

        if (startRank > endRank)
            throw ctx.Error("Window frame start bound cannot be greater than end bound.", contextToken);
    }

    private static long GetWindowFrameBoundRank(WindowFrameBound bound)
    {
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => long.MinValue,
            WindowFrameBoundKind.Preceding => -bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.CurrentRow => 0,
            WindowFrameBoundKind.Following => bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.UnboundedFollowing => long.MaxValue,
            _ => 0
        };
    }

    private static bool TryReadIntegralLiteral(SqlExpr expr, out long value)
    {
        value = default;
        if (expr is not LiteralExpr { Value: not null and not DBNull and IConvertible literalValue })
            return false;

        try
        {
            value = Convert.ToInt64(literalValue, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void ExpectSymbol(
        SqlExpressionParserContext ctx,
        string symbol)
    {
        if (!ctx.IsSymbol(symbol))
            throw ctx.Error($"Expected '{symbol}' in window specification.");

        ctx.Consume();
    }
}
