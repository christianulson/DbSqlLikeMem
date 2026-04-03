using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryWindowExecutionHelper
{
    internal static void ComputeWindowSlots(
        this QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        List<WindowSlot> slots,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        if (slots.Count == 0 || rows.Count == 0)
            return;

        foreach (var slotGroup in GroupWindowSlotsBySpec(slots))
        {
            var spec = slotGroup[0].Expr.Spec;
            var partitions = WindowPartitionHelper.BuildPartitions(
                slotGroup[0].Expr,
                rows,
                (expr, row) => eval(expr, row, null, ctes),
                context.NormalizeDistinctKey);

            foreach (var part in partitions.Values)
            {
                var orderValuesByRow = WindowPartitionHelper.SortPartition(
                    part,
                    spec.OrderBy,
                    (expr, row) => eval(expr, row, null, ctes),
                    context.CompareSql);
                var partitionContext = new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);

                foreach (var slot in slotGroup)
                {
                    var w = slot.Expr;
                    var dialectInstance = context.Dialect ?? throw new InvalidOperationException("Dialect is required for window function validation.");
                    var windowDefinition = w.ResolvedWindowFunction;
                    if (windowDefinition is null
                        && dialectInstance.TryGetWindowFunctionDefinition(w, out var resolvedWindowDefinition))
                    {
                        windowDefinition = resolvedWindowDefinition;
                    }

                    var isRowNumber = dialectInstance.IsRowNumberWindowFunction(w.Name);
                    var isRank = w.Name.Equals("RANK", StringComparison.OrdinalIgnoreCase);
                    var isDenseRank = w.Name.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase);
                    var isNtile = w.Name.Equals("NTILE", StringComparison.OrdinalIgnoreCase);
                    var isPercentRank = w.Name.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase);
                    var isCumeDist = w.Name.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase);
                    var isLag = w.Name.Equals("LAG", StringComparison.OrdinalIgnoreCase);
                    var isLead = w.Name.Equals("LEAD", StringComparison.OrdinalIgnoreCase);
                    var isFirstValue = w.Name.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isLastValue = w.Name.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isNthValue = w.Name.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isCount = w.Name.Equals("COUNT", StringComparison.OrdinalIgnoreCase);
                    var isAggregateWindow = AggregateFunctionCatalog.Contains(w.Name);
                    var isSqlServer = dialectInstance.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase);

                    var resolvedWindowDefinition1 = windowDefinition
                        ?? throw SqlUnsupported.NotSupported(dialectInstance, $"window functions ({w.Name})");

                    if (isSqlServer
                        && w.Spec.Frame is not null
                        && !AstQueryWindowFunctionSupport.SupportsWindowFrame(w.Name))
                    {
                        throw new InvalidOperationException(
                            $"Window function '{w.Name}' does not support ROWS, RANGE or GROUPS clauses.");
                    }

                    if (resolvedWindowDefinition1.RequiresOrderBy && w.Spec.OrderBy.Count == 0)
                        throw new InvalidOperationException($"Window function '{w.Name}' requires ORDER BY in OVER clause.");

                    if (isRowNumber)
                    {
                        long rn = 1;
                        foreach (var r in part)
                        {
                            slot.Map[r] = rn;
                            rn++;
                        }
                        continue;
                    }

                    var valueSelector = part.Count > 0
                        ? partitionContext.TryCreateWindowValueSelector(
                            w.Args.Count > 0 ? w.Args[0] : null!,
                            part[0])
                        : null;

                    if (isNtile)
                    {
                        partitionContext.FillNtile(slot.Map, w, ctes, eval);
                        continue;
                    }

                    if (isCount)
                    {
                        partitionContext.FillCount(slot.Map, w, ctes, eval, valueSelector);
                        continue;
                    }

                    if (isAggregateWindow)
                    {
                        partitionContext.FillAggregate(slot.Map, w, ctes, eval);
                        continue;
                    }

                    if (isPercentRank || isCumeDist)
                    {
                        partitionContext.FillPercentRankOrCumeDist(slot.Map, isPercentRank);
                        continue;
                    }

                    if (isLag || isLead)
                    {
                        partitionContext.FillLagOrLead(slot.Map, w, ctes, eval, valueSelector, isLead);
                        continue;
                    }

                    if (isFirstValue || isLastValue)
                    {
                        partitionContext.FillFirstOrLastValue(slot.Map, w, ctes, eval, valueSelector, isLastValue);
                        continue;
                    }

                    if (isNthValue)
                    {
                        partitionContext.FillNthValue(slot.Map, w, ctes, eval, valueSelector);
                        continue;
                    }

                    partitionContext.FillRankOrDenseRank(slot.Map, context.CompareSql, isRank);
                }
            }
        }
    }

    private static List<List<WindowSlot>> GroupWindowSlotsBySpec(List<WindowSlot> slots)
    {
        var groups = new Dictionary<string, List<WindowSlot>>(Math.Max(1, slots.Count), StringComparer.Ordinal);
        foreach (var slot in slots)
        {
            var key = BuildWindowSpecCacheKey(slot.Expr.Spec);
            if (!groups.TryGetValue(key, out var group))
            {
                group = new List<WindowSlot>();
                groups[key] = group;
            }

            group.Add(slot);
        }

        return [.. groups.Values];
    }

    internal static string BuildWindowSpecCacheKey(WindowSpec spec)
    {
        var sb = new StringBuilder();
        sb.Append("PART:");
        for (var i = 0; i < spec.PartitionBy.Count; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(spec.PartitionBy[i]));
        }

        sb.Append(";ORDER:");
        for (var i = 0; i < spec.OrderBy.Count; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(spec.OrderBy[i].Expr));
            sb.Append(spec.OrderBy[i].Desc ? ":DESC" : ":ASC");
        }

        sb.Append(";FRAME:");
        if (spec.Frame is null)
        {
            sb.Append(SqlConst.NULL);
            return sb.ToString();
        }

        sb.Append(spec.Frame.Unit);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, spec.Frame.Start);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, spec.Frame.End);
        return sb.ToString();
    }

    private static void AppendWindowFrameBoundCacheKey(StringBuilder sb, WindowFrameBound bound)
    {
        sb.Append(bound.Kind);
        sb.Append('(');
        sb.Append(bound.Offset?.ToString(CultureInfo.InvariantCulture) ?? "null");
        sb.Append(')');
    }

    private static void FillCount(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector)
    {
        var context = partitionContext.QueryExecutionContext;
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        var countArg = windowFunctionExpr.Args.Count > 0 ? windowFunctionExpr.Args[0] : null;
        var countAllRows = countArg is null || countArg is StarExpr;
        var countDistinct = windowFunctionExpr.Distinct;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            mapAllRows(
                map,
                part,
                countAllRows
                    ? CountWholePartition(part)
                    : CountWholePartition(context, part, countArg!, ctes, eval, valueSelector, countDistinct));
            return;
        }

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = 0L;
                continue;
            }

            map[part[i]] = countAllRows
                ? frameRange.EndIndex - frameRange.StartIndex + 1L
                : CountFrame(context, frameRange, part, countArg!, ctes, eval, valueSelector, countDistinct);
        }

        static void mapAllRows(
            Dictionary<EvalRow, object?> target,
            List<EvalRow> rows,
            long value)
        {
            foreach (var row in rows)
                target[row] = value;
        }
    }

    private static long CountWholePartition(
        List<EvalRow> part)
        => part.Count;

    private static long CountWholePartition(
        QueryExecutionContext context,
        List<EvalRow> part,
        SqlExpr countArg,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector,
        bool distinct)
        => CountFrame(context, new RowsFrameRange(0, part.Count - 1, false), part, countArg, ctes, eval, valueSelector, distinct);

    private static long CountFrame(
        QueryExecutionContext context,
        RowsFrameRange frameRange,
        List<EvalRow> part,
        SqlExpr countArg,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector,
        bool distinct)
    {
        if (frameRange.IsEmpty)
            return 0L;

        HashSet<string>? seen = distinct ? new HashSet<string>(StringComparer.Ordinal) : null;
        long count = 0;
        for (var i = frameRange.StartIndex; i <= frameRange.EndIndex; i++)
        {
            var row = part[i];
            var value = valueSelector is null
                ? eval(countArg, row, null, ctes)
                : valueSelector(row);
            if (AstQueryExecutorBase.IsNullish(value))
                continue;

            if (seen is not null)
            {
                var key = context.NormalizeDistinctKey(value);
                if (!seen.Add(key))
                    continue;
            }

            count++;
        }

        return count;
    }

    private static void FillAggregate(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var wholePartitionValue = partitionContext.QueryExecutionContext.EvalAggregate(
                new FunctionCallExpr(windowFunctionExpr.Name, windowFunctionExpr.Args, windowFunctionExpr.Distinct),
                new EvalGroup(part),
                ctes,
                eval);

            foreach (var row in part)
                map[row] = wholePartitionValue;

            return;
        }

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            var frameRows = frameRange.IsEmpty
                ? []
                : part.GetRange(frameRange.StartIndex, frameRange.EndIndex - frameRange.StartIndex + 1);

            map[part[i]] = partitionContext.QueryExecutionContext.EvalAggregate(
                new FunctionCallExpr(windowFunctionExpr.Name, windowFunctionExpr.Args, windowFunctionExpr.Distinct),
                new EvalGroup(frameRows),
                ctes,
                eval);
        }
    }
}
