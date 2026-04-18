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

        var dialectInstance = context.Dialect ?? throw new InvalidOperationException("Dialect is required for window function validation.");
        var isSqlServer = dialectInstance.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase);

        foreach (var slotGroup in GroupWindowSlotsBySpec(slots))
        {
            var firstSlot = slotGroup[0];
            var firstExpr = firstSlot.Expr;
            var spec = firstExpr.Spec;
            var frame = spec.Frame;
            var orderBy = spec.OrderBy;
            var orderByCount = orderBy.Count;
            var hasFrame = frame is not null;
            var slotGroupRequiresOrderValues = SlotGroupRequiresOrderValues(slotGroup);
            var partitions = WindowPartitionHelper.BuildPartitions(
                firstExpr,
                rows,
                (expr, row) => eval(expr, row, null, ctes),
                context.NormalizeDistinctKey);

            foreach (var part in partitions.Values)
            {
                var partCount = part.Count;
                var orderValuesByRow = WindowPartitionHelper.SortPartition(
                    part,
                    orderBy,
                    (expr, row) => eval(expr, row, null, ctes),
                    context.CompareSql,
                    includeOrderValues: slotGroupRequiresOrderValues);
                var slotGroupCount = slotGroup.Count;
                WindowPartitionExecutionContext? partitionContext = null;
                EvalRow? sampleRow = null;

                for (var slotIndex = 0; slotIndex < slotGroupCount; slotIndex++)
                {
                    var slot = slotGroup[slotIndex];
                    var slotMap = slot.Map;
                    var w = slot.Expr;
                    var windowName = w.Name;
                    var windowDefinition = w.ResolvedWindowFunction;
                    if (windowDefinition is null
                        && dialectInstance.TryGetWindowFunctionDefinition(w, out var resolvedWindowDefinition))
                    {
                        windowDefinition = resolvedWindowDefinition;
                    }

                    var windowKind = AstQueryWindowFunctionSupport.ClassifyWindowFunction(windowName);
                    var isAggregateWindow = windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.Other
                        && AggregateFunctionCatalog.Contains(windowName);

                    var resolvedWindowDefinition1 = windowDefinition
                        ?? throw SqlUnsupported.NotSupported(dialectInstance, $"window functions ({windowName})");
                    var args = w.Args;
                    var argsCount = args.Count;

                    if (isSqlServer
                        && hasFrame
                        && !AstQueryWindowFunctionSupport.SupportsWindowFrame(windowName))
                    {
                        throw new InvalidOperationException(
                            $"Window function '{windowName}' does not support ROWS, RANGE or GROUPS clauses.");
                    }

                    if (resolvedWindowDefinition1.RequiresOrderBy && orderByCount == 0)
                        throw new InvalidOperationException($"Window function '{windowName}' requires ORDER BY in OVER clause.");

                    var firstArg = argsCount > 0 ? args[0] : null;
                    Func<EvalRow, object?>? valueSelector = null;

                    if (windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.RowNumber)
                    {
                        long rn = 1;
                        for (var i = 0; i < partCount; i++)
                        {
                            slotMap[part[i]] = rn++;
                        }
                        continue;
                    }

                    if (windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.Ntile)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        partitionContext.FillNtile(slotMap, w, ctes, eval);
                        continue;
                    }

                    if (windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.Count)
                    {
                        if (firstArg is not null && firstArg is not StarExpr)
                        {
                            sampleRow ??= part[0];
                            partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                            valueSelector = GetValueSelector(partitionContext, firstArg, sampleRow);
                        }
                        else
                        {
                            partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        }

                        partitionContext.FillCount(
                            slotMap,
                            w,
                            ctes,
                            eval,
                            valueSelector);
                        continue;
                    }

                    if (isAggregateWindow)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        partitionContext.FillAggregate(slotMap, w, ctes, eval);
                        continue;
                    }

                    if (windowKind is AstQueryWindowFunctionSupport.WindowFunctionKind.PercentRank
                        or AstQueryWindowFunctionSupport.WindowFunctionKind.CumeDist)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        partitionContext.FillPercentRankOrCumeDist(
                            slotMap,
                            windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.PercentRank);
                        continue;
                    }

                    if (windowKind is AstQueryWindowFunctionSupport.WindowFunctionKind.Lag
                        or AstQueryWindowFunctionSupport.WindowFunctionKind.Lead)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        valueSelector ??= firstArg is null
                            ? null
                            : GetValueSelector(partitionContext, firstArg, sampleRow ??= part[0]);

                        partitionContext.FillLagOrLead(
                            slotMap,
                            w,
                            ctes,
                            eval,
                            valueSelector,
                            windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.Lead);
                        continue;
                    }

                    if (windowKind is AstQueryWindowFunctionSupport.WindowFunctionKind.FirstValue
                        or AstQueryWindowFunctionSupport.WindowFunctionKind.LastValue)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        valueSelector ??= firstArg is null
                            ? null
                            : GetValueSelector(partitionContext, firstArg, sampleRow ??= part[0]);

                        partitionContext.FillFirstOrLastValue(
                            slotMap,
                            w,
                            ctes,
                            eval,
                            valueSelector,
                            windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.LastValue);
                        continue;
                    }

                    if (windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.NthValue)
                    {
                        partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                        valueSelector ??= firstArg is null
                            ? null
                            : GetValueSelector(partitionContext, firstArg, sampleRow ??= part[0]);

                        partitionContext.FillNthValue(slotMap, w, ctes, eval, valueSelector);
                        continue;
                    }

                    partitionContext ??= new WindowPartitionExecutionContext(context, part, spec, ctes, orderValuesByRow, eval);
                    partitionContext.FillRankOrDenseRank(
                        slotMap,
                        context.CompareSql,
                        windowKind == AstQueryWindowFunctionSupport.WindowFunctionKind.Rank);
                }
            }
        }
    }

    private static Func<EvalRow, object?>? GetValueSelector(
        WindowPartitionExecutionContext partitionContext,
        SqlExpr? firstArg,
        EvalRow sampleRow)
        => firstArg is null
            ? null
            : partitionContext.TryCreateWindowValueSelector(firstArg, sampleRow);

    private static bool SlotGroupRequiresOrderValues(List<WindowSlot> slotGroup)
    {
        for (var i = 0; i < slotGroup.Count; i++)
        {
            var windowKind = AstQueryWindowFunctionSupport.ClassifyWindowFunction(slotGroup[i].Expr.Name);
            if (windowKind is not AstQueryWindowFunctionSupport.WindowFunctionKind.RowNumber
                and not AstQueryWindowFunctionSupport.WindowFunctionKind.Ntile)
            {
                return true;
            }
        }

        return false;
    }

    private static List<List<WindowSlot>> GroupWindowSlotsBySpec(List<WindowSlot> slots)
    {
        var slotCount = slots.Count;
        var groups = new Dictionary<string, List<WindowSlot>>(Math.Max(1, slotCount), StringComparer.Ordinal);
        var keyCache = new Dictionary<WindowSpec, string>(ReferenceEqualityComparer<WindowSpec>.Instance);
        foreach (var slot in slots)
        {
            var spec = slot.Expr.Spec;
            if (!keyCache.TryGetValue(spec, out var key))
            {
                key = BuildWindowSpecCacheKey(spec);
                keyCache[spec] = key;
            }

            if (!groups.TryGetValue(key, out var group))
            {
                group = new List<WindowSlot>(4);
                groups[key] = group;
            }

            group.Add(slot);
        }

        var groupedSlots = new List<List<WindowSlot>>(groups.Count);
        foreach (var group in groups.Values)
            groupedSlots.Add(group);

        return groupedSlots;
    }

    internal static string BuildWindowSpecCacheKey(WindowSpec spec)
    {
        var partitionBy = spec.PartitionBy;
        var orderBy = spec.OrderBy;
        var partitionByCount = partitionBy.Count;
        var orderByCount = orderBy.Count;
        var frame = spec.Frame;
        var estimatedCapacity = 32 + (partitionByCount * 24) + (orderByCount * 24);
        if (frame is not null)
            estimatedCapacity += 48;

        var sb = new StringBuilder(estimatedCapacity);

        sb.Append("PART:");
        for (var i = 0; i < partitionByCount; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(partitionBy[i]));
        }

        sb.Append(";ORDER:");
        for (var i = 0; i < orderByCount; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(orderBy[i].Expr));
            sb.Append(orderBy[i].Desc ? ":DESC" : ":ASC");
        }

        sb.Append(";FRAME:");
        if (frame is null)
        {
            sb.Append(SqlConst.NULL);
            return sb.ToString();
        }

        sb.Append(frame.Unit);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, frame.Start);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, frame.End);
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

        if (windowFunctionExpr.Spec.Frame is null)
        {
            if (windowFunctionExpr.Spec.OrderBy.Count == 0)
            {
                mapAllRows(
                    map,
                    part,
                    countAllRows
                        ? CountWholePartition(part)
                        : CountWholePartition(context, part, countArg!, ctes, eval, valueSelector, countDistinct));
                return;
            }

            FillOrderedCount(
                partitionContext,
                map,
                countArg!,
                ctes,
                eval,
                valueSelector,
                countAllRows,
                countDistinct);
            return;
        }

        if (partitionContext.CoversWholePartition())
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

        static void FillOrderedCount(
            WindowPartitionExecutionContext partitionContext,
            Dictionary<EvalRow, object?> target,
            SqlExpr countArg,
            IDictionary<string, Source> ctes,
            Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
            Func<EvalRow, object?>? valueSelector,
            bool countAllRows,
            bool countDistinct)
        {
            var part = partitionContext.Part;
            var valuesByPeerEnd = new Dictionary<int, long>(Math.Max(1, part.Count));

            foreach (var peerGroup in partitionContext.GetPeerGroups())
            {
                if (!valuesByPeerEnd.TryGetValue(peerGroup.End, out var value))
                {
                    value = countAllRows
                        ? peerGroup.End + 1L
                        : CountFrame(
                            partitionContext.QueryExecutionContext,
                            new RowsFrameRange(0, peerGroup.End, false),
                            part,
                            countArg,
                            ctes,
                            eval,
                            valueSelector,
                            countDistinct);
                    valuesByPeerEnd[peerGroup.End] = value;
                }

                for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                    target[part[i]] = value;
            }
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
        var partCount = part.Count;
        if (partCount == 0)
            return;

        var aggregateCall = new FunctionCallExpr(windowFunctionExpr.Name, windowFunctionExpr.Args, windowFunctionExpr.Distinct);
        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var wholePartitionValue = partitionContext.QueryExecutionContext.EvalAggregate(
                aggregateCall,
                new EvalGroup(part),
                ctes,
                eval);

            foreach (var row in part)
                map[row] = wholePartitionValue;

            return;
        }

        for (var i = 0; i < partCount; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            var frameRows = frameRange.IsEmpty
                ? []
                : part.GetRange(frameRange.StartIndex, frameRange.EndIndex - frameRange.StartIndex + 1);

            map[part[i]] = partitionContext.QueryExecutionContext.EvalAggregate(
                aggregateCall,
                new EvalGroup(frameRows),
                ctes,
                eval);
        }
    }
}
