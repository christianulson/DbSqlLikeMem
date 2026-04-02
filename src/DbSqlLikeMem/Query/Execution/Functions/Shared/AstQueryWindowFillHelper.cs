using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryWindowFillHelper
{
    internal static void FillFirstOrLastValue(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector,
        bool fillLast)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetRow = part[fillLast ? part.Count - 1 : 0];
            var value = valueSelector is null
                ? eval(valueExpr, targetRow, null, ctes)
                : valueSelector(targetRow);
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }

    internal static void FillNthValue(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var nth = AstQueryWindowFunctionSupport.ResolveNthValueIndex(windowFunctionExpr.Args, part[0], ctes, eval);
        if (nth <= 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetIndex = nth - 1;
            var value = targetIndex < part.Count
                ? valueSelector is null
                    ? eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex])
                : null;
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = frameRange.StartIndex + (nth - 1);
            if (targetIndex > frameRange.EndIndex)
            {
                map[part[i]] = null;
                continue;
            }

            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }

    internal static void FillLagOrLead(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<EvalRow, object?>? valueSelector,
        bool fillLead)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var offset = AstQueryWindowFunctionSupport.ResolveLagLeadOffset(windowFunctionExpr.Args, part[0], ctes, eval);
        var defaultExpr = windowFunctionExpr.Args.Count >= 3 ? windowFunctionExpr.Args[2] : null;

        if (offset == 0)
        {
            foreach (var currentRow in part)
            {
                map[currentRow] = valueSelector is null
                    ? eval(valueExpr, currentRow, null, ctes)
                    : valueSelector(currentRow);
            }

            return;
        }

        for (var i = 0; i < part.Count; i++)
        {
            var targetIndex = fillLead ? i + offset : i - offset;
            var currentRow = part[i];
            map[currentRow] = targetIndex >= 0 && targetIndex < part.Count
                ? valueSelector is null
                    ? eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex])
                : defaultExpr is null ? null : eval(defaultExpr, currentRow, null, ctes);
        }
    }

    internal static void FillRankOrDenseRank(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        Func<object?, object?, int> compareSql,
        bool fillRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        var denseRank = 1L;
        foreach (var peerGroup in partitionContext.GetPeerGroups())
        {
            var value = fillRank ? peerGroup.Start + 1L : denseRank;
            for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                map[part[i]] = value;

            denseRank++;
        }
    }

    internal static void FillPercentRankOrCumeDist(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        bool fillPercentRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        foreach (var peerGroup in partitionContext.GetPeerGroups())
        {
            var peerCount = peerGroup.End - peerGroup.Start + 1;
            var value = fillPercentRank
                ? part.Count <= 1 ? 0d : (double)peerGroup.Start / (part.Count - 1)
                : (double)(peerGroup.Start + peerCount) / part.Count;

            for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                map[part[i]] = value;
        }
    }

    internal static void FillNtile(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        var bucketCount = AstQueryWindowFunctionSupport.ResolveNtileBucketCount(windowFunctionExpr, part.Count, part[0], ctes, eval);
        if (bucketCount <= 0)
            return;

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
            map[part[rowIndex]] = (rowIndex * bucketCount) / part.Count + 1;
    }
}
