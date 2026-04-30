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
        var partCount = partitionContext.PartCount;
        var args = windowFunctionExpr.Args;
        var argsCount = args.Count;
        if (partCount == 0 || argsCount == 0)
            return;

        var valueExpr = args[0];
        var hasValueSelector = valueSelector is not null;
        if (partitionContext.CoversWholePartition())
        {
            var targetRow = part[fillLast ? partCount - 1 : 0];
            var value = hasValueSelector
                ? valueSelector!(targetRow)
                : eval(valueExpr, targetRow, null, ctes);
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, partCount));
        if (hasValueSelector)
        {
            for (var i = 0; i < partCount; i++)
            {
                var currentRow = part[i];
                var frameRange = partitionContext.GetFrameRange(i);
                if (frameRange.IsEmpty)
                {
                    map[currentRow] = null;
                    continue;
                }

                var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
                if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
                {
                    var targetRow = part[targetIndex];
                    value = valueSelector!(targetRow);
                    valuesByTargetIndex[targetIndex] = value;
                }

                map[currentRow] = value;
            }

            return;
        }

        for (var i = 0; i < partCount; i++)
        {
            var currentRow = part[i];
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[currentRow] = null;
                continue;
            }

            var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                var targetRow = part[targetIndex];
                value = eval(valueExpr, targetRow, null, ctes);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[currentRow] = value;
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
        var partCount = partitionContext.PartCount;
        var args = windowFunctionExpr.Args;
        var argsCount = args.Count;
        if (partCount == 0 || argsCount == 0)
            return;

        var valueExpr = args[0];
        var hasValueSelector = valueSelector is not null;
        var firstRow = part[0];
        var nth = AstQueryWindowFunctionSupport.ResolveNthValueIndex(args, firstRow, ctes, eval);
        if (nth <= 0)
            return;

        if (partitionContext.CoversWholePartition())
        {
            var targetIndex = nth - 1;
            object? value = null;
            if (targetIndex < partCount)
            {
                value = hasValueSelector
                    ? valueSelector!(part[targetIndex])
                    : eval(valueExpr, part[targetIndex], null, ctes);
            }

            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, partCount));
        if (hasValueSelector)
        {
            for (var i = 0; i < partCount; i++)
            {
                var currentRow = part[i];
                var frameRange = partitionContext.GetFrameRange(i);
                if (frameRange.IsEmpty)
                {
                    map[currentRow] = null;
                    continue;
                }

                var targetIndex = frameRange.StartIndex + (nth - 1);
                if (targetIndex > frameRange.EndIndex)
                {
                    map[currentRow] = null;
                    continue;
                }

                if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
                {
                    var targetRow = part[targetIndex];
                    value = valueSelector!(targetRow);
                    valuesByTargetIndex[targetIndex] = value;
                }

                map[currentRow] = value;
            }

            return;
        }

        for (var i = 0; i < partCount; i++)
        {
            var currentRow = part[i];
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[currentRow] = null;
                continue;
            }

            var targetIndex = frameRange.StartIndex + (nth - 1);
            if (targetIndex > frameRange.EndIndex)
            {
                map[currentRow] = null;
                continue;
            }

            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                var targetRow = part[targetIndex];
                value = eval(valueExpr, targetRow, null, ctes);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[currentRow] = value;
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
        var partCount = partitionContext.PartCount;
        var args = windowFunctionExpr.Args;
        var argsCount = args.Count;
        if (partCount == 0 || argsCount == 0)
            return;

        var valueExpr = args[0];
        var hasValueSelector = valueSelector is not null;
        var firstRow = part[0];
        var offset = AstQueryWindowFunctionSupport.ResolveLagLeadOffset(args, firstRow, ctes, eval);
        var defaultExpr = argsCount >= 3 ? args[2] : null;
        var hasDefaultExpr = defaultExpr is not null;

        if (offset == 0)
        {
            if (hasValueSelector)
            {
                foreach (var currentRow in part)
                    map[currentRow] = valueSelector!(currentRow);
            }
            else
            {
                foreach (var currentRow in part)
                    map[currentRow] = eval(valueExpr, currentRow, null, ctes);
            }

            return;
        }

        if (hasValueSelector)
        {
            for (var i = 0; i < partCount; i++)
            {
                var currentRow = part[i];
                var targetIndex = fillLead ? i + offset : i - offset;
                if (targetIndex >= 0 && targetIndex < partCount)
                {
                    map[currentRow] = valueSelector!(part[targetIndex]);
                    continue;
                }

                map[currentRow] = hasDefaultExpr ? eval(defaultExpr!, currentRow, null, ctes) : null;
            }

            return;
        }

        for (var i = 0; i < partCount; i++)
        {
            var currentRow = part[i];
            var targetIndex = fillLead ? i + offset : i - offset;
            if (targetIndex >= 0 && targetIndex < partCount)
            {
                var targetRow = part[targetIndex];
                map[currentRow] = eval(valueExpr, targetRow, null, ctes);
                continue;
            }

            map[currentRow] = hasDefaultExpr ? eval(defaultExpr!, currentRow, null, ctes) : null;
        }
    }

    internal static void FillRankOrDenseRank(
        this WindowPartitionExecutionContext partitionContext,
        Dictionary<EvalRow, object?> map,
        Func<object?, object?, int> compareSql,
        bool fillRank)
    {
        var part = partitionContext.Part;
        var partCount = partitionContext.PartCount;
        if (partCount == 0)
            return;

        var denseRank = 1L;
        foreach (var peerGroup in partitionContext.GetPeerGroups())
        {
            var start = peerGroup.Start;
            var end = peerGroup.End;
            var value = fillRank ? start + 1L : denseRank;
            for (var i = start; i <= end; i++)
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
        var partCount = partitionContext.PartCount;
        if (partCount == 0)
            return;

        var percentRankDenominator = partCount > 1 ? partCount - 1 : 1;
        foreach (var peerGroup in partitionContext.GetPeerGroups())
        {
            var start = peerGroup.Start;
            var end = peerGroup.End;
            var peerCount = end - start + 1;
            var value = fillPercentRank
                ? partCount <= 1 ? 0d : (double)start / percentRankDenominator
                : (double)(start + peerCount) / partCount;

            for (var i = start; i <= end; i++)
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
        var partCount = partitionContext.PartCount;
        if (partCount == 0)
            return;

        var firstRow = part[0];
        var bucketCount = AstQueryWindowFunctionSupport.ResolveNtileBucketCount(windowFunctionExpr, partCount, firstRow, ctes, eval);
        if (bucketCount <= 0)
            return;

        for (var rowIndex = 0; rowIndex < partCount; rowIndex++)
        {
            var currentRow = part[rowIndex];
            map[currentRow] = (rowIndex * bucketCount) / partCount + 1;
        }
    }
}
