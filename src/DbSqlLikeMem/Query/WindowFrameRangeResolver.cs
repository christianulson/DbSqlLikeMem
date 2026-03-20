namespace DbSqlLikeMem;

internal static class WindowFrameRangeResolver
{
    internal static RowsFrameRange ResolveRowsFrameRange(WindowFrameSpec? frame, int partitionSize, int rowIndex)
    {
        if (partitionSize <= 0)
            return RowsFrameRange.Empty;

        if (frame is null)
            return new RowsFrameRange(0, partitionSize - 1, IsEmpty: false);

        var lastIndex = partitionSize - 1;
        var rawStartIndex = ResolveRowsFrameBoundIndex(frame.Start, rowIndex, partitionSize, isStartBound: true);
        var rawEndIndex = ResolveRowsFrameBoundIndex(frame.End, rowIndex, partitionSize, isStartBound: false);

        if (rawStartIndex > rawEndIndex)
            return RowsFrameRange.Empty;

        var startIndex = Math.Max(rawStartIndex, 0);
        var endIndex = Math.Min(rawEndIndex, lastIndex);
        if (startIndex > endIndex)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(startIndex, endIndex, IsEmpty: false);
    }

    internal static RowsFrameRange Resolve<T>(
        WindowFrameSpec frame,
        List<T> partition,
        int rowIndex,
        IReadOnlyList<WindowOrderItem> orderBy,
        Dictionary<T, object?[]> orderValuesByRow,
        Func<object?[], object?[], bool> windowOrderValuesEqual)
        where T : notnull
        => frame.Unit switch
        {
            WindowFrameUnit.Groups => ResolveGroupsFrameRange(frame, partition, rowIndex, orderValuesByRow, windowOrderValuesEqual),
            WindowFrameUnit.Range => ResolveRangeFrameRange(frame, partition, rowIndex, orderValuesByRow, orderBy, windowOrderValuesEqual),
            _ => ResolveRowsFrameRange(frame, partition.Count, rowIndex)
        };

    private static int ResolveRowsFrameBoundIndex(WindowFrameBound bound, int rowIndex, int partitionSize, bool isStartBound)
    {
        var lastIndex = partitionSize - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => lastIndex,
            WindowFrameBoundKind.CurrentRow => rowIndex,
            WindowFrameBoundKind.Preceding => rowIndex - bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.Following => rowIndex + bound.Offset.GetValueOrDefault(),
            _ => isStartBound ? 0 : lastIndex
        };
    }

    private static RowsFrameRange ResolveGroupsFrameRange<T>(
        WindowFrameSpec frame,
        List<T> partition,
        int rowIndex,
        Dictionary<T, object?[]> orderValuesByRow,
        Func<object?[], object?[], bool> windowOrderValuesEqual)
        where T : notnull
    {
        var groups = BuildPeerGroups(partition, orderValuesByRow, windowOrderValuesEqual);
        var currentGroupIndex = groups.FindIndex(group => rowIndex >= group.Start && rowIndex <= group.End);
        if (currentGroupIndex < 0)
            return RowsFrameRange.Empty;

        var startGroup = ResolveGroupsBoundIndex(frame.Start, currentGroupIndex, groups.Count, isStartBound: true);
        var endGroup = ResolveGroupsBoundIndex(frame.End, currentGroupIndex, groups.Count, isStartBound: false);
        if (startGroup > endGroup)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(groups[startGroup].Start, groups[endGroup].End, IsEmpty: false);
    }

    private static RowsFrameRange ResolveRangeFrameRange<T>(
        WindowFrameSpec frame,
        List<T> partition,
        int rowIndex,
        Dictionary<T, object?[]> orderValuesByRow,
        IReadOnlyList<WindowOrderItem> orderBy,
        Func<object?[], object?[], bool> windowOrderValuesEqual)
        where T : notnull
    {
        var hasOffsetBound = frame.Start.Kind is WindowFrameBoundKind.Preceding or WindowFrameBoundKind.Following
            || frame.End.Kind is WindowFrameBoundKind.Preceding or WindowFrameBoundKind.Following;

        ValidateRangeOffsetOrderBy(orderBy, hasOffsetBound);

        var peerRange = ResolvePeerRange(partition, rowIndex, orderValuesByRow, windowOrderValuesEqual);

        int startIndex;
        int endIndex;
        if (hasOffsetBound)
        {
            var scalarValues = BuildRangeScalarValues(partition, orderValuesByRow, orderBy);
            var current = scalarValues[rowIndex];
            startIndex = ResolveRangeBoundIndex(frame.Start, scalarValues, current, peerRange, isStartBound: true);
            endIndex = ResolveRangeBoundIndex(frame.End, scalarValues, current, peerRange, isStartBound: false);
        }
        else
        {
            startIndex = ResolveRangeBoundIndexWithoutOffsets(frame.Start, partition.Count, peerRange, isStartBound: true);
            endIndex = ResolveRangeBoundIndexWithoutOffsets(frame.End, partition.Count, peerRange, isStartBound: false);
        }

        if (startIndex > endIndex)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(startIndex, endIndex, IsEmpty: false);
    }

    private static void ValidateRangeOffsetOrderBy(IReadOnlyList<WindowOrderItem> orderBy, bool hasOffsetBound)
    {
        if (!hasOffsetBound)
            return;

        if (orderBy.Count != 1)
            throw new InvalidOperationException("RANGE with PRECEDING/FOLLOWING offset requires exactly one ORDER BY expression.");
    }

    private static List<(int Start, int End)> BuildPeerGroups<T>(
        List<T> partition,
        Dictionary<T, object?[]> orderValuesByRow,
        Func<object?[], object?[], bool> windowOrderValuesEqual)
        where T : notnull
    {
        var groups = new List<(int Start, int End)>();
        var start = 0;
        for (var i = 1; i <= partition.Count; i++)
        {
            var isBoundary = i == partition.Count
                || !windowOrderValuesEqual(orderValuesByRow[partition[i - 1]], orderValuesByRow[partition[i]]);
            if (!isBoundary)
                continue;

            groups.Add((start, i - 1));
            start = i;
        }

        return groups;
    }

    private static int ResolveGroupsBoundIndex(WindowFrameBound bound, int currentGroupIndex, int groupCount, bool isStartBound)
    {
        var last = groupCount - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => currentGroupIndex,
            WindowFrameBoundKind.Preceding => (currentGroupIndex - bound.Offset.GetValueOrDefault()).Clamp(0, last),
            WindowFrameBoundKind.Following => (currentGroupIndex + bound.Offset.GetValueOrDefault()).Clamp(0, last),
            _ => isStartBound ? 0 : last
        };
    }

    private static (int Start, int End) ResolvePeerRange<T>(
        List<T> partition,
        int rowIndex,
        Dictionary<T, object?[]> orderValuesByRow,
        Func<object?[], object?[], bool> windowOrderValuesEqual)
        where T : notnull
    {
        var current = orderValuesByRow[partition[rowIndex]];
        var start = rowIndex;
        while (start > 0 && windowOrderValuesEqual(orderValuesByRow[partition[start - 1]], current))
            start--;

        var end = rowIndex;
        while (end < partition.Count - 1 && windowOrderValuesEqual(orderValuesByRow[partition[end + 1]], current))
            end++;

        return (start, end);
    }

    private static decimal[] BuildRangeScalarValues<T>(
        List<T> partition,
        Dictionary<T, object?[]> orderValuesByRow,
        IReadOnlyList<WindowOrderItem> orderBy)
        where T : notnull
    {
        var desc = orderBy.Count > 0 && orderBy[0].Desc;
        var values = new decimal[partition.Count];
        for (var i = 0; i < partition.Count; i++)
        {
            var rawOrderValue = orderValuesByRow[partition[i]].Length == 0 ? null : orderValuesByRow[partition[i]][0];
            if (!TryConvertRangeOrderToDecimal(rawOrderValue, out var scalar))
            {
                var valueType = rawOrderValue?.GetType().Name ?? SqlConst.NULL;
                throw new InvalidOperationException(
                    $"RANGE with PRECEDING/FOLLOWING offset requires numeric/date ORDER BY values. Actual ORDER BY value type: {valueType}.");
            }

            values[i] = desc ? -scalar : scalar;
        }

        return values;
    }

    private static bool TryConvertRangeOrderToDecimal(object? value, out decimal scalar)
    {
        scalar = default;
        if (value is null or DBNull)
            return false;

        try
        {
            scalar = value switch
            {
                DateTime dateTime => dateTime.Ticks,
                DateTimeOffset dateTimeOffset => dateTimeOffset.Ticks,
                TimeSpan timeSpan => timeSpan.Ticks,
                _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture)
            };
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static int ResolveRangeBoundIndexWithoutOffsets(
        WindowFrameBound bound,
        int partitionSize,
        (int Start, int End) peerRange,
        bool isStartBound)
    {
        var last = partitionSize - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => isStartBound ? peerRange.Start : peerRange.End,
            _ => isStartBound ? 0 : last
        };
    }

    private static int ResolveRangeBoundIndex(
        WindowFrameBound bound,
        decimal[] scalarValues,
        decimal currentScalar,
        (int Start, int End) peerRange,
        bool isStartBound)
    {
        var last = scalarValues.Length - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => isStartBound ? peerRange.Start : peerRange.End,
            WindowFrameBoundKind.Preceding => isStartBound
                ? FirstIndexGreaterOrEqual(scalarValues, currentScalar - bound.Offset.GetValueOrDefault())
                : LastIndexLessOrEqual(scalarValues, currentScalar - bound.Offset.GetValueOrDefault()),
            WindowFrameBoundKind.Following => isStartBound
                ? FirstIndexGreaterOrEqual(scalarValues, currentScalar + bound.Offset.GetValueOrDefault())
                : LastIndexLessOrEqual(scalarValues, currentScalar + bound.Offset.GetValueOrDefault()),
            _ => isStartBound ? 0 : last
        };
    }

    private static int FirstIndexGreaterOrEqual(decimal[] sortedValues, decimal threshold)
    {
        for (var i = 0; i < sortedValues.Length; i++)
        {
            if (sortedValues[i] >= threshold)
                return i;
        }

        return sortedValues.Length;
    }

    private static int LastIndexLessOrEqual(decimal[] sortedValues, decimal threshold)
    {
        for (var i = sortedValues.Length - 1; i >= 0; i--)
        {
            if (sortedValues[i] <= threshold)
                return i;
        }

        return -1;
    }
}
