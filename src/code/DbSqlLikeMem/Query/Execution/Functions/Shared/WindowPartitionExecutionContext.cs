using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class WindowPartitionExecutionContext(
    QueryExecutionContext context,
    List<EvalRow> part,
    WindowSpec spec,
    IDictionary<string, Source> ctes,
    Dictionary<EvalRow, object?[]>? precomputedOrderValuesByRow,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
{
    private readonly WindowSpec _spec = spec;
    private readonly IDictionary<string, Source> _ctes = ctes;
    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval = eval;
    private Dictionary<EvalRow, object?[]>? _orderValuesByRow = precomputedOrderValuesByRow;
    private Dictionary<EvalRow, object?>? _singleOrderValueByRow;
    private RowsFrameRange[]? _frameRangesByRow;
    private List<(int Start, int End)>? _peerGroups;
    private bool? _coversWholePartition;
    internal QueryExecutionContext QueryExecutionContext => context;

    internal List<EvalRow> Part { get; } = part;
    internal int PartCount => Part.Count;

    internal Dictionary<EvalRow, object?[]> GetRequiredOrderValuesByRow()
    {
        var part = Part;
        _orderValuesByRow ??= WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            part,
            _spec.OrderBy,
            (expr, row) => _eval(expr, row, null, _ctes));
        return _orderValuesByRow;
    }

    internal Dictionary<EvalRow, object?> GetRequiredSingleOrderValueByRow()
    {
        var part = Part;
        if (_singleOrderValueByRow is not null)
            return _singleOrderValueByRow;

        var singleValues = new Dictionary<EvalRow, object?>(Math.Max(1, part.Count), ReferenceEqualityComparer<EvalRow>.Instance);
        var orderExpr = _spec.OrderBy[0].Expr;
        for (var i = 0; i < part.Count; i++)
        {
            var row = part[i];
            singleValues[row] = _eval(orderExpr, row, null, _ctes);
        }

        _singleOrderValueByRow = singleValues;
        return _singleOrderValueByRow;
    }

    internal RowsFrameRange GetFrameRange(int rowIndex)
    {
        if (_frameRangesByRow is null)
        {
            var part = Part;
            var partCount = part.Count;
            var frame = _spec.Frame;
            var orderBy = _spec.OrderBy;
            _frameRangesByRow = new RowsFrameRange[partCount];
            var needsOrderValues = frame is not null
                && frame.Unit != WindowFrameUnit.Rows
                && orderBy.Count > 0;
            var orderValuesByRow = needsOrderValues
                ? orderBy.Count == 1
                    ? GetRequiredSingleOrderValueByRow()
                    : null
                : null;
            for (var i = 0; i < partCount; i++)
            {
                _frameRangesByRow[i] = AstQueryWindowFrameHelper.ResolveWindowFrameRange(
                    this,
                    frame,
                    part,
                    i,
                    orderBy,
                    _ctes,
                    _eval,
                    orderValuesByRow);
            }
        }

        return _frameRangesByRow[rowIndex];
    }

    internal bool CoversWholePartition()
    {
        if (_coversWholePartition.HasValue)
            return _coversWholePartition.Value;

        var part = Part;
        var partCount = part.Count;
        if (partCount == 0)
        {
            _coversWholePartition = false;
            return false;
        }

        var lastIndex = partCount - 1;
        if (_frameRangesByRow is null)
            GetFrameRange(0);

        var frameRangesByRow = _frameRangesByRow!;
        for (var i = 0; i < partCount; i++)
        {
            var frameRange = frameRangesByRow[i];
            if (frameRange.IsEmpty || frameRange.StartIndex != 0 || frameRange.EndIndex != lastIndex)
            {
                _coversWholePartition = false;
                return false;
            }
        }

        _coversWholePartition = true;
        return true;
    }

    internal List<(int Start, int End)> GetPeerGroups()
    {
        if (_peerGroups is not null)
            return _peerGroups;

        var part = Part;
        var partCount = part.Count;
        var peerGroups = new List<(int Start, int End)>(partCount);
        if (partCount == 0)
            return _peerGroups = peerGroups;

        if (partCount == 1)
            return _peerGroups = [(0, 0)];

        if (_spec.OrderBy.Count == 1)
            return _peerGroups = BuildSingleOrderPeerGroups(part, GetRequiredSingleOrderValueByRow());

        var orderValuesByRow = GetRequiredOrderValuesByRow();

        var start = 0;
        var previousValues = orderValuesByRow[part[0]];
        for (var i = 1; i < partCount; i++)
        {
            var currentRow = part[i];
            var currentValues = orderValuesByRow[currentRow];
            if (this.WindowOrderValuesEqual(previousValues, currentValues))
            {
                previousValues = currentValues;
                continue;
            }

            peerGroups.Add((start, i - 1));
            start = i;
            previousValues = currentValues;
        }

        peerGroups.Add((start, partCount - 1));
        _peerGroups = peerGroups;
        return _peerGroups;
    }

    private List<(int Start, int End)> BuildSingleOrderPeerGroups(
        List<EvalRow> part,
        Dictionary<EvalRow, object?> orderValuesByRow)
    {
        var partCount = part.Count;
        var peerGroups = new List<(int Start, int End)>(partCount);
        var compareSql = QueryExecutionContext.CompareSql;
        var start = 0;
        var previousValue = orderValuesByRow[part[0]];

        for (var i = 1; i < partCount; i++)
        {
            var currentRow = part[i];
            var currentValue = orderValuesByRow[currentRow];
            if (compareSql(previousValue, currentValue) == 0)
            {
                previousValue = currentValue;
                continue;
            }

            peerGroups.Add((start, i - 1));
            start = i;
            previousValue = currentValue;
        }

        peerGroups.Add((start, partCount - 1));
        return peerGroups;
    }
}
