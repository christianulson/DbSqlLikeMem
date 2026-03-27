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
    private RowsFrameRange[]? _frameRangesByRow;
    private List<(int Start, int End)>? _peerGroups;
    private bool? _coversWholePartition;
    internal QueryExecutionContext QueryExecutionContext => context;

    internal List<EvalRow> Part { get; } = part;

    internal Dictionary<EvalRow, object?[]> GetRequiredOrderValuesByRow()
    {
        _orderValuesByRow ??= WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            Part,
            _spec.OrderBy,
            (expr, row) => _eval(expr, row, null, _ctes));
        return _orderValuesByRow;
    }

    internal RowsFrameRange GetFrameRange(int rowIndex)
    {
        if (_frameRangesByRow is null)
        {
            _frameRangesByRow = new RowsFrameRange[Part.Count];
            var needsOrderValues = _spec.Frame is not null
                && _spec.Frame.Unit != WindowFrameUnit.Rows
                && _spec.OrderBy.Count > 0;
            var orderValuesByRow = needsOrderValues ? GetRequiredOrderValuesByRow() : null;
            for (var i = 0; i < Part.Count; i++)
            {
                _frameRangesByRow[i] = AstQueryWindowFrameHelper.ResolveWindowFrameRange(
                    this,
                    _spec.Frame,
                    Part,
                    i,
                    _spec.OrderBy,
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

        if (Part.Count == 0)
        {
            _coversWholePartition = false;
            return false;
        }

        for (var i = 0; i < Part.Count; i++)
        {
            var frameRange = GetFrameRange(i);
            if (frameRange.IsEmpty || frameRange.StartIndex != 0 || frameRange.EndIndex != Part.Count - 1)
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

        var peerGroups = new List<(int Start, int End)>();
        if (Part.Count == 0)
            return _peerGroups = peerGroups;

        if (Part.Count == 1)
            return _peerGroups = [(0, 0)];

        var orderValuesByRow = GetRequiredOrderValuesByRow();
        var start = 0;
        for (var i = 1; i <= Part.Count; i++)
        {
            var isBoundary = i == Part.Count
                || !this.WindowOrderValuesEqual(
                    orderValuesByRow[Part[i - 1]],
                    orderValuesByRow[Part[i]]);
            if (!isBoundary)
                continue;

            peerGroups.Add((start, i - 1));
            start = i;
        }

        _peerGroups = peerGroups;
        return _peerGroups;
    }
}
