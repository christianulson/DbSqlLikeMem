namespace DbSqlLikeMem;

internal sealed class DbConnectionDebugTraceManager
{
    private readonly List<QueryDebugTrace> _lastDebugTraces = [];
    private int _debugTraceCaptureDepth;

    public QueryDebugTrace? LastDebugTrace { get; private set; }

    public IReadOnlyList<QueryDebugTrace> LastDebugTraces => _lastDebugTraces;

    public int DebugTraceRetentionLimit { get; set; } = int.MaxValue;

    public bool IsDebugTraceCaptureEnabled => _debugTraceCaptureDepth > 0;

    public void Clear()
    {
        LastDebugTrace = null;
        _lastDebugTraces.Clear();
    }

    public IDisposable BeginCapture()
    {
        if (_debugTraceCaptureDepth == 0)
            Clear();

        _debugTraceCaptureDepth++;
        return new DebugTraceCaptureScope(this);
    }

    public void Register(QueryDebugTrace trace)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(trace, nameof(trace));
        LastDebugTrace = trace;
        _lastDebugTraces.Add(trace);
        TrimHistoryIfNeeded();
    }

    public void Restore(IReadOnlyList<QueryDebugTrace> traces)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(traces, nameof(traces));
        _lastDebugTraces.Clear();
        _lastDebugTraces.AddRange(traces);
        TrimHistoryIfNeeded();
        LastDebugTrace = _lastDebugTraces.Count > 0 ? _lastDebugTraces[^1] : null;
    }

    public void Contextualize(string sql, ISqlDialect executionDialect)
    {
        var statements = new List<string>(4);
        foreach (var statement in SqlQueryParser.SplitStatements(sql, executionDialect))
        {
            if (string.IsNullOrWhiteSpace(statement))
                continue;

            var trimmed = statement.AsSpan().Trim();
            statements.Add(trimmed.Length == statement.Length ? statement : trimmed.ToString());
        }

        if (statements.Count == 0 || _lastDebugTraces.Count == 0)
            return;

        var contextualized = new List<QueryDebugTrace>(_lastDebugTraces.Count);
        var statementOffset = Math.Max(0, statements.Count - _lastDebugTraces.Count);
        for (var i = 0; i < _lastDebugTraces.Count; i++)
        {
            var statementIndex = statementOffset + i;
            var sqlText = statementIndex < statements.Count
                ? statements[statementIndex]
                : null;
            contextualized.Add(_lastDebugTraces[i].WithStatementContext(statementIndex, sqlText));
        }

        Restore(contextualized);
    }

    public IReadOnlyList<QueryDebugTrace> Snapshot()
        => [.. _lastDebugTraces];

    private void TrimHistoryIfNeeded()
    {
        var retentionLimit = Math.Max(1, DebugTraceRetentionLimit);

        if (_lastDebugTraces.Count <= retentionLimit)
            return;

        var removeCount = _lastDebugTraces.Count - retentionLimit;
        _lastDebugTraces.RemoveRange(0, removeCount);
        LastDebugTrace = _lastDebugTraces[^1];
    }

    private sealed class DebugTraceCaptureScope(DbConnectionDebugTraceManager manager) : IDisposable
    {
        private DbConnectionDebugTraceManager? _manager = manager;

        public void Dispose()
        {
            if (_manager is null)
                return;

            _manager._debugTraceCaptureDepth = Math.Max(0, _manager._debugTraceCaptureDepth - 1);
            _manager = null;
        }
    }
}
