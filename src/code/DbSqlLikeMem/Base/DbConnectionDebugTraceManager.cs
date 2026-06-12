namespace DbSqlLikeMem;

/// <summary>
/// EN: Stores and trims the debug traces captured during query execution.
/// PT-br: Guarda e ajusta os traces de debug capturados durante a execucao da query.
/// </summary>
internal sealed class DbConnectionDebugTraceManager
{
    private readonly object _traceLock = new();
    private readonly List<QueryDebugTrace> _lastDebugTraces = [];
    private int _debugTraceCaptureDepth;
    private QueryDebugTrace? _lastDebugTrace;

    /// <summary>
    /// EN: Gets the last captured debug trace.
    /// PT-br: Obtem o ultimo trace de debug capturado.
    /// </summary>
    public QueryDebugTrace? LastDebugTrace
    {
        get
        {
            lock (_traceLock)
                return _lastDebugTrace;
        }
    }

    /// <summary>
    /// EN: Gets the retained debug trace history.
    /// PT-br: Obtem o historico de traces de debug retido.
    /// </summary>
    public IReadOnlyList<QueryDebugTrace> LastDebugTraces
    {
        get
        {
            lock (_traceLock)
                return _lastDebugTraces.ToArray();
        }
    }

    /// <summary>
    /// EN: Gets or sets the maximum number of debug traces kept in memory.
    /// PT-br: Obtem ou define o numero maximo de traces de debug mantidos em memoria.
    /// </summary>
    public int DebugTraceRetentionLimit { get; set; } = int.MaxValue;

    /// <summary>
    /// EN: Gets whether debug trace capture is active.
    /// PT-br: Obtem se a captura de trace de debug esta ativa.
    /// </summary>
    public bool IsDebugTraceCaptureEnabled => System.Threading.Volatile.Read(ref _debugTraceCaptureDepth) > 0;

    /// <summary>
    /// EN: Clears the current trace state and history.
    /// PT-br: Limpa o estado atual e o historico de traces.
    /// </summary>
    public void Clear()
    {
        lock (_traceLock)
        {
            _lastDebugTrace = null;
            _lastDebugTraces.Clear();
        }
    }

    /// <summary>
    /// EN: Starts a capture scope for debug traces.
    /// PT-br: Inicia um escopo de captura para traces de debug.
    /// </summary>
    public IDisposable BeginCapture()
    {
        lock (_traceLock)
        {
            if (_debugTraceCaptureDepth == 0)
            {
                _lastDebugTrace = null;
                _lastDebugTraces.Clear();
            }

            _debugTraceCaptureDepth++;
        }

        return new DebugTraceCaptureScope(this);
    }

    /// <summary>
    /// EN: Registers one debug trace in the retained history.
    /// PT-br: Registra um trace de debug no historico retido.
    /// </summary>
    public void Register(QueryDebugTrace trace)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(trace, nameof(trace));
        lock (_traceLock)
        {
            _lastDebugTrace = trace;
            _lastDebugTraces.Add(trace);
            TrimHistoryIfNeeded();
        }
    }

    /// <summary>
    /// EN: Restores the retained debug trace history from a snapshot.
    /// PT-br: Restaura o historico de traces de debug retido a partir de um snapshot.
    /// </summary>
    public void Restore(IReadOnlyList<QueryDebugTrace> traces)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(traces, nameof(traces));
        lock (_traceLock)
        {
            _lastDebugTraces.Clear();
            _lastDebugTraces.AddRange(traces);
            TrimHistoryIfNeeded();
            _lastDebugTrace = _lastDebugTraces.Count > 0 ? _lastDebugTraces[^1] : null;
        }
    }

    /// <summary>
    /// EN: Aligns trace statements with the SQL text being executed.
    /// PT-br: Alinha os traces com o texto SQL que esta sendo executado.
    /// </summary>
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

        lock (_traceLock)
        {
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

            _lastDebugTraces.Clear();
            _lastDebugTraces.AddRange(contextualized);
            TrimHistoryIfNeeded();
            _lastDebugTrace = _lastDebugTraces.Count > 0 ? _lastDebugTraces[^1] : null;
        }
    }

    /// <summary>
    /// EN: Creates a snapshot of the retained trace history.
    /// PT-br: Cria um snapshot do historico de traces retido.
    /// </summary>
    public IReadOnlyList<QueryDebugTrace> Snapshot()
    {
        lock (_traceLock)
            return [.. _lastDebugTraces];
    }

    private void TrimHistoryIfNeeded()
    {
        var retentionLimit = Math.Max(1, DebugTraceRetentionLimit);

        if (_lastDebugTraces.Count <= retentionLimit)
            return;

        var removeCount = _lastDebugTraces.Count - retentionLimit;
        _lastDebugTraces.RemoveRange(0, removeCount);
        _lastDebugTrace = _lastDebugTraces[^1];
    }

    private sealed class DebugTraceCaptureScope(DbConnectionDebugTraceManager manager) : IDisposable
    {
        private DbConnectionDebugTraceManager? _manager = manager;

        public void Dispose()
        {
            if (_manager is null)
                return;

            lock (_manager._traceLock)
                _manager._debugTraceCaptureDepth = Math.Max(0, _manager._debugTraceCaptureDepth - 1);
            _manager = null;
        }
    }
}
