using System.Collections.Concurrent;
using System.Threading;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Accumulates usage and timing metrics for in-memory DB operations.
/// PT-br: Acumula métricas de uso e tempo para operações no banco em memória.
/// </summary>
public sealed class DbMetrics
{
    /// <summary>
    /// EN: Enables or disables metric collection for this instance without changing SQL behavior.
    /// PT-br: Habilita ou desabilita a coleta de metricas desta instancia sem alterar o comportamento SQL.
    /// </summary>
    public bool Enabled { get; set; } = true;

    private int _selects;
    private int _inserts;
    private int _updates;
    private int _deletes;
    private int _nonQueryStatements;
    private int _nonQueryParseCacheHits;
    private int _nonQueryParseCacheMisses;
    private int _indexLookups;
    private int _nonQueryExceptions;
    private int _nonQueryUnhandledStatements;
    private int _readerProcessedStatements;
    private int _readerControlStatements;
    private int _readerCallStatements;
    private int _readerStoredProcedureStatements;
    private int _readerResultTables;
    private int _readerRowsReturned;
    private int _readerWithoutSelectErrors;
    private int _batchNonQueryCommands;
    private int _batchReaderCommands;
    private int _batchScalarCommands;
    private int _batchReaderFallbackToNonQuery;
    private int _batchMaterializations;
    private int _batchResultTables;
    private int _batchRowsReturned;
    private int _batchExceptions;
    private int _batchCancellations;
    private int _batchEmptyNonQueryExecutions;
    private int _batchEmptyReaderExecutions;
    private int _batchEmptyScalarExecutions;

    /// <summary>
    /// EN: Number of SELECT queries executed.
    /// PT-br: Quantidade de consultas SELECT executadas.
    /// </summary>
    public int Selects
    {
        get => _selects;
        internal set
        {
            if (Enabled)
                _selects = value;
        }
    }
    /// <summary>
    /// EN: Number of INSERT operations executed.
    /// PT-br: Quantidade de operações INSERT executadas.
    /// </summary>
    public int Inserts
    {
        get => _inserts;
        internal set
        {
            if (Enabled)
                _inserts = value;
        }
    }
    /// <summary>
    /// EN: Number of UPDATE operations executed.
    /// PT-br: Quantidade de operações UPDATE executadas.
    /// </summary>
    public int Updates
    {
        get => _updates;
        internal set
        {
            if (Enabled)
                _updates = value;
        }
    }
    /// <summary>
    /// EN: Number of DELETE operations executed.
    /// PT-br: Quantidade de operações DELETE executadas.
    /// </summary>
    public int Deletes
    {
        get => _deletes;
        internal set
        {
            if (Enabled)
                _deletes = value;
        }
    }
    /// <summary>
    /// EN: Number of non-query statements executed by command pipeline.
    /// PT-br: Quantidade de statements non-query executados pelo pipeline de comando.
    /// </summary>
    public int NonQueryStatements
    {
        get => _nonQueryStatements;
        internal set
        {
            if (Enabled)
                _nonQueryStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of parse cache hits inside non-query pipeline context.
    /// PT-br: Quantidade de acertos de cache de parse no contexto do pipeline non-query.
    /// </summary>
    public int NonQueryParseCacheHits
    {
        get => _nonQueryParseCacheHits;
        internal set
        {
            if (Enabled)
                _nonQueryParseCacheHits = value;
        }
    }
    /// <summary>
    /// EN: Number of parse cache misses inside non-query pipeline context.
    /// PT-br: Quantidade de perdas de cache de parse no contexto do pipeline non-query.
    /// </summary>
    public int NonQueryParseCacheMisses
    {
        get => _nonQueryParseCacheMisses;
        internal set
        {
            if (Enabled)
                _nonQueryParseCacheMisses = value;
        }
    }
    /// <summary>
    /// EN: Number of index lookups used to pre-filter rows during SELECT.
    /// PT-br: Quantidade de consultas em índice usadas para pré-filtrar linhas em SELECT.
    /// </summary>
    public int IndexLookups
    {
        get => _indexLookups;
        internal set
        {
            if (Enabled)
                _indexLookups = value;
        }
    }
    /// <summary>
    /// EN: Number of calls per index name during index lookup flow.
    /// PT-br: Quantidade de chamadas por nome de índice durante o fluxo de lookup.
    /// </summary>
    public ConcurrentDictionary<string, int> IndexHints { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of accesses per table name during query source resolution.
    /// PT-br: Quantidade de acessos por nome de tabela durante resolução de origem da consulta.
    /// </summary>
    public ConcurrentDictionary<string, int> TableHints { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of handler activations by handler name in non-query pipeline.
    /// PT-br: Quantidade de ativações por nome de handler no pipeline non-query.
    /// </summary>
    public ConcurrentDictionary<string, int> NonQueryHandlerHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks per non-query handler (handled path only).
    /// PT-br: Ticks acumulados de tempo por handler non-query (apenas caminho tratado).
    /// </summary>
    public ConcurrentDictionary<string, long> NonQueryHandlerElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of failures by non-query handler name.
    /// PT-br: Quantidade de falhas por nome de handler non-query.
    /// </summary>
    public ConcurrentDictionary<string, int> NonQueryHandlerFailures { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of non-query statements that raised exceptions in pipeline flow.
    /// PT-br: Quantidade de statements non-query que geraram exceções no fluxo do pipeline.
    /// </summary>
    public int NonQueryExceptions
    {
        get => _nonQueryExceptions;
        internal set
        {
            if (Enabled)
                _nonQueryExceptions = value;
        }
    }
    /// <summary>
    /// EN: Number of non-query statements not handled by any configured handler.
    /// PT-br: Quantidade de statements non-query não tratados por nenhum handler configurado.
    /// </summary>
    public int NonQueryUnhandledStatements
    {
        get => _nonQueryUnhandledStatements;
        internal set
        {
            if (Enabled)
                _nonQueryUnhandledStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of statements processed by ExecuteReader flows.
    /// PT-br: Quantidade de statements processados pelos fluxos de ExecuteReader.
    /// </summary>
    public int ReaderProcessedStatements
    {
        get => _readerProcessedStatements;
        internal set
        {
            if (Enabled)
                _readerProcessedStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of transaction-control statements handled in ExecuteReader flow.
    /// PT-br: Quantidade de statements de controle transacional tratados no fluxo de ExecuteReader.
    /// </summary>
    public int ReaderControlStatements
    {
        get => _readerControlStatements;
        internal set
        {
            if (Enabled)
                _readerControlStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of CALL statements handled in ExecuteReader flow.
    /// PT-br: Quantidade de statements CALL tratados no fluxo de ExecuteReader.
    /// </summary>
    public int ReaderCallStatements
    {
        get => _readerCallStatements;
        internal set
        {
            if (Enabled)
                _readerCallStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of stored procedures executed through ExecuteReader prelude.
    /// PT-br: Quantidade de procedures executadas via prelude de ExecuteReader.
    /// </summary>
    public int ReaderStoredProcedureStatements
    {
        get => _readerStoredProcedureStatements;
        internal set
        {
            if (Enabled)
                _readerStoredProcedureStatements = value;
        }
    }
    /// <summary>
    /// EN: Number of result tables produced by ExecuteReader.
    /// PT-br: Quantidade de tabelas de resultado produzidas por ExecuteReader.
    /// </summary>
    public int ReaderResultTables
    {
        get => _readerResultTables;
        internal set
        {
            if (Enabled)
                _readerResultTables = value;
        }
    }
    /// <summary>
    /// EN: Number of rows returned by ExecuteReader result tables.
    /// PT-br: Quantidade de linhas retornadas pelas tabelas de resultado de ExecuteReader.
    /// </summary>
    public int ReaderRowsReturned
    {
        get => _readerRowsReturned;
        internal set
        {
            if (Enabled)
                _readerRowsReturned = value;
        }
    }
    /// <summary>
    /// EN: Number of ExecuteReader attempts with no SELECT result after parsed statements.
    /// PT-br: Quantidade de tentativas de ExecuteReader sem resultado SELECT após statements parseados.
    /// </summary>
    public int ReaderWithoutSelectErrors
    {
        get => _readerWithoutSelectErrors;
        internal set
        {
            if (Enabled)
                _readerWithoutSelectErrors = value;
        }
    }
    /// <summary>
    /// EN: Number of parsed query dispatches by query type name in ExecuteReader flow.
    /// PT-br: Quantidade de despachos de query parseada por tipo de query no fluxo de ExecuteReader.
    /// </summary>
    public ConcurrentDictionary<string, int> ReaderQueryTypeHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteNonQuery flow.
    /// PT-br: Quantidade de comandos executados via fluxo batch ExecuteNonQuery.
    /// </summary>
    public int BatchNonQueryCommands
    {
        get => _batchNonQueryCommands;
        internal set
        {
            if (Enabled)
                _batchNonQueryCommands = value;
        }
    }
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteReader flow.
    /// PT-br: Quantidade de comandos executados via fluxo batch ExecuteReader.
    /// </summary>
    public int BatchReaderCommands
    {
        get => _batchReaderCommands;
        internal set
        {
            if (Enabled)
                _batchReaderCommands = value;
        }
    }
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteScalar flow.
    /// PT-br: Quantidade de comandos executados via fluxo batch ExecuteScalar.
    /// </summary>
    public int BatchScalarCommands
    {
        get => _batchScalarCommands;
        internal set
        {
            if (Enabled)
                _batchScalarCommands = value;
        }
    }
    /// <summary>
    /// EN: Number of batch reader commands that fell back to ExecuteNonQuery due to no SELECT result.
    /// PT-br: Quantidade de comandos batch reader que fizeram fallback para ExecuteNonQuery por ausência de resultado SELECT.
    /// </summary>
    public int BatchReaderFallbackToNonQuery
    {
        get => _batchReaderFallbackToNonQuery;
        internal set
        {
            if (Enabled)
                _batchReaderFallbackToNonQuery = value;
        }
    }
    /// <summary>
    /// EN: Number of batch command materializations performed before execution.
    /// PT-br: Quantidade de materializações de comandos batch realizadas antes da execução.
    /// </summary>
    public int BatchMaterializations
    {
        get => _batchMaterializations;
        internal set
        {
            if (Enabled)
                _batchMaterializations = value;
        }
    }
    /// <summary>
    /// EN: Number of result tables produced by batch reader executions.
    /// PT-br: Quantidade de tabelas de resultado produzidas por execuções batch reader.
    /// </summary>
    public int BatchResultTables
    {
        get => _batchResultTables;
        internal set
        {
            if (Enabled)
                _batchResultTables = value;
        }
    }
    /// <summary>
    /// EN: Number of rows produced by batch reader executions.
    /// PT-br: Quantidade de linhas produzidas por execuções batch reader.
    /// </summary>
    public int BatchRowsReturned
    {
        get => _batchRowsReturned;
        internal set
        {
            if (Enabled)
                _batchRowsReturned = value;
        }
    }
    /// <summary>
    /// EN: Number of exceptions raised during batch execution flows.
    /// PT-br: Quantidade de exceções geradas durante fluxos de execução em batch.
    /// </summary>
    public int BatchExceptions
    {
        get => _batchExceptions;
        internal set
        {
            if (Enabled)
                _batchExceptions = value;
        }
    }
    /// <summary>
    /// EN: Number of batch executions canceled via cancellation tokens or explicit cancellation flow.
    /// PT-br: Quantidade de execuções batch canceladas via cancellation token ou fluxo explícito de cancelamento.
    /// </summary>
    public int BatchCancellations
    {
        get => _batchCancellations;
        internal set
        {
            if (Enabled)
                _batchCancellations = value;
        }
    }
    /// <summary>
    /// EN: Number of batch ExecuteNonQuery executions with no commands.
    /// PT-br: Quantidade de execuções batch ExecuteNonQuery sem comandos.
    /// </summary>
    public int BatchEmptyNonQueryExecutions
    {
        get => _batchEmptyNonQueryExecutions;
        internal set
        {
            if (Enabled)
                _batchEmptyNonQueryExecutions = value;
        }
    }
    /// <summary>
    /// EN: Number of batch ExecuteReader executions with no commands.
    /// PT-br: Quantidade de execuções batch ExecuteReader sem comandos.
    /// </summary>
    public int BatchEmptyReaderExecutions
    {
        get => _batchEmptyReaderExecutions;
        internal set
        {
            if (Enabled)
                _batchEmptyReaderExecutions = value;
        }
    }
    /// <summary>
    /// EN: Number of batch ExecuteScalar executions with no commands.
    /// PT-br: Quantidade de execuções batch ExecuteScalar sem comandos.
    /// </summary>
    public int BatchEmptyScalarExecutions
    {
        get => _batchEmptyScalarExecutions;
        internal set
        {
            if (Enabled)
                _batchEmptyScalarExecutions = value;
        }
    }
    /// <summary>
    /// EN: Number of batch command executions by mode/type (e.g., reader:text, nonquery:storedprocedure).
    /// PT-br: Quantidade de execuções de comando batch por modo/tipo (ex.: reader:text, nonquery:storedprocedure).
    /// </summary>
    public ConcurrentDictionary<string, int> BatchCommandTypeHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of failures grouped by batch phase (materialization, reader, nonquery, scalar).
    /// PT-br: Quantidade de falhas agrupadas por fase do batch (materialization, reader, nonquery, scalar).
    /// </summary>
    public ConcurrentDictionary<string, int> BatchPhaseFailures { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of cancellations grouped by batch phase.
    /// PT-br: Quantidade de cancelamentos agrupados por fase do batch.
    /// </summary>
    public ConcurrentDictionary<string, int> BatchPhaseCancellations { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks by batch phase (materialization, reader, fallback-nonquery, nonquery, scalar).
    /// PT-br: Ticks acumulados por fase de batch (materialization, reader, fallback-nonquery, nonquery, scalar).
    /// </summary>
    public ConcurrentDictionary<string, long> BatchPhaseElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of hits grouped by custom performance phase name.
    /// PT-br: Quantidade de ocorrencias agrupadas por nome de fase de performance.
    /// </summary>
    public ConcurrentDictionary<string, int> PerformancePhaseHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks grouped by custom performance phase name.
    /// PT-br: Ticks acumulados agrupados por nome de fase de performance.
    /// </summary>
    public ConcurrentDictionary<string, long> PerformancePhaseElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Total elapsed time since metrics started.
    /// PT-br: Tempo total decorrido desde o início da coleta.
    /// </summary>
    public TimeSpan Elapsed => Enabled ? _sw.Elapsed : TimeSpan.Zero;

    internal void IncrementIndexHint(string indexName) => IndexHints.AddOrUpdate(indexName, 1, _incrementHandler);

    internal void IncrementTableHint(string tableName) => TableHints.AddOrUpdate(tableName, 1, _incrementHandler);

    internal void IncrementNonQueryStatement() => Interlocked.Increment(ref _nonQueryStatements);

    internal void IncrementNonQueryParseCacheHit() => Interlocked.Increment(ref _nonQueryParseCacheHits);

    internal void IncrementNonQueryParseCacheMiss() => Interlocked.Increment(ref _nonQueryParseCacheMisses);

    internal void IncrementNonQueryHandlerHit(string handlerName) => NonQueryHandlerHits.AddOrUpdate(handlerName, 1, _incrementHandler);

    internal void IncrementNonQueryHandlerElapsedTicks(string handlerName, long elapsedTicks) => NonQueryHandlerElapsedTicks.AddOrUpdate(handlerName, elapsedTicks, (_, current) => current + elapsedTicks);

    internal void IncrementNonQueryHandlerFailure(string handlerName) => NonQueryHandlerFailures.AddOrUpdate(handlerName, 1, _incrementHandler);

    internal void IncrementNonQueryException() => Interlocked.Increment(ref _nonQueryExceptions);

    internal void IncrementNonQueryUnhandledStatement() => Interlocked.Increment(ref _nonQueryUnhandledStatements);

    internal void IncrementReaderProcessedStatements(int count = 1) => Interlocked.Add(ref _readerProcessedStatements, count);

    internal void IncrementReaderControlStatement() => Interlocked.Increment(ref _readerControlStatements);

    internal void IncrementReaderCallStatement() => Interlocked.Increment(ref _readerCallStatements);

    internal void IncrementReaderStoredProcedureStatement() => Interlocked.Increment(ref _readerStoredProcedureStatements);

    internal void IncrementReaderResultTables(int count) => Interlocked.Add(ref _readerResultTables, count);

    internal void IncrementReaderRowsReturned(int count) => Interlocked.Add(ref _readerRowsReturned, count);

    internal void IncrementReaderWithoutSelectError() => Interlocked.Increment(ref _readerWithoutSelectErrors);

    internal void IncrementReaderQueryTypeHit(string queryTypeName) => ReaderQueryTypeHits.AddOrUpdate(queryTypeName, 1, _incrementHandler);

    internal void IncrementBatchNonQueryCommand() => Interlocked.Increment(ref _batchNonQueryCommands);

    internal void IncrementBatchReaderCommand() => Interlocked.Increment(ref _batchReaderCommands);

    internal void IncrementBatchScalarCommand() => Interlocked.Increment(ref _batchScalarCommands);

    internal void IncrementBatchReaderFallbackToNonQuery() => Interlocked.Increment(ref _batchReaderFallbackToNonQuery);

    internal void IncrementBatchMaterialization() => Interlocked.Increment(ref _batchMaterializations);

    internal void IncrementBatchResultTables(int count) => Interlocked.Add(ref _batchResultTables, count);

    internal void IncrementBatchRowsReturned(int count) => Interlocked.Add(ref _batchRowsReturned, count);

    internal void IncrementBatchException() => Interlocked.Increment(ref _batchExceptions);

    internal void IncrementBatchCancellation() => Interlocked.Increment(ref _batchCancellations);

    internal void IncrementBatchEmptyNonQueryExecution() => Interlocked.Increment(ref _batchEmptyNonQueryExecutions);

    internal void IncrementBatchEmptyReaderExecution() => Interlocked.Increment(ref _batchEmptyReaderExecutions);

    internal void IncrementBatchEmptyScalarExecution() => Interlocked.Increment(ref _batchEmptyScalarExecutions);

    internal void IncrementBatchCommandTypeHit(string key) => BatchCommandTypeHits.AddOrUpdate(key, 1, _incrementHandler);

    internal void IncrementBatchPhaseFailure(string phase) => BatchPhaseFailures.AddOrUpdate(phase, 1, _incrementHandler);

    internal void IncrementBatchPhaseCancellation(string phase) => BatchPhaseCancellations.AddOrUpdate(phase, 1, _incrementHandler);

    internal void IncrementBatchPhaseElapsedTicks(string phase, long elapsedTicks) => BatchPhaseElapsedTicks.AddOrUpdate(phase, elapsedTicks, (_, current) => current + elapsedTicks);

    internal void IncrementPerformancePhaseHit(string phase) => PerformancePhaseHits.AddOrUpdate(phase, 1, _incrementHandler);

    internal void IncrementPerformancePhaseElapsedTicks(string phase, long elapsedTicks) => PerformancePhaseElapsedTicks.AddOrUpdate(phase, elapsedTicks, (_, current) => current + elapsedTicks);

    internal string? FormatPerformancePhases()
    {
        if (!Enabled)
            return null;

        return FormatPerformancePhases(PerformancePhaseHits, PerformancePhaseElapsedTicks);
    }

    internal string? FormatPerformancePhasesDelta(DbMetricsSnapshot before)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(before, nameof(before));

        if (!Enabled)
            return null;

        Dictionary<string, int>? hits = null;
        Dictionary<string, long>? elapsedTicks = null;

        foreach (var key in PerformancePhaseElapsedTicks.Keys.Union(PerformancePhaseHits.Keys, StringComparer.OrdinalIgnoreCase))
        {
            PerformancePhaseElapsedTicks.TryGetValue(key, out var currentTicks);
            before.PerformancePhaseElapsedTicks.TryGetValue(key, out var beforeTicks);
            var deltaTicks = currentTicks - beforeTicks;
            PerformancePhaseHits.TryGetValue(key, out var currentHits);
            before.PerformancePhaseHits.TryGetValue(key, out var beforeHits);
            var deltaHits = currentHits - beforeHits;

            if (deltaTicks <= 0 && deltaHits <= 0)
                continue;

            (hits ??= new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase))[key] = Math.Max(0, deltaHits);
            (elapsedTicks ??= new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase))[key] = Math.Max(0, deltaTicks);
        }

        if (hits is null || elapsedTicks is null || hits.Count == 0)
            return null;

        return FormatPerformancePhases(hits, elapsedTicks);
    }

    internal DbMetricsSnapshot CapturePerformanceSnapshot()
    {
        if (!Enabled)
        {
            return new DbMetricsSnapshot(
                new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase));
        }

        return new DbMetricsSnapshot(
            new Dictionary<string, int>(PerformancePhaseHits, StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, long>(PerformancePhaseElapsedTicks, StringComparer.OrdinalIgnoreCase));
    }

    private static string? FormatPerformancePhases(
        IReadOnlyDictionary<string, int> hitsByPhase,
        IReadOnlyDictionary<string, long> elapsedTicksByPhase)
    {
        List<(string Key, int Hits, long ElapsedTicks)>? items = null;

        foreach (var key in elapsedTicksByPhase.Keys.Union(hitsByPhase.Keys, StringComparer.OrdinalIgnoreCase))
        {
            elapsedTicksByPhase.TryGetValue(key, out var elapsedTicks);
            hitsByPhase.TryGetValue(key, out var hits);
            if (elapsedTicks <= 0 && hits <= 0)
                continue;

            (items ??= []).Add((key, hits, elapsedTicks));
        }

        if (items is null || items.Count == 0)
            return null;

        items.Sort(static (left, right) =>
        {
            var ticks = right.ElapsedTicks.CompareTo(left.ElapsedTicks);
            return ticks != 0
                ? ticks
                : StringComparer.OrdinalIgnoreCase.Compare(left.Key, right.Key);
        });

        var builder = new StringBuilder();
        var wroteAny = false;
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            if (item.ElapsedTicks <= 0 && item.Hits <= 0)
                continue;
            if (wroteAny)
                builder.Append(',');
            else
                wroteAny = true;

            builder.Append(item.Key);
            builder.Append("[hits=");
            builder.Append(item.Hits);
            builder.Append(",ms=");
            builder.Append(TimeSpan.FromTicks(item.ElapsedTicks).TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(']');
        }

        return wroteAny ? builder.ToString() : null;
    }

    internal IDisposable BeginAmbientScope()
        => Enabled ? new AmbientMetricsScope(this) : NoopDisposable.Instance;

    internal static DbMetrics? Current
        => _ambientMetrics.Value is { Enabled: true } metrics ? metrics : null;

    private readonly Stopwatch _sw = Stopwatch.StartNew();
    private static readonly AsyncLocal<DbMetrics?> _ambientMetrics = new();
    private static readonly Func<string, int, int> _incrementHandler = static (_, current) => current + 1;

    private sealed class NoopDisposable : IDisposable
    {
        public static readonly NoopDisposable Instance = new();

        public void Dispose()
        {
        }
    }

    private sealed class AmbientMetricsScope : IDisposable
    {
        private readonly DbMetrics? _previous;
        private bool _disposed;

        public AmbientMetricsScope(DbMetrics metrics)
        {
            _previous = _ambientMetrics.Value;
            _ambientMetrics.Value = metrics;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _ambientMetrics.Value = _previous;
            _disposed = true;
        }
    }
}
