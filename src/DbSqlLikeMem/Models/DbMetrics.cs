using System.Collections.Concurrent;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Accumulates usage and timing metrics for in-memory DB operations.
/// PT: Acumula métricas de uso e tempo para operações no banco em memória.
/// </summary>
public sealed class DbMetrics
{
    /// <summary>
    /// EN: Number of SELECT queries executed.
    /// PT: Quantidade de consultas SELECT executadas.
    /// </summary>
    public int Selects { get; internal set; }
    /// <summary>
    /// EN: Number of INSERT operations executed.
    /// PT: Quantidade de operações INSERT executadas.
    /// </summary>
    public int Inserts { get; internal set; }
    /// <summary>
    /// EN: Number of UPDATE operations executed.
    /// PT: Quantidade de operações UPDATE executadas.
    /// </summary>
    public int Updates { get; internal set; }
    /// <summary>
    /// EN: Number of DELETE operations executed.
    /// PT: Quantidade de operações DELETE executadas.
    /// </summary>
    public int Deletes { get; internal set; }
    /// <summary>
    /// EN: Number of non-query statements executed by command pipeline.
    /// PT: Quantidade de statements non-query executados pelo pipeline de comando.
    /// </summary>
    public int NonQueryStatements { get; internal set; }
    /// <summary>
    /// EN: Number of parse cache hits inside non-query pipeline context.
    /// PT: Quantidade de acertos de cache de parse no contexto do pipeline non-query.
    /// </summary>
    public int NonQueryParseCacheHits { get; internal set; }
    /// <summary>
    /// EN: Number of parse cache misses inside non-query pipeline context.
    /// PT: Quantidade de perdas de cache de parse no contexto do pipeline non-query.
    /// </summary>
    public int NonQueryParseCacheMisses { get; internal set; }
    /// <summary>
    /// EN: Number of index lookups used to pre-filter rows during SELECT.
    /// PT: Quantidade de consultas em índice usadas para pré-filtrar linhas em SELECT.
    /// </summary>
    public int IndexLookups { get; internal set; }
    /// <summary>
    /// EN: Number of calls per index name during index lookup flow.
    /// PT: Quantidade de chamadas por nome de índice durante o fluxo de lookup.
    /// </summary>
    public ConcurrentDictionary<string, int> IndexHints { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of accesses per table name during query source resolution.
    /// PT: Quantidade de acessos por nome de tabela durante resolução de origem da consulta.
    /// </summary>
    public ConcurrentDictionary<string, int> TableHints { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of handler activations by handler name in non-query pipeline.
    /// PT: Quantidade de ativações por nome de handler no pipeline non-query.
    /// </summary>
    public ConcurrentDictionary<string, int> NonQueryHandlerHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks per non-query handler (handled path only).
    /// PT: Ticks acumulados de tempo por handler non-query (apenas caminho tratado).
    /// </summary>
    public ConcurrentDictionary<string, long> NonQueryHandlerElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of failures by non-query handler name.
    /// PT: Quantidade de falhas por nome de handler non-query.
    /// </summary>
    public ConcurrentDictionary<string, int> NonQueryHandlerFailures { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of non-query statements that raised exceptions in pipeline flow.
    /// PT: Quantidade de statements non-query que geraram exceções no fluxo do pipeline.
    /// </summary>
    public int NonQueryExceptions { get; internal set; }
    /// <summary>
    /// EN: Number of non-query statements not handled by any configured handler.
    /// PT: Quantidade de statements non-query não tratados por nenhum handler configurado.
    /// </summary>
    public int NonQueryUnhandledStatements { get; internal set; }
    /// <summary>
    /// EN: Number of statements processed by ExecuteReader flows.
    /// PT: Quantidade de statements processados pelos fluxos de ExecuteReader.
    /// </summary>
    public int ReaderProcessedStatements { get; internal set; }
    /// <summary>
    /// EN: Number of transaction-control statements handled in ExecuteReader flow.
    /// PT: Quantidade de statements de controle transacional tratados no fluxo de ExecuteReader.
    /// </summary>
    public int ReaderControlStatements { get; internal set; }
    /// <summary>
    /// EN: Number of CALL statements handled in ExecuteReader flow.
    /// PT: Quantidade de statements CALL tratados no fluxo de ExecuteReader.
    /// </summary>
    public int ReaderCallStatements { get; internal set; }
    /// <summary>
    /// EN: Number of stored procedures executed through ExecuteReader prelude.
    /// PT: Quantidade de procedures executadas via prelude de ExecuteReader.
    /// </summary>
    public int ReaderStoredProcedureStatements { get; internal set; }
    /// <summary>
    /// EN: Number of result tables produced by ExecuteReader.
    /// PT: Quantidade de tabelas de resultado produzidas por ExecuteReader.
    /// </summary>
    public int ReaderResultTables { get; internal set; }
    /// <summary>
    /// EN: Number of rows returned by ExecuteReader result tables.
    /// PT: Quantidade de linhas retornadas pelas tabelas de resultado de ExecuteReader.
    /// </summary>
    public int ReaderRowsReturned { get; internal set; }
    /// <summary>
    /// EN: Number of ExecuteReader attempts with no SELECT result after parsed statements.
    /// PT: Quantidade de tentativas de ExecuteReader sem resultado SELECT após statements parseados.
    /// </summary>
    public int ReaderWithoutSelectErrors { get; internal set; }
    /// <summary>
    /// EN: Number of parsed query dispatches by query type name in ExecuteReader flow.
    /// PT: Quantidade de despachos de query parseada por tipo de query no fluxo de ExecuteReader.
    /// </summary>
    public ConcurrentDictionary<string, int> ReaderQueryTypeHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteNonQuery flow.
    /// PT: Quantidade de comandos executados via fluxo batch ExecuteNonQuery.
    /// </summary>
    public int BatchNonQueryCommands { get; internal set; }
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteReader flow.
    /// PT: Quantidade de comandos executados via fluxo batch ExecuteReader.
    /// </summary>
    public int BatchReaderCommands { get; internal set; }
    /// <summary>
    /// EN: Number of commands executed via batch ExecuteScalar flow.
    /// PT: Quantidade de comandos executados via fluxo batch ExecuteScalar.
    /// </summary>
    public int BatchScalarCommands { get; internal set; }
    /// <summary>
    /// EN: Number of batch reader commands that fell back to ExecuteNonQuery due to no SELECT result.
    /// PT: Quantidade de comandos batch reader que fizeram fallback para ExecuteNonQuery por ausência de resultado SELECT.
    /// </summary>
    public int BatchReaderFallbackToNonQuery { get; internal set; }
    /// <summary>
    /// EN: Number of batch command materializations performed before execution.
    /// PT: Quantidade de materializações de comandos batch realizadas antes da execução.
    /// </summary>
    public int BatchMaterializations { get; internal set; }
    /// <summary>
    /// EN: Number of result tables produced by batch reader executions.
    /// PT: Quantidade de tabelas de resultado produzidas por execuções batch reader.
    /// </summary>
    public int BatchResultTables { get; internal set; }
    /// <summary>
    /// EN: Number of rows produced by batch reader executions.
    /// PT: Quantidade de linhas produzidas por execuções batch reader.
    /// </summary>
    public int BatchRowsReturned { get; internal set; }
    /// <summary>
    /// EN: Number of exceptions raised during batch execution flows.
    /// PT: Quantidade de exceções geradas durante fluxos de execução em batch.
    /// </summary>
    public int BatchExceptions { get; internal set; }
    /// <summary>
    /// EN: Number of batch executions canceled via cancellation tokens or explicit cancellation flow.
    /// PT: Quantidade de execuções batch canceladas via cancellation token ou fluxo explícito de cancelamento.
    /// </summary>
    public int BatchCancellations { get; internal set; }
    /// <summary>
    /// EN: Number of batch ExecuteNonQuery executions with no commands.
    /// PT: Quantidade de execuções batch ExecuteNonQuery sem comandos.
    /// </summary>
    public int BatchEmptyNonQueryExecutions { get; internal set; }
    /// <summary>
    /// EN: Number of batch ExecuteReader executions with no commands.
    /// PT: Quantidade de execuções batch ExecuteReader sem comandos.
    /// </summary>
    public int BatchEmptyReaderExecutions { get; internal set; }
    /// <summary>
    /// EN: Number of batch ExecuteScalar executions with no commands.
    /// PT: Quantidade de execuções batch ExecuteScalar sem comandos.
    /// </summary>
    public int BatchEmptyScalarExecutions { get; internal set; }
    /// <summary>
    /// EN: Number of batch command executions by mode/type (e.g., reader:text, nonquery:storedprocedure).
    /// PT: Quantidade de execuções de comando batch por modo/tipo (ex.: reader:text, nonquery:storedprocedure).
    /// </summary>
    public ConcurrentDictionary<string, int> BatchCommandTypeHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of failures grouped by batch phase (materialization, reader, nonquery, scalar).
    /// PT: Quantidade de falhas agrupadas por fase do batch (materialization, reader, nonquery, scalar).
    /// </summary>
    public ConcurrentDictionary<string, int> BatchPhaseFailures { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of cancellations grouped by batch phase.
    /// PT: Quantidade de cancelamentos agrupados por fase do batch.
    /// </summary>
    public ConcurrentDictionary<string, int> BatchPhaseCancellations { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks by batch phase (materialization, reader, fallback-nonquery, nonquery, scalar).
    /// PT: Ticks acumulados por fase de batch (materialization, reader, fallback-nonquery, nonquery, scalar).
    /// </summary>
    public ConcurrentDictionary<string, long> BatchPhaseElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Number of hits grouped by custom performance phase name.
    /// PT: Quantidade de ocorrencias agrupadas por nome de fase de performance.
    /// </summary>
    public ConcurrentDictionary<string, int> PerformancePhaseHits { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Accumulated elapsed ticks grouped by custom performance phase name.
    /// PT: Ticks acumulados agrupados por nome de fase de performance.
    /// </summary>
    public ConcurrentDictionary<string, long> PerformancePhaseElapsedTicks { get; } =
        new(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Total elapsed time since metrics started.
    /// PT: Tempo total decorrido desde o início da coleta.
    /// </summary>
    public TimeSpan Elapsed => _sw.Elapsed;

    internal void IncrementIndexHint(string indexName)
        => IndexHints.AddOrUpdate(indexName, 1, (_, current) => current + 1);

    internal void IncrementTableHint(string tableName)
        => TableHints.AddOrUpdate(tableName, 1, (_, current) => current + 1);

    internal void IncrementNonQueryStatement()
        => NonQueryStatements++;

    internal void IncrementNonQueryParseCacheHit()
        => NonQueryParseCacheHits++;

    internal void IncrementNonQueryParseCacheMiss()
        => NonQueryParseCacheMisses++;

    internal void IncrementNonQueryHandlerHit(string handlerName)
        => NonQueryHandlerHits.AddOrUpdate(handlerName, 1, (_, current) => current + 1);

    internal void IncrementNonQueryHandlerElapsedTicks(string handlerName, long elapsedTicks)
        => NonQueryHandlerElapsedTicks.AddOrUpdate(handlerName, elapsedTicks, (_, current) => current + elapsedTicks);

    internal void IncrementNonQueryHandlerFailure(string handlerName)
        => NonQueryHandlerFailures.AddOrUpdate(handlerName, 1, (_, current) => current + 1);

    internal void IncrementNonQueryException()
        => NonQueryExceptions++;

    internal void IncrementNonQueryUnhandledStatement()
        => NonQueryUnhandledStatements++;

    internal void IncrementReaderProcessedStatements(int count = 1)
        => ReaderProcessedStatements += count;

    internal void IncrementReaderControlStatement()
        => ReaderControlStatements++;

    internal void IncrementReaderCallStatement()
        => ReaderCallStatements++;

    internal void IncrementReaderStoredProcedureStatement()
        => ReaderStoredProcedureStatements++;

    internal void IncrementReaderResultTables(int count)
        => ReaderResultTables += count;

    internal void IncrementReaderRowsReturned(int count)
        => ReaderRowsReturned += count;

    internal void IncrementReaderWithoutSelectError()
        => ReaderWithoutSelectErrors++;

    internal void IncrementReaderQueryTypeHit(string queryTypeName)
        => ReaderQueryTypeHits.AddOrUpdate(queryTypeName, 1, (_, current) => current + 1);

    internal void IncrementBatchNonQueryCommand()
        => BatchNonQueryCommands++;

    internal void IncrementBatchReaderCommand()
        => BatchReaderCommands++;

    internal void IncrementBatchScalarCommand()
        => BatchScalarCommands++;

    internal void IncrementBatchReaderFallbackToNonQuery()
        => BatchReaderFallbackToNonQuery++;

    internal void IncrementBatchMaterialization()
        => BatchMaterializations++;

    internal void IncrementBatchResultTables(int count)
        => BatchResultTables += count;

    internal void IncrementBatchRowsReturned(int count)
        => BatchRowsReturned += count;

    internal void IncrementBatchException()
        => BatchExceptions++;

    internal void IncrementBatchCancellation()
        => BatchCancellations++;

    internal void IncrementBatchEmptyNonQueryExecution()
        => BatchEmptyNonQueryExecutions++;

    internal void IncrementBatchEmptyReaderExecution()
        => BatchEmptyReaderExecutions++;

    internal void IncrementBatchEmptyScalarExecution()
        => BatchEmptyScalarExecutions++;

    internal void IncrementBatchCommandTypeHit(string key)
        => BatchCommandTypeHits.AddOrUpdate(key, 1, (_, current) => current + 1);

    internal void IncrementBatchPhaseFailure(string phase)
        => BatchPhaseFailures.AddOrUpdate(phase, 1, (_, current) => current + 1);

    internal void IncrementBatchPhaseCancellation(string phase)
        => BatchPhaseCancellations.AddOrUpdate(phase, 1, (_, current) => current + 1);

    internal void IncrementBatchPhaseElapsedTicks(string phase, long elapsedTicks)
        => BatchPhaseElapsedTicks.AddOrUpdate(phase, elapsedTicks, (_, current) => current + elapsedTicks);

    internal void IncrementPerformancePhaseHit(string phase)
        => PerformancePhaseHits.AddOrUpdate(phase, 1, (_, current) => current + 1);

    internal void IncrementPerformancePhaseElapsedTicks(string phase, long elapsedTicks)
        => PerformancePhaseElapsedTicks.AddOrUpdate(phase, elapsedTicks, (_, current) => current + elapsedTicks);

    internal string? FormatPerformancePhases()
        => FormatPerformancePhases(PerformancePhaseHits, PerformancePhaseElapsedTicks);

    internal string? FormatPerformancePhasesDelta(DbMetricsSnapshot before)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(before, nameof(before));

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
        => new(
            new Dictionary<string, int>(PerformancePhaseHits, StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, long>(PerformancePhaseElapsedTicks, StringComparer.OrdinalIgnoreCase));

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
        => new AmbientMetricsScope(this);

    internal static DbMetrics? Current => _ambientMetrics.Value;

    private readonly Stopwatch _sw = Stopwatch.StartNew();
    private static readonly AsyncLocal<DbMetrics?> _ambientMetrics = new();

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
