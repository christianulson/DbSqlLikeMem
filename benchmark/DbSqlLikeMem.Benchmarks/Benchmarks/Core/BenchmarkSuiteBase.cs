namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Defines the base BenchmarkDotNet suite lifecycle shared by all provider-specific suites.
/// PT-br: Define o ciclo de vida base do BenchmarkDotNet compartilhado por todas as suítes específicas de provedor.
/// </summary>
[MemoryDiagnoser]
public abstract class BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected IBenchmarkSession Session { get; private set; } = null!;

    /// <summary>
    /// 
    /// </summary>
    protected abstract IBenchmarkSession CreateSession();

    private bool _sessionReady;
    private Exception? _setupException;

    /// <summary>
    /// 
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        Session = CreateSession();
        try
        {
            Session.Initialize();
            _sessionReady = true;
            _setupException = null;
        }
        catch (Exception ex)
        {
            _sessionReady = false;
            _setupException = ex;
            LogSetupIssue(ex);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Session.Dispose();
    }

    /// <summary>
    /// 
    /// </summary>
    public void Run(BenchmarkFeatureId feature)
    {
        if (!_sessionReady)
        {
            var reason = _setupException?.GetBaseException()?.ToString() ?? "Session not initialized.";
            LogBenchmarkIssue(
                feature,
                new InvalidOperationException($"Session not initialized. Setup failure: {reason}"));
            return;
        }

        Session.Execute(feature);
    }

    /// <summary>
    /// EN: Executes a connection-open benchmark.
    /// PT: Executa um benchmark de abertura de conexao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ConnectionOpen() => Run(BenchmarkFeatureId.ConnectionOpen);

    /// <summary>
    /// EN: Executes a schema-creation benchmark.
    /// PT: Executa um benchmark de criacao de esquema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CreateSchema() => Run(BenchmarkFeatureId.CreateSchema);

    /// <summary>
    /// EN: Executes a single-row insert benchmark.
    /// PT: Executa um benchmark de insercao de uma linha.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertSingle() => Run(BenchmarkFeatureId.InsertSingle);

    /// <summary>
    /// EN: Executes a 100-row batch insert benchmark.
    /// PT: Executa um benchmark de insercao em lote de 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertBatch100() => Run(BenchmarkFeatureId.InsertBatch100);

    /// <summary>
    /// EN: Executes a parallel 100-row batch insert benchmark.
    /// PT: Executa um benchmark de insercao em lote paralelo de 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertBatch100Parallel() => Run(BenchmarkFeatureId.InsertBatch100Parallel);

    /// <summary>
    /// EN: Executes a primary-key lookup benchmark.
    /// PT: Executa um benchmark de consulta por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectByPk() => Run(BenchmarkFeatureId.SelectByPk);

    /// <summary>
    /// EN: Executes a join-query benchmark.
    /// PT: Executa um benchmark de consulta com join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectJoin() => Run(BenchmarkFeatureId.SelectJoin);

    /// <summary>
    /// EN: Executes a primary-key update benchmark.
    /// PT: Executa um benchmark de atualizacao por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void UpdateByPk() => Run(BenchmarkFeatureId.UpdateByPk);

    /// <summary>
    /// EN: Executes a primary-key delete benchmark.
    /// PT: Executa um benchmark de exclusao por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void DeleteByPk() => Run(BenchmarkFeatureId.DeleteByPk);

    /// <summary>
    /// EN: Executes a transaction-commit benchmark.
    /// PT: Executa um benchmark de confirmacao de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionCommit() => Run(BenchmarkFeatureId.TransactionCommit);

    /// <summary>
    /// EN: Executes a transaction-rollback benchmark.
    /// PT: Executa um benchmark de rollback de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionRollback() => Run(BenchmarkFeatureId.TransactionRollback);

    /// <summary>
    /// EN: Executes a provider-specific upsert benchmark.
    /// PT: Executa um benchmark de upsert especifico do provedor.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Upsert() => Run(BenchmarkFeatureId.Upsert);

    /// <summary>
    /// EN: Executes a string-aggregation benchmark.
    /// PT: Executa um benchmark de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregate() => Run(BenchmarkFeatureId.StringAggregate);

    /// <summary>
    /// EN: Executes a date-scalar benchmark.
    /// PT: Executa um benchmark escalar de data.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void DateScalar() => Run(BenchmarkFeatureId.DateScalar);

    private static readonly object _setupLogSync = new();
    protected void LogSetupIssue(Exception ex)
    {
        var root = ex.GetBaseException();

        var message =
            $"[SETUP-{root.GetType().Name}] {root.ToString()}";

        Console.WriteLine(message);

        var logEntry =
            $"{DateTime.UtcNow:O} {message}{Environment.NewLine}{root.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        lock (_setupLogSync)
        {
            var file = Path.Combine("Logs", $"{GetType().Namespace}-setup-errors.log");
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                logEntry);
        }
    }

    private static readonly object _logSync = new();
    private static readonly HashSet<string> Errors = [];
    protected virtual void LogBenchmarkIssue(BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex.GetBaseException();
        var message = $"[NA-{root.GetType().Name}] {feature}: {root.Message} -- {ex.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            if (Errors.Contains(root.Message))
                return;
            Errors.Add(root.Message);
            var file = Path.Combine("Logs", $"{GetType().Namespace}-errors.log");
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void InsertBatch10() => Run(BenchmarkFeatureId.InsertBatch10);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void SavepointCreate() => Run(BenchmarkFeatureId.SavepointCreate);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void RollbackToSavepoint() => Run(BenchmarkFeatureId.RollbackToSavepoint);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void ReleaseSavepoint() => Run(BenchmarkFeatureId.ReleaseSavepoint);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void NestedSavepointFlow() => Run(BenchmarkFeatureId.NestedSavepointFlow);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert10() => Run(BenchmarkFeatureId.BatchInsert10);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert100() => Run(BenchmarkFeatureId.BatchInsert100);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchMixedReadWrite() => Run(BenchmarkFeatureId.BatchMixedReadWrite);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchScalar() => Run(BenchmarkFeatureId.BatchScalar);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchNonQuery() => Run(BenchmarkFeatureId.BatchNonQuery);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonScalarRead() => Run(BenchmarkFeatureId.JsonScalarRead);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonPathRead() => Run(BenchmarkFeatureId.JsonPathRead);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalCurrentTimestamp() => Run(BenchmarkFeatureId.TemporalCurrentTimestamp);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalDateAdd() => Run(BenchmarkFeatureId.TemporalDateAdd);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalNowWhere() => Run(BenchmarkFeatureId.TemporalNowWhere);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalNowOrderBy() => Run(BenchmarkFeatureId.TemporalNowOrderBy);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateOrdered() => Run(BenchmarkFeatureId.StringAggregateOrdered);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateDistinct() => Run(BenchmarkFeatureId.StringAggregateDistinct);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateCustomSeparator() => Run(BenchmarkFeatureId.StringAggregateCustomSeparator);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateLargeGroup() => Run(BenchmarkFeatureId.StringAggregateLargeGroup);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterInsert() => Run(BenchmarkFeatureId.RowCountAfterInsert);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterUpdate() => Run(BenchmarkFeatureId.RowCountAfterUpdate);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterSelect() => Run(BenchmarkFeatureId.RowCountAfterSelect);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void CteSimple() => Run(BenchmarkFeatureId.CteSimple);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowRowNumber() => Run(BenchmarkFeatureId.WindowRowNumber);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowLag() => Run(BenchmarkFeatureId.WindowLag);

    /// <summary>
    /// EN: Executes an EXISTS predicate benchmark query.
    /// PT: Executa uma consulta de benchmark com predicado EXISTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectExistsPredicate() => Run(BenchmarkFeatureId.SelectExistsPredicate);

    /// <summary>
    /// EN: Executes a correlated COUNT subquery benchmark.
    /// PT: Executa um benchmark com subconsulta correlacionada de COUNT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCorrelatedCount() => Run(BenchmarkFeatureId.SelectCorrelatedCount);

    /// <summary>
    /// EN: Executes a GROUP BY HAVING benchmark query.
    /// PT: Executa uma consulta de benchmark com GROUP BY HAVING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByHaving() => Run(BenchmarkFeatureId.GroupByHaving);

    /// <summary>
    /// EN: Executes a UNION ALL projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao UNION ALL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    /// <summary>
    /// EN: Executes a DISTINCT projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    /// <summary>
    /// EN: Executes a multi-join aggregate benchmark query.
    /// PT: Executa uma consulta de benchmark com agregacao e multiplos joins.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

    /// <summary>
    /// EN: Executes a scalar subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarSubquery() => Run(BenchmarkFeatureId.SelectScalarSubquery);

    /// <summary>
    /// EN: Executes an IN subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta IN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectInSubquery() => Run(BenchmarkFeatureId.SelectInSubquery);

    /// <summary>
    /// EN: Executes a CROSS APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com CROSS APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void CrossApplyProjection() => Run(BenchmarkFeatureId.CrossApplyProjection);

    /// <summary>
    /// EN: Executes an OUTER APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com OUTER APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchReaderMultiResult() => Run(BenchmarkFeatureId.BatchReaderMultiResult);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchTransactionControl() => Run(BenchmarkFeatureId.BatchTransactionControl);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseSimpleSelect() => Run(BenchmarkFeatureId.ParseSimpleSelect);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseComplexJoin() => Run(BenchmarkFeatureId.ParseComplexJoin);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseInsertReturning() => Run(BenchmarkFeatureId.ParseInsertReturning);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseOnConflictDoUpdate() => Run(BenchmarkFeatureId.ParseOnConflictDoUpdate);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void ParseJsonExtract() => Run(BenchmarkFeatureId.ParseJsonExtract);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ParseStringAggregateWithinGroup() => Run(BenchmarkFeatureId.ParseStringAggregateWithinGroup);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseAutoDialectTopLimitFetch() => Run(BenchmarkFeatureId.ParseAutoDialectTopLimitFetch);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void ParseMultiStatementBatch() => Run(BenchmarkFeatureId.ParseMultiStatementBatch);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonInsertCast() => Run(BenchmarkFeatureId.JsonInsertCast);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void RowCountInBatch() => Run(BenchmarkFeatureId.RowCountInBatch);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PivotCount() => Run(BenchmarkFeatureId.PivotCount);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningInsert() => Run(BenchmarkFeatureId.ReturningInsert);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningUpdate() => Run(BenchmarkFeatureId.ReturningUpdate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MergeBasic() => Run(BenchmarkFeatureId.MergeBasic);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PartitionPruningSelect() => Run(BenchmarkFeatureId.PartitionPruningSelect);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlan() => Run(BenchmarkFeatureId.ExecutionPlan);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanSelect() => Run(BenchmarkFeatureId.ExecutionPlanSelect);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanJoin() => Run(BenchmarkFeatureId.ExecutionPlanJoin);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanDml() => Run(BenchmarkFeatureId.ExecutionPlanDml);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceSelect() => Run(BenchmarkFeatureId.DebugTraceSelect);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceBatch() => Run(BenchmarkFeatureId.DebugTraceBatch);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceJson() => Run(BenchmarkFeatureId.DebugTraceJson);

    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void LastExecutionPlansHistory() => Run(BenchmarkFeatureId.LastExecutionPlansHistory);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void TempTableCreateAndUse() => Run(BenchmarkFeatureId.TempTableCreateAndUse);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TempTableRollback() => Run(BenchmarkFeatureId.TempTableRollback);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void TempTableCrossConnectionIsolation() => Run(BenchmarkFeatureId.TempTableCrossConnectionIsolation);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ResetVolatileData() => Run(BenchmarkFeatureId.ResetVolatileData);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ResetAllVolatileData() => Run(BenchmarkFeatureId.ResetAllVolatileData);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ConnectionReopenAfterClose() => Run(BenchmarkFeatureId.ConnectionReopenAfterClose);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotExport() => Run(BenchmarkFeatureId.SchemaSnapshotExport);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotToJson() => Run(BenchmarkFeatureId.SchemaSnapshotToJson);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotLoadJson() => Run(BenchmarkFeatureId.SchemaSnapshotLoadJson);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotApply() => Run(BenchmarkFeatureId.SchemaSnapshotApply);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotRoundTrip() => Run(BenchmarkFeatureId.SchemaSnapshotRoundTrip);

    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotCompare() => Run(BenchmarkFeatureId.SchemaSnapshotCompare);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSchemaBuild() => Run(BenchmarkFeatureId.FluentSchemaBuild);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSeed100() => Run(BenchmarkFeatureId.FluentSeed100);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSeed1000() => Run(BenchmarkFeatureId.FluentSeed1000);

    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentScenarioCompose() => Run(BenchmarkFeatureId.FluentScenarioCompose);

}

