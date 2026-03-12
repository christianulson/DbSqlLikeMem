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

    /// <summary>
    /// 
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        Session = CreateSession();
        Session.Initialize();
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
    protected void Run(BenchmarkFeatureId feature)
    {
        Session.Execute(feature);
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
