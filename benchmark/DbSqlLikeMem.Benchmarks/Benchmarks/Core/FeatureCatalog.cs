namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the catalog of benchmark features supported by the benchmark matrix and documentation.
/// PT-br: Fornece o catálogo de recursos de benchmark suportados pela matriz de benchmark e pela documentação.
/// </summary>
public static class FeatureCatalog
{

    /// <summary>
    /// EN: Gets all benchmark feature definitions included in the current benchmark catalog.
    /// PT-br: Obtém todas as definições de recursos de benchmark incluídas no catálogo atual.
    /// </summary>
    public static IReadOnlyList<FeatureDefinition> All { get; } =
    [
        new(BenchmarkFeatureId.ConnectionOpen, "Connection open", "Core", true, ["1.1.3", "2.1.1"]),
        new(BenchmarkFeatureId.CreateSchema, "Create schema", "Setup", true, ["1.2.1", "1.4.1"]),
        new(BenchmarkFeatureId.InsertSingle, "Insert single row", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.InsertBatch10, "Insert batch 10", "Batch", true, []),
        new(BenchmarkFeatureId.InsertBatch100, "Insert batch 100", "Batch", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.InsertBatch100Parallel, "Insert batch 100 parallel", "Batch", true, []),
        new(BenchmarkFeatureId.SelectByPk, "Select by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.SelectJoin, "Join select", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.UpdateByPk, "Update by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.DeleteByPk, "Delete by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.TransactionCommit, "Transaction commit", "Transactions", true, ["1.1.2", "1.3.2"]),
        new(BenchmarkFeatureId.TransactionRollback, "Transaction rollback", "Transactions", true, ["1.1.2", "1.3.2"]),
        new(BenchmarkFeatureId.SavepointCreate, "Savepoint create", "Transactions", true, []),
        new(BenchmarkFeatureId.RollbackToSavepoint, "Rollback to savepoint", "Transactions", true, []),
        new(BenchmarkFeatureId.ReleaseSavepoint, "Release savepoint", "Transactions", true, []),
        new(BenchmarkFeatureId.NestedSavepointFlow, "Nested savepoint flow", "Transactions", true, []),
        new(BenchmarkFeatureId.Upsert, "Upsert", "Dialect", true, ["1.2.2", "3.1.2", "3.4.2", "3.6.2"], "MySQL usa ON DUPLICATE, PostgreSQL/SQLite usam ON CONFLICT e SQL Server/Oracle/DB2 usam MERGE."),
        new(BenchmarkFeatureId.SequenceNextValue, "Sequence next value", "Dialect", true, ["1.1.1", "1.2.2", "3.2.2", "3.3.2", "3.4.2", "3.6.2"]),
        new(BenchmarkFeatureId.BatchInsert10, "Batch insert 10", "Batch", true, []),
        new(BenchmarkFeatureId.BatchInsert100, "Batch insert 100", "Batch", true, []),
        new(BenchmarkFeatureId.BatchMixedReadWrite, "Batch mixed read/write", "Batch", true, []),
        new(BenchmarkFeatureId.BatchScalar, "Batch scalar", "Batch", true, []),
        new(BenchmarkFeatureId.BatchNonQuery, "Batch non-query", "Batch", true, []),
        new(BenchmarkFeatureId.StringAggregate, "String aggregate", "Dialect", true, ["1.2.5", "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", "3.6.2"]),
        new(BenchmarkFeatureId.StringAggregateOrdered, "String aggregate ordered", "Dialect", true, []),
        new(BenchmarkFeatureId.StringAggregateDistinct, "String aggregate distinct", "Dialect", true, []),
        new(BenchmarkFeatureId.StringAggregateCustomSeparator, "String aggregate custom separator", "Dialect", true, []),
        new(BenchmarkFeatureId.StringAggregateLargeGroup, "String aggregate large group", "Dialect", true, []),
        new(BenchmarkFeatureId.DateScalar, "Date scalar", "Temporal", true, ["1.2.6"]),
        new(BenchmarkFeatureId.TemporalCurrentTimestamp, "Temporal current timestamp", "Temporal", true, []),
        new(BenchmarkFeatureId.TemporalDateAdd, "Temporal DATEADD", "Temporal", true, []),
        new(BenchmarkFeatureId.TemporalNowWhere, "Temporal NOW in WHERE", "Temporal", true, []),
        new(BenchmarkFeatureId.TemporalNowOrderBy, "Temporal NOW in ORDER BY", "Temporal", true, []),
        new(BenchmarkFeatureId.JsonScalarRead, "JSON scalar read", "Json", true, []),
        new(BenchmarkFeatureId.JsonPathRead, "JSON path read", "Json", true, []),
        new(BenchmarkFeatureId.RowCountAfterInsert, "Row count after insert", "Core", true, []),
        new(BenchmarkFeatureId.RowCountAfterUpdate, "Row count after update", "Core", true, []),
        new(BenchmarkFeatureId.RowCountAfterSelect, "Row count after select", "Core", true, []),
        new(BenchmarkFeatureId.BatchReaderMultiResult, "Batch reader multi-result", "Batch", true, [], "ExecuteReader and multi-result batch flow."),
        new(BenchmarkFeatureId.BatchTransactionControl, "Batch transaction control", "Batch", true, [], "Batch flow with BEGIN/COMMIT/ROLLBACK or equivalent transaction control."),
        new(BenchmarkFeatureId.ParseSimpleSelect, "Parse simple select", "AdvancedQuery", false, [], "Parser-focused benchmark to isolate simple SELECT parsing cost."),
        new(BenchmarkFeatureId.ParseComplexJoin, "Parse complex join", "AdvancedQuery", false, [], "Parser-focused benchmark for larger join trees and predicates."),
        new(BenchmarkFeatureId.ParseInsertReturning, "Parse insert returning", "AdvancedQuery", false, [], "Parser benchmark for INSERT ... RETURNING flows."),
        new(BenchmarkFeatureId.ParseOnConflictDoUpdate, "Parse on conflict do update", "AdvancedQuery", false, [], "Parser benchmark for PostgreSQL/SQLite style UPSERT syntax handling."),
        new(BenchmarkFeatureId.ParseJsonExtract, "Parse JSON extract", "Json", false, [], "Parser benchmark for JSON path extraction expressions."),
        new(BenchmarkFeatureId.ParseStringAggregateWithinGroup, "Parse string aggregate within group", "Dialect", false, [], "Parser benchmark for ordered-set aggregate syntax."),
        new(BenchmarkFeatureId.ParseAutoDialectTopLimitFetch, "Parse auto-dialect TOP/LIMIT/FETCH", "AdvancedQuery", false, [], "Parser benchmark for automatic pagination dialect adaptation."),
        new(BenchmarkFeatureId.ParseMultiStatementBatch, "Parse multi-statement batch", "Batch", false, [], "Parser benchmark for multi-statement scripts and batch splitting."),
        new(BenchmarkFeatureId.JsonInsertCast, "JSON insert cast", "Json", true, [], "JSON insert/extract flow including type coercion and cast overhead."),
        new(BenchmarkFeatureId.RowCountInBatch, "Row count in batch", "Batch", true, [], "Rowcount tracking across batch statements."),
        new(BenchmarkFeatureId.PivotCount, "Pivot count", "AdvancedQuery", true, [], "Backlog benchmark for PIVOT/UNPIVOT style transformation."),
        new(BenchmarkFeatureId.ReturningInsert, "Returning insert", "AdvancedQuery", true, [], "INSERT ... RETURNING / OUTPUT benchmark."),
        new(BenchmarkFeatureId.ReturningUpdate, "Returning update", "AdvancedQuery", true, [], "UPDATE ... RETURNING / OUTPUT benchmark."),
        new(BenchmarkFeatureId.MergeBasic, "Merge basic", "AdvancedQuery", true, [], "Backlog benchmark for MERGE-based upsert flows."),
        new(BenchmarkFeatureId.PartitionPruningSelect, "Partition pruning select", "AdvancedQuery", true, [], "Backlog benchmark for partition-pruned read paths."),
        new(BenchmarkFeatureId.ExecutionPlan, "Execution plan / LastExecutionPlan", "Diagnostics", false, ["1.5.1", "1.5.2", "1.5.3"], "Mock-only backlog seed for the wiki matrix."),
        new(BenchmarkFeatureId.ExecutionPlan, "Execution plan / LastExecutionPlan", "Observability", false, ["1.5.1", "1.5.2", "1.5.3"], "Catalogado para a wiki, mas não entra na comparação mock-vs-real desta primeira malha."),
    ];
}
