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
        new(BenchmarkFeatureId.CreateSchema, "Create schema", "Core", true, ["1.2.1", "1.4.1"]),
        new(BenchmarkFeatureId.InsertSingle, "Insert single row", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.InsertBatch100, "Insert batch 100", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.SelectByPk, "Select by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.SelectJoin, "Join select", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.UpdateByPk, "Update by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.DeleteByPk, "Delete by PK", "Core", true, ["1.2.2", "1.3.2"]),
        new(BenchmarkFeatureId.TransactionCommit, "Transaction commit", "Transactions", true, ["1.1.2", "1.3.2"]),
        new(BenchmarkFeatureId.TransactionRollback, "Transaction rollback", "Transactions", true, ["1.1.2", "1.3.2"]),
        new(BenchmarkFeatureId.Upsert, "Upsert", "Dialect", true, ["1.2.2", "3.1.2", "3.4.2", "3.6.2"], "MySQL usa ON DUPLICATE, PostgreSQL/SQLite usam ON CONFLICT e SQL Server/Oracle/DB2 usam MERGE."),
        new(BenchmarkFeatureId.SequenceNextValue, "Sequence next value", "Dialect", true, ["1.1.1", "1.2.2", "3.2.2", "3.3.2", "3.4.2", "3.6.2"]),
        new(BenchmarkFeatureId.StringAggregate, "String aggregate", "Dialect", true, ["1.2.5", "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", "3.6.2"]),
        new(BenchmarkFeatureId.DateScalar, "Date scalar", "Dialect", true, ["1.2.6"]),
        new(BenchmarkFeatureId.ExecutionPlan, "Execution plan / LastExecutionPlan", "Observability", false, ["1.5.1", "1.5.2", "1.5.3"], "Catalogado para a wiki, mas não entra na comparação mock-vs-real desta primeira malha."),
    ];
}
