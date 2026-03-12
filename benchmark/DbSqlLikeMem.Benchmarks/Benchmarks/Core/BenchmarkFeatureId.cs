namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
public enum BenchmarkFeatureId
{
    /// <summary>
    /// 
    /// </summary>
    ConnectionOpen,
    /// <summary>
    /// 
    /// </summary>
    CreateSchema,
    /// <summary>
    /// 
    /// </summary>
    InsertSingle,
    /// <summary>
    /// 
    /// </summary>
    InsertBatch100,
    /// <summary>
    /// 
    /// </summary>
    InsertBatch100Parallel,
    /// <summary>
    /// 
    /// </summary>
    SelectByPk,
    /// <summary>
    /// 
    /// </summary>
    SelectJoin,
    /// <summary>
    /// 
    /// </summary>
    UpdateByPk,
    /// <summary>
    /// 
    /// </summary>
    DeleteByPk,
    /// <summary>
    /// 
    /// </summary>
    TransactionCommit,
    /// <summary>
    /// 
    /// </summary>
    TransactionRollback,
    /// <summary>
    /// 
    /// </summary>
    Upsert,
    /// <summary>
    /// 
    /// </summary>
    SequenceNextValue,
    /// <summary>
    /// 
    /// </summary>
    StringAggregate,
    /// <summary>
    /// 
    /// </summary>
    DateScalar,
    /// <summary>
    /// 
    /// </summary>
    ExecutionPlan
}
