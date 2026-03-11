using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// 
/// </summary>
public class Oracle_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new OracleTestcontainersSession();

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ConnectionOpen() => Run(BenchmarkFeatureId.ConnectionOpen);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CreateSchema() => Run(BenchmarkFeatureId.CreateSchema);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertSingle() => Run(BenchmarkFeatureId.InsertSingle);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertBatch100() => Run(BenchmarkFeatureId.InsertBatch100);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectByPk() => Run(BenchmarkFeatureId.SelectByPk);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectJoin() => Run(BenchmarkFeatureId.SelectJoin);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void UpdateByPk() => Run(BenchmarkFeatureId.UpdateByPk);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void DeleteByPk() => Run(BenchmarkFeatureId.DeleteByPk);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionCommit() => Run(BenchmarkFeatureId.TransactionCommit);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionRollback() => Run(BenchmarkFeatureId.TransactionRollback);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Upsert() => Run(BenchmarkFeatureId.Upsert);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregate() => Run(BenchmarkFeatureId.StringAggregate);

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void DateScalar() => Run(BenchmarkFeatureId.DateScalar);
}
