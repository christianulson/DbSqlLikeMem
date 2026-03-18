using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// 
/// </summary>public class Oracle_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new OracleTestcontainersSession();

    /// <summary>
    /// 
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

