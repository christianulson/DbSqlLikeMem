using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for SQL Azure.
/// PT-br: Define a suite de benchmark DbSqlLikeMem para SQL Azure.
/// </summary>
public class SqlAzure_DbSqlLikeMem_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the SQL Azure DbSqlLikeMem suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de SQL Azure.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqlAzureDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

