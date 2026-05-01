using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for DB2.
/// PT: Define a suite de benchmark DbSqlLikeMem para DB2.
/// </summary>
public class Db2_DbSqlLikeMem_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the DB2 DbSqlLikeMem suite.
    /// PT: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de DB2.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new Db2DbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

