using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for Oracle.
/// PT: Define a suite de benchmark DbSqlLikeMem para Oracle.
/// </summary>
public class Oracle_DbSqlLikeMem_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the Oracle DbSqlLikeMem suite.
    /// PT: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de Oracle.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new OracleDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

