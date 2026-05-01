using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Runs DbSqlLikeMem benchmarks against the MariaDB in-memory mock session.
/// PT-br: Executa benchmarks do DbSqlLikeMem contra a sessao mock em memoria de MariaDB.
/// </summary>
public class MariaDb_DbSqlLikeMem_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the MariaDB in-memory benchmark session.
    /// PT-br: Cria a sessao de benchmark em memoria de MariaDB.
    /// </summary>
    protected override IBenchmarkSession CreateSession()
        => new MariaDbDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}
