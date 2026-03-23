using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DB2 benchmark suite backed by Testcontainers.
/// PT: Define a suite de benchmark de DB2 apoiada por Testcontainers.
/// </summary>
public class Db2_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the DB2 Testcontainers suite.
    /// PT: Cria a sessao de benchmark usada pela suite DB2 Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new Db2TestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

