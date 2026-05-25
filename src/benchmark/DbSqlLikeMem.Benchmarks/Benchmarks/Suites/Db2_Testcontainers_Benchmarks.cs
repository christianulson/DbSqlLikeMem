using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DB2 benchmark suite backed by Testcontainers.
/// PT-br: Define a suite de benchmark de DB2 apoiada por Testcontainers.
/// </summary>
public class Db2_Testcontainers_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the DB2 Testcontainers suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite DB2 Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new Db2TestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

