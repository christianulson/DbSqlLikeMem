using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the Oracle benchmark suite backed by Testcontainers.
/// PT-br: Define a suite de benchmark de Oracle apoiada por Testcontainers.
/// </summary>
public class Oracle_Testcontainers_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the Oracle Testcontainers suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite Oracle Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new OracleTestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

