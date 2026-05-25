using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the Npgsql benchmark suite backed by Testcontainers.
/// PT-br: Define a suite de benchmark de Npgsql apoiada por Testcontainers.
/// </summary>
public class Npgsql_Testcontainers_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the Npgsql Testcontainers suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite Npgsql Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new NpgsqlTestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

