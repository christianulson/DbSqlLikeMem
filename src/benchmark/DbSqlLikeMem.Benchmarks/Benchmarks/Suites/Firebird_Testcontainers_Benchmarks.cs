using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the Firebird benchmark suite backed by Testcontainers.
/// PT: Define a suite de benchmark de Firebird apoiada por Testcontainers.
/// </summary>
public class Firebird_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the Firebird Testcontainers suite.
    /// PT: Cria a sessao de benchmark usada pela suite Firebird Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new FirebirdTestcontainersSession();

    /// <summary>
    /// EN: Executes the Firebird EXECUTE BLOCK benchmark that handles SQLSTATE 23000.
    /// PT: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ExecuteBlockSqlState23000() => Run(BenchmarkFeatureId.ExecuteBlockSqlState23000);

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}
