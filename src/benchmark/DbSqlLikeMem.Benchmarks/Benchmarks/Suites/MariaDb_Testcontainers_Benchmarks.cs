using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Runs benchmark scenarios against a MariaDB container managed by Testcontainers.
/// PT-br: Executa cenarios de benchmark contra um contêiner de MariaDB gerenciado pelo Testcontainers.
/// </summary>
public class MariaDb_Testcontainers_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the MariaDB Testcontainers benchmark session.
    /// PT-br: Cria a sessao de benchmark MariaDB do Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession()
        => new MariaDbTestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}
