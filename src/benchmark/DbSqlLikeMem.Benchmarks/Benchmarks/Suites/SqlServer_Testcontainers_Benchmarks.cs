using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the SQL Server benchmark suite backed by Testcontainers.
/// PT: Define a suite de benchmark de SQL Server apoiada por Testcontainers.
/// </summary>
public class SqlServer_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the SQL Server Testcontainers suite.
    /// PT: Cria a sessao de benchmark usada pela suite SQL Server Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqlServerTestcontainersSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

