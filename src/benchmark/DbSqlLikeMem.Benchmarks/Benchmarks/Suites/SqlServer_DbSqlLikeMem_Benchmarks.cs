using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for SQL Server.
/// PT: Define a suite de benchmark DbSqlLikeMem para SQL Server.
/// </summary>
public class SqlServer_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the SQL Server DbSqlLikeMem suite.
    /// PT: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de SQL Server.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqlServerDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

