using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for Npgsql.
/// PT: Define a suite de benchmark DbSqlLikeMem para Npgsql.
/// </summary>
public class Npgsql_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the Npgsql DbSqlLikeMem suite.
    /// PT: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de Npgsql.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new NpgsqlDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}

