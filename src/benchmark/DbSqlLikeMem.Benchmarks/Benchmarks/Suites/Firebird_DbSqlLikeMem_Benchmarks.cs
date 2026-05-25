using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for Firebird.
/// PT-br: Define a suite de benchmark DbSqlLikeMem para Firebird.
/// </summary>
public class Firebird_DbSqlLikeMem_Benchmarks : SequenceBenchmarkSuiteBase
{
    /// <inheritdoc />
    protected override IBenchmarkSession CreateSession() => new FirebirdDbSqlLikeMemSession();

    /// <summary>
    /// EN: Executes the Firebird EXECUTE BLOCK benchmark that handles SQLSTATE 23000.
    /// PT-br: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("psql")]
    public void ExecuteBlockSqlState23000() => Run(BenchmarkFeatureId.ExecuteBlockSqlState23000);

    /// <summary>
    /// EN: Executes the sequence-next-value benchmark.
    /// PT-br: Executa o benchmark de sequence next value.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public new void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);
}
