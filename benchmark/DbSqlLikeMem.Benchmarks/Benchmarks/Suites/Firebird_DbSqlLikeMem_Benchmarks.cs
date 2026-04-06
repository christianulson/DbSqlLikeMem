using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for Firebird.
/// PT: Define a suite de benchmark DbSqlLikeMem para Firebird.
/// </summary>
public class Firebird_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <inheritdoc />
    protected override IBenchmarkSession CreateSession() => new FirebirdDbSqlLikeMemSession();
}
