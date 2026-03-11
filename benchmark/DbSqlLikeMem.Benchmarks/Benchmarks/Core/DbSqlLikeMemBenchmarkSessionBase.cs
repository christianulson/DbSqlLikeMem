namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
public abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{

}
