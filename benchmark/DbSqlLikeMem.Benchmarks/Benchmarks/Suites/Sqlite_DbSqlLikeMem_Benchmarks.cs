using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// 
/// </summary>public class Sqlite_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqliteDbSqlLikeMemSession();

}

