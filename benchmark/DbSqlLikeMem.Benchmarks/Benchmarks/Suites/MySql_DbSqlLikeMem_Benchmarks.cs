using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// 
/// </summary>public class MySql_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new MySqlDbSqlLikeMemSession();

}

