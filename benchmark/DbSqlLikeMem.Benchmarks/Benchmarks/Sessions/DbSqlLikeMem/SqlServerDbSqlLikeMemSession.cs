using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// 
/// </summary>
public sealed class SqlServerDbSqlLikeMemSession() 
    : DbSqlLikeMemBenchmarkSessionBase(new SqlServerDialect())
{

    /// <summary>
    /// 
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        return new SqlServerConnectionMock();
    }
}
