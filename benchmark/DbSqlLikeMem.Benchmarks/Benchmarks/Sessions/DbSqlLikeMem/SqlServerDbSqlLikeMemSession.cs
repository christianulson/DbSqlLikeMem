using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// 
/// </summary>
public sealed class SqlServerDbSqlLikeMemSession() 
    : DbSqlLikeMemBenchmarkSessionBase(new SqlServerDialect())
{
    private readonly SqlServerDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// 
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        return new SqlServerConnectionMock(Db);
    }
}
