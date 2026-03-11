using DbSqlLikeMem.Sqlite;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// 
/// </summary>
public sealed class SqliteDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqliteDialect())
{

    /// <summary>
    /// 
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        return new SqliteConnectionMock();
    }
}
