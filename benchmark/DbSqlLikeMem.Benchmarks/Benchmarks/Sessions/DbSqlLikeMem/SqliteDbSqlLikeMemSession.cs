using DbSqlLikeMem.Sqlite;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// 
/// </summary>
public sealed class SqliteDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqliteDialect())
{
    private readonly SqliteDbMock Db = [];
    /// <summary>
    /// 
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        return new SqliteConnectionMock(Db);
    }
}
