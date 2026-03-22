using DbSqlLikeMem.SqlServer;
using DbSqlLikeMem.SqlServer.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs SQL Server benchmark scenarios against the in-memory DbSqlLikeMem SQL Server mock provider.
/// PT-br: Executa cenários de benchmark de SQL Server contra o provedor mock em memória DbSqlLikeMem de SQL Server.
/// </summary>
public sealed class SqlServerDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqlServerProviderSqlDialect())
{
    private readonly SqlServerDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem SQL Server mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de SQL Server.
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        return new SqlServerConnectionMock(Db);
    }
}
