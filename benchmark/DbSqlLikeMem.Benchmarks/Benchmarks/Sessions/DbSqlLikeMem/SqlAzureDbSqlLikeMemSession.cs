using DbSqlLikeMem.SqlAzure;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs SQL Azure benchmark scenarios against the in-memory DbSqlLikeMem SQL Azure mock provider.
/// PT-br: Executa cenários de benchmark de SQL Azure contra o provedor mock em memória DbSqlLikeMem de SQL Azure.
/// </summary>
public sealed class SqlAzureDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqlAzureDialect())
{
    private readonly SqlAzureDbMock Db = [];
    /// <summary>
    /// EN: Creates a new DbSqlLikeMem SQL Azure mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de SQL Azure.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem SQL Azure mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de SQL Azure.</returns>
    protected override DbConnection CreateConnection()
    {
        return new SqlAzureConnectionMock(Db);
    }
}
