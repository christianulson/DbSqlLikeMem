using DbSqlLikeMem.MySql;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs MySQL benchmark scenarios against the in-memory DbSqlLikeMem MySQL mock provider.
/// PT-br: Executa cenários de benchmark de MySQL contra o provedor mock em memória DbSqlLikeMem de MySQL.
/// </summary>
public sealed class MySqlDbSqlLikeMemSession() 
    : DbSqlLikeMemBenchmarkSessionBase(new MySqlDialect())
{
    private readonly MySqlDbMock Db = [];

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem MySQL mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de MySQL.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem MySQL mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de MySQL.</returns>
    protected override DbConnection CreateConnection()
    {
        return new MySqlConnectionMock(Db);
    }
}
