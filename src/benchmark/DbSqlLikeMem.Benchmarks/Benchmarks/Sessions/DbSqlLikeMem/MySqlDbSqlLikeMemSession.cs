using DbSqlLikeMem.MySql;
using DbSqlLikeMem.MySql.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs MySQL benchmark scenarios against the in-memory DbSqlLikeMem MySQL mock provider.
/// PT-br: Executa cenários de benchmark de MySQL contra o provedor mock em memória DbSqlLikeMem de MySQL.
/// </summary>
internal sealed class MySqlDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new MySqlProviderSqlDialect())
{
    private readonly MySqlDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem MySQL mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de MySQL.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem MySQL mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de MySQL.</returns>
    protected override DbConnection CreateConnection()
        => new MySqlConnectionMock(Db);
}
