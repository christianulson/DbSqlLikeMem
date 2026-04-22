using DbSqlLikeMem.MariaDb;
using DbSqlLikeMem.MariaDb.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs MariaDB benchmark scenarios against the in-memory DbSqlLikeMem MariaDB mock provider.
/// PT-br: Executa cenarios de benchmark de MariaDB contra o provedor mock em memoria DbSqlLikeMem de MariaDB.
/// </summary>
internal sealed class MariaDbDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new MariaDbProviderSqlDialect())
{
    private readonly MariaDbDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem MariaDB mock connection.
    /// PT-br: Cria uma nova conexao mock DbSqlLikeMem de MariaDB.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem MariaDB mock connection. PT-br: Uma nova conexao mock DbSqlLikeMem de MariaDB.</returns>
    protected override DbConnection CreateConnection()
        => new MariaDbConnectionMock(Db);
}
