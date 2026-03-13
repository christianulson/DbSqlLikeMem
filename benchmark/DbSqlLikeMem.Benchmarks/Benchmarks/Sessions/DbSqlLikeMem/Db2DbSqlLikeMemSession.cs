using DbSqlLikeMem.Db2;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs DB2 benchmark scenarios against the in-memory DbSqlLikeMem DB2 mock provider.
/// PT-br: Executa cenários de benchmark de DB2 contra o provedor mock em memória DbSqlLikeMem de DB2.
/// </summary>
public sealed class Db2DbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new Db2Dialect())
{
    private readonly Db2DbMock Db = [];

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem DB2 mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de DB2.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem DB2 mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de DB2.</returns>
    protected override DbConnection CreateConnection()
    {
        return new Db2ConnectionMock(Db);
    }
}
