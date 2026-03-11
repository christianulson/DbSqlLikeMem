using DbSqlLikeMem.Oracle;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs Oracle benchmark scenarios against the in-memory DbSqlLikeMem Oracle mock provider.
/// PT-br: Executa cenários de benchmark de Oracle contra o provedor mock em memória DbSqlLikeMem de Oracle.
/// </summary>
public sealed class OracleDbSqlLikeMemSession() : DbSqlLikeMemBenchmarkSessionBase(new OracleDialect())
{

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem Oracle mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de Oracle.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem Oracle mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de Oracle.</returns>
    protected override DbConnection CreateConnection()
    {
        return new OracleConnectionMock();
    }
}
