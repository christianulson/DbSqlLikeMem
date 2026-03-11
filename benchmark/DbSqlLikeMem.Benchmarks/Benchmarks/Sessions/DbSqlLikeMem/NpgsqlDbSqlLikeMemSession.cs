using DbSqlLikeMem.Npgsql;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs PostgreSQL/Npgsql benchmark scenarios against the in-memory DbSqlLikeMem mock provider.
/// PT-br: Executa cenários de benchmark de PostgreSQL/Npgsql contra o provedor mock em memória DbSqlLikeMem.
/// </summary>
public sealed class NpgsqlDbSqlLikeMemSession() : DbSqlLikeMemBenchmarkSessionBase(new NpgsqlDialect())
{
    /// <summary>
    /// EN: Creates a new DbSqlLikeMem PostgreSQL/Npgsql mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de PostgreSQL/Npgsql.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem PostgreSQL/Npgsql mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de PostgreSQL/Npgsql.</returns>
    protected override DbConnection CreateConnection()
    {
        return new NpgsqlConnectionMock();
    }
}
