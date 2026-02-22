using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Npgsql.EfCore;

/// <summary>
/// EN: Creates opened Npgsql mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Npgsql abertas para pontos de integração com EF Core.
/// </summary>
public sealed class NpgsqlEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Npgsql mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Npgsql apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new NpgsqlConnectionMock(new NpgsqlDbMock());
        connection.Open();
        return connection;
    }
}
