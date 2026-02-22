using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Npgsql.LinqToDb;

/// <summary>
/// EN: Creates opened Npgsql mock connections for LinqToDB integration entry points.
/// PT: Cria conexões simulado Npgsql abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class NpgsqlLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
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
