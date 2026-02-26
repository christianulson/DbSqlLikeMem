using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Sqlite.EfCore;

/// <summary>
/// EN: Creates opened Sqlite mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Sqlite abertas para pontos de integração com EF Core.
/// </summary>
public sealed class SqliteEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Sqlite mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Sqlite apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock(new SqliteDbMock());
        connection.Open();
        return connection;
    }
}
