using DbSqlLikeMem.EfCore;
using DbSqlLikeMem.Sqlite;
namespace DbSqlLikeMem.Sqlite.EfCore;

/// <summary>
/// EN: Creates opened Sqlite mock connections for EF Core integration entry points.
/// PT: Cria conexões mock Sqlite abertas para pontos de integração com EF Core.
/// </summary>
public sealed class SqliteEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Sqlite mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão mock Sqlite apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock(new SqliteDbMock());
        connection.Open();
        return connection;
    }
}
