using DbSqlLikeMem.EfCore;
using DbSqlLikeMem.MySql;
namespace DbSqlLikeMem.MySql.EfCore;

/// <summary>
/// EN: Creates opened MySql mock connections for EF Core integration entry points.
/// PT: Cria conexões mock MySql abertas para pontos de integração com EF Core.
/// </summary>
public sealed class MySqlEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a MySql mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão mock MySql apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new MySqlConnectionMock(new MySqlDbMock());
        connection.Open();
        return connection;
    }
}
