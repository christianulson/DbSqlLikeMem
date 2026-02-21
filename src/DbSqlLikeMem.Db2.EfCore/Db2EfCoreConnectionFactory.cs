using DbSqlLikeMem.EfCore;
using DbSqlLikeMem.Db2;
namespace DbSqlLikeMem.Db2.EfCore;

/// <summary>
/// EN: Creates opened Db2 mock connections for EF Core integration entry points.
/// PT: Cria conexões mock Db2 abertas para pontos de integração com EF Core.
/// </summary>
public sealed class Db2EfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Db2 mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão mock Db2 apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock(new Db2DbMock());
        connection.Open();
        return connection;
    }
}
