using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.SqlServer.EfCore;

/// <summary>
/// EN: Creates opened SqlServer mock connections for EF Core integration entry points.
/// PT: Cria conexões mock SqlServer abertas para pontos de integração com EF Core.
/// </summary>
public sealed class SqlServerEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a SqlServer mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão mock SqlServer apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new SqlServerConnectionMock(new SqlServerDbMock());
        connection.Open();
        return connection;
    }
}
