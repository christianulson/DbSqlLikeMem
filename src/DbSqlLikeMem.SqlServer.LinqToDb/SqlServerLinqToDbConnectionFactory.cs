using DbSqlLikeMem.SqlServer;
using DbSqlLikeMem.LinqToDb;

namespace DbSqlLikeMem.SqlServer.LinqToDb;

/// <summary>
/// EN: Creates opened SqlServer mock connections for LinqToDB integration entry points.
/// PT: Cria conexões mock SqlServer abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class SqlServerLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
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
