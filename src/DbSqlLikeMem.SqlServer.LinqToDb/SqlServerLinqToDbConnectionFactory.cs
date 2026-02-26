using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.SqlServer.LinqToDb;

/// <summary>
/// EN: Creates opened SqlServer mock connections for LinqToDB integration entry points.
/// PT: Cria conexões simulado SqlServer abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class SqlServerLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a SqlServer mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada SqlServer apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new SqlServerConnectionMock(new SqlServerDbMock());
        connection.Open();
        return connection;
    }
}
