using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Db2.LinqToDb;

/// <summary>
/// EN: Creates opened Db2 mock connections for LinqToDB integration entry points.
/// PT: Cria conexões mock Db2 abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class Db2LinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
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
