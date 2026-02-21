using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.MySql.LinqToDb;

/// <summary>
/// EN: Creates opened MySql mock connections for LinqToDB integration entry points.
/// PT: Cria conexões mock MySql abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class MySqlLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
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
