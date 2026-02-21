using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.LinqToDb;

/// <summary>
/// EN: Creates opened Sqlite mock connections for LinqToDB integration entry points.
/// PT: Cria conexões mock Sqlite abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class SqliteLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
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
