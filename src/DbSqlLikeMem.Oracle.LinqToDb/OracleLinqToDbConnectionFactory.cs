using DbSqlLikeMem.Oracle;
using DbSqlLikeMem.LinqToDb;

namespace DbSqlLikeMem.Oracle.LinqToDb;

/// <summary>
/// EN: Creates opened Oracle mock connections for LinqToDB integration entry points.
/// PT: Cria conexões mock Oracle abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class OracleLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Oracle mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão mock Oracle apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new OracleConnectionMock(new OracleDbMock());
        connection.Open();
        return connection;
    }
}
