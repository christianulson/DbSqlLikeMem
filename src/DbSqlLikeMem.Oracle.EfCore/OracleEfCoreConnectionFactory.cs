using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Oracle.EfCore;

/// <summary>
/// EN: Creates opened Oracle mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Oracle abertas para pontos de integração com EF Core.
/// </summary>
public sealed class OracleEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a Oracle mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Oracle apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new OracleConnectionMock(new OracleDbMock());
        connection.Open();
        return connection;
    }
}
