using DbSqlLikeMem.EfCore;
using System.Data.Common;

namespace DbSqlLikeMem.Firebird.EfCore;

/// <summary>
/// EN: Creates opened Firebird mock connections for EF Core integration entry points.
/// PT-br: Cria conexoes simuladas Firebird abertas para pontos de integracao com EF Core.
/// </summary>
public sealed class FirebirdEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates a Firebird EF Core connection factory without extra interception.
    /// PT-br: Cria uma fabrica de conexao Firebird para EF Core sem interceptacao adicional.
    /// </summary>
    public FirebirdEfCoreConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates and opens a Firebird mock connection backed by an in-memory database.
    /// PT-br: Cria e abre uma conexao simulada Firebird apoiada por um banco em memoria.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        var connection = new FirebirdConnectionMock([]);
        connection.Open();
        return connection;
    }
}
