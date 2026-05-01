using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Firebird.LinqToDb;

/// <summary>
/// EN: Creates opened Firebird mock connections for LinqToDB integration entry points.
/// PT-br: Cria conexoes simuladas Firebird abertas para pontos de integracao com LinqToDB.
/// </summary>
public sealed class FirebirdLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    /// <summary>
    /// EN: Creates a Firebird LinqToDB connection factory without extra interception.
    /// PT-br: Cria uma fabrica de conexao Firebird para LinqToDB sem interceptacao adicional.
    /// </summary>
    public FirebirdLinqToDbConnectionFactory()
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
