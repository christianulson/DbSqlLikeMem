using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.MySql.LinqToDb;

/// <summary>
/// EN: Creates opened MySql mock connections for LinqToDB integration entry points.
/// PT: Cria conexões simulado MySql abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class MySqlLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates a MySql LinqToDB connection factory without additional interception.
    /// PT: Cria uma factory de conexao MySql para LinqToDB sem interceptacao adicional.
    /// </summary>
    public MySqlLinqToDbConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates a MySql LinqToDB connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao MySql para LinqToDB que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public MySqlLinqToDbConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new MySqlConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates a MySql LinqToDB connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao MySql para LinqToDB que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public MySqlLinqToDbConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new MySqlConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a MySql mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada MySql apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new MySqlConnectionMock([]);
        connection.Open();
        return connection;
    }
}
