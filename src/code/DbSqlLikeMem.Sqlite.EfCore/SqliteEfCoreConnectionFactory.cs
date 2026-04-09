using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Sqlite.EfCore;

/// <summary>
/// EN: Creates opened Sqlite mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Sqlite abertas para pontos de integração com EF Core.
/// </summary>
public sealed class SqliteEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates a Sqlite EF Core connection factory without additional interception.
    /// PT: Cria uma factory de conexao Sqlite para EF Core sem interceptacao adicional.
    /// </summary>
    public SqliteEfCoreConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates a Sqlite EF Core connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao Sqlite para EF Core que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public SqliteEfCoreConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new SqliteConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates a Sqlite EF Core connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao Sqlite para EF Core que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public SqliteEfCoreConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new SqliteConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a Sqlite mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Sqlite apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new SqliteConnectionMock([]);
        connection.Open();
        return connection;
    }
}
