using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.SqlServer.EfCore;

/// <summary>
/// EN: Creates opened SqlServer mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado SqlServer abertas para pontos de integração com EF Core.
/// </summary>
public sealed class SqlServerEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates a SqlServer EF Core connection factory without additional interception.
    /// PT: Cria uma factory de conexao SqlServer para EF Core sem interceptacao adicional.
    /// </summary>
    public SqlServerEfCoreConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates a SqlServer EF Core connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao SqlServer para EF Core que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public SqlServerEfCoreConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new SqlServerConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates a SqlServer EF Core connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao SqlServer para EF Core que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public SqlServerEfCoreConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new SqlServerConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a SqlServer mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada SqlServer apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new SqlServerConnectionMock([]);
        connection.Open();
        return connection;
    }
}
