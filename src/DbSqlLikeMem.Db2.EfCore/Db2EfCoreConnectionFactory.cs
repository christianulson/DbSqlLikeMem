using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Db2.EfCore;

/// <summary>
/// EN: Creates opened Db2 mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Db2 abertas para pontos de integração com EF Core.
/// </summary>
public sealed class Db2EfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates a Db2 EF Core connection factory without additional interception.
    /// PT: Cria uma factory de conexao Db2 para EF Core sem interceptacao adicional.
    /// </summary>
    public Db2EfCoreConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates a Db2 EF Core connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao Db2 para EF Core que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public Db2EfCoreConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new Db2ConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates a Db2 EF Core connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao Db2 para EF Core que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public Db2EfCoreConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new Db2ConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a Db2 mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Db2 apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new Db2ConnectionMock([]);
        connection.Open();
        return connection;
    }
}
