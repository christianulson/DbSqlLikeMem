using DbSqlLikeMem.EfCore;
using System.Data.Common;
namespace DbSqlLikeMem.Oracle.EfCore;

/// <summary>
/// EN: Creates opened Oracle mock connections for EF Core integration entry points.
/// PT: Cria conexões simulado Oracle abertas para pontos de integração com EF Core.
/// </summary>
public sealed class OracleEfCoreConnectionFactory : IDbSqlLikeMemEfCoreConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates an Oracle EF Core connection factory without additional interception.
    /// PT: Cria uma factory de conexao Oracle para EF Core sem interceptacao adicional.
    /// </summary>
    public OracleEfCoreConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates an Oracle EF Core connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao Oracle para EF Core que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public OracleEfCoreConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new OracleConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates an Oracle EF Core connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao Oracle para EF Core que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public OracleEfCoreConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new OracleConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a Oracle mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Oracle apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new OracleConnectionMock([]);
        connection.Open();
        return connection;
    }
}
