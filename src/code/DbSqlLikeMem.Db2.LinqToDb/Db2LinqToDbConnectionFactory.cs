using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Db2.LinqToDb;

/// <summary>
/// EN: Creates opened Db2 mock connections for LinqToDB integration entry points.
/// PT: Cria conexões simulado Db2 abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class Db2LinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates a Db2 LinqToDB connection factory without additional interception.
    /// PT: Cria uma factory de conexao Db2 para LinqToDB sem interceptacao adicional.
    /// </summary>
    public Db2LinqToDbConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates a Db2 LinqToDB connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao Db2 para LinqToDB que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public Db2LinqToDbConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new Db2ConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates a Db2 LinqToDB connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao Db2 para LinqToDB que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public Db2LinqToDbConnectionFactory(DbInterceptionOptions options)
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
