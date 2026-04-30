using DbSqlLikeMem.LinqToDb;
using System.Data.Common;

namespace DbSqlLikeMem.Npgsql.LinqToDb;

/// <summary>
/// EN: Creates opened Npgsql mock connections for LinqToDB integration entry points.
/// PT: Cria conexões simulado Npgsql abertas para pontos de integração com LinqToDB.
/// </summary>
public sealed class NpgsqlLinqToDbConnectionFactory : IDbSqlLikeMemLinqToDbConnectionFactory
{
    private readonly IDbInterceptionConnectionFactory? _interceptionFactory;

    /// <summary>
    /// EN: Creates an Npgsql LinqToDB connection factory without additional interception.
    /// PT: Cria uma factory de conexao Npgsql para LinqToDB sem interceptacao adicional.
    /// </summary>
    public NpgsqlLinqToDbConnectionFactory()
    {
    }

    /// <summary>
    /// EN: Creates an Npgsql LinqToDB connection factory that wraps each created connection with explicit interceptors.
    /// PT: Cria uma factory de conexao Npgsql para LinqToDB que encapsula cada conexao criada com interceptors explicitos.
    /// </summary>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    public NpgsqlLinqToDbConnectionFactory(params DbConnectionInterceptor[] interceptors)
        => _interceptionFactory = new Func<DbConnection>(() => new NpgsqlConnectionMock([]))
            .WithInterceptionFactory(interceptors);

    /// <summary>
    /// EN: Creates an Npgsql LinqToDB connection factory that wraps each created connection using interception options.
    /// PT: Cria uma factory de conexao Npgsql para LinqToDB que encapsula cada conexao criada usando opcoes de interceptacao.
    /// </summary>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public NpgsqlLinqToDbConnectionFactory(DbInterceptionOptions options)
        => _interceptionFactory = new DbInterceptionConnectionFactory(
            () => new NpgsqlConnectionMock([]),
            options);

    /// <summary>
    /// EN: Creates and opens a Npgsql mock connection backed by an in-memory DbSqlLikeMem database.
    /// PT: Cria e abre uma conexão simulada Npgsql apoiada por um banco em memória do DbSqlLikeMem.
    /// </summary>
    public DbConnection CreateOpenConnection()
    {
        if (_interceptionFactory is not null)
            return _interceptionFactory.CreateOpenConnection();

        var connection = new NpgsqlConnectionMock([]);
        connection.Open();
        return connection;
    }
}
