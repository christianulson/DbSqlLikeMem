namespace DbSqlLikeMem;

/// <summary>
/// EN: Creates intercepted ADO.NET connections from an inner connection factory.
/// PT: Cria conexoes ADO.NET interceptadas a partir de uma factory interna de conexoes.
/// </summary>
public sealed class DbInterceptionConnectionFactory : IDbInterceptionConnectionFactory
{
    private readonly Func<DbConnection> _connectionFactory;
    private readonly Func<DbConnectionInterceptor[]> _interceptorsFactory;

    /// <summary>
    /// EN: Creates a factory that wraps connections produced by the supplied delegate.
    /// PT: Cria uma factory que encapsula conexoes produzidas pelo delegate informado.
    /// </summary>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="interceptorsFactory">EN: Delegate that provides the interceptors for each connection. PT: Delegate que fornece os interceptors para cada conexao.</param>
    public DbInterceptionConnectionFactory(
        Func<DbConnection> connectionFactory,
        Func<DbConnectionInterceptor[]> interceptorsFactory)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptorsFactory, nameof(interceptorsFactory));
        _connectionFactory = connectionFactory;
        _interceptorsFactory = interceptorsFactory;
    }

    /// <summary>
    /// EN: Creates a factory that wraps connections using interceptors built from the supplied options.
    /// PT: Cria uma factory que encapsula conexoes usando interceptors construidos a partir das opcoes informadas.
    /// </summary>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    public DbInterceptionConnectionFactory(
        Func<DbConnection> connectionFactory,
        DbInterceptionOptions options)
        : this(connectionFactory, options.BuildInterceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(options, nameof(options));
    }

    /// <inheritdoc />
    public DbConnection CreateConnection()
        => DbInterceptionPipeline.Wrap(_connectionFactory(), _interceptorsFactory());

    /// <inheritdoc />
    public DbConnection CreateOpenConnection()
    {
        var connection = CreateConnection();
        connection.Open();
        return connection;
    }
}
