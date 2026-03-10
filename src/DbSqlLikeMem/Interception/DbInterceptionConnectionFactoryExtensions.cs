namespace DbSqlLikeMem;

/// <summary>
/// EN: Adds convenience helpers for creating interception connection factories.
/// PT: Adiciona helpers de conveniencia para criar factories de conexao com interceptacao.
/// </summary>
public static class DbInterceptionConnectionFactoryExtensions
{
    /// <summary>
    /// EN: Creates an interception factory from a connection delegate and explicit interceptors.
    /// PT: Cria uma factory de interceptacao a partir de um delegate de conexao e interceptors explicitos.
    /// </summary>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    /// <returns>EN: Interception connection factory. PT: Factory de conexao com interceptacao.</returns>
    public static IDbInterceptionConnectionFactory WithInterceptionFactory(
        this Func<DbConnection> connectionFactory,
        params DbConnectionInterceptor[] interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        return new DbInterceptionConnectionFactory(connectionFactory, () => interceptors);
    }

    /// <summary>
    /// EN: Creates an interception factory from a connection delegate and a prebuilt interception options instance.
    /// PT: Cria uma factory de interceptacao a partir de um delegate de conexao e de uma instancia pronta de opcoes de interceptacao.
    /// </summary>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    /// <returns>EN: Interception connection factory. PT: Factory de conexao com interceptacao.</returns>
    public static IDbInterceptionConnectionFactory WithInterceptionFactory(
        this Func<DbConnection> connectionFactory,
        DbInterceptionOptions options)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(options, nameof(options));
        return new DbInterceptionConnectionFactory(connectionFactory, options);
    }

    /// <summary>
    /// EN: Creates an interception factory from a connection delegate and interception options.
    /// PT: Cria uma factory de interceptacao a partir de um delegate de conexao e opcoes de interceptacao.
    /// </summary>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="configure">EN: Options configuration. PT: Configuracao das opcoes.</param>
    /// <returns>EN: Interception connection factory. PT: Factory de conexao com interceptacao.</returns>
    public static IDbInterceptionConnectionFactory WithInterceptionFactory(
        this Func<DbConnection> connectionFactory,
        Action<DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        var options = new DbInterceptionOptions();
        configure(options);
        return new DbInterceptionConnectionFactory(connectionFactory, options);
    }
}
