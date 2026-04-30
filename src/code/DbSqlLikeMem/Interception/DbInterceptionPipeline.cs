namespace DbSqlLikeMem;

/// <summary>
/// EN: Wraps ADO.NET connections with the DbSqlLikeMem interception pipeline.
/// PT: Encapsula conexoes ADO.NET com o pipeline de interceptacao do DbSqlLikeMem.
/// </summary>
public static class DbInterceptionPipeline
{
    /// <summary>
    /// EN: Wraps a connection so interceptors can observe and modify its commands and lifecycle.
    /// PT: Encapsula uma conexao para que interceptors observem e modifiquem seus comandos e ciclo de vida.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="interceptors">EN: Interceptors applied in registration order. PT: Interceptors aplicados na ordem de registro.</param>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    public static DbConnection Wrap(
        DbConnection connection,
        params DbConnectionInterceptor[] interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        return new InterceptingDbConnection(connection, interceptors);
    }

    /// <summary>
    /// EN: Wraps a connection using interceptors built from the supplied options object.
    /// PT: Encapsula uma conexao usando interceptors construidos a partir do objeto de opcoes informado.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    public static DbConnection Wrap(
        DbConnection connection,
        DbInterceptionOptions options)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(options, nameof(options));
        return new InterceptingDbConnection(connection, options.BuildInterceptors());
    }

    /// <summary>
    /// EN: Wraps a connection using interception options configured inline.
    /// PT: Encapsula uma conexao usando opcoes de interceptacao configuradas inline.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="configure">EN: Options configuration. PT: Configuracao das opcoes.</param>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    public static DbConnection Wrap(
        DbConnection connection,
        Action<DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        var options = new DbInterceptionOptions();
        configure(options);
        return Wrap(connection, options);
    }
}
