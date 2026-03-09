namespace DbSqlLikeMem;

/// <summary>
/// EN: Adds convenience helpers for creating interception pipelines from options.
/// PT: Adiciona helpers de conveniencia para criar pipelines de interceptacao a partir de opcoes.
/// </summary>
public static class DbInterceptionOptionsExtensions
{
    /// <summary>
    /// EN: Wraps the connection with the interceptors built from the supplied options.
    /// PT: Encapsula a conexao com os interceptors construidos a partir das opcoes informadas.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="configure">EN: Options configuration. PT: Configuracao das opcoes.</param>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    public static DbConnection WithInterception(
        this DbConnection connection,
        Action<DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        var options = new DbInterceptionOptions();
        configure(options);
        return DbInterceptionPipeline.Wrap(connection, options);
    }
}
