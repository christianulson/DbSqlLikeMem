namespace DbSqlLikeMem;

/// <summary>
/// EN: Adds convenience helpers for wrapping ADO.NET connections with interceptors.
/// PT: Adiciona helpers de conveniencia para encapsular conexoes ADO.NET com interceptors.
/// </summary>
public static class DbInterceptionPipelineExtensions
{
    /// <summary>
    /// EN: Wraps the connection with the supplied interceptors.
    /// PT: Encapsula a conexao com os interceptors informados.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="interceptors">EN: Interceptors applied in registration order. PT: Interceptors aplicados na ordem de registro.</param>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    public static DbConnection WithInterceptors(
        this DbConnection connection,
        params DbConnectionInterceptor[] interceptors)
        => DbInterceptionPipeline.Wrap(connection, interceptors);
}
