using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Applies dependency-injection registered interceptors to ADO.NET connections.
/// PT: Aplica interceptors registrados em injecao de dependencia a conexoes ADO.NET.
/// </summary>
public static class DbInterceptionServiceProviderExtensions
{
    internal static DbConnectionInterceptor[] ResolveRegisteredInterceptors(IServiceProvider serviceProvider)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceProvider, nameof(serviceProvider));

        var interceptors = new List<DbConnectionInterceptor>();

        foreach (var interceptor in serviceProvider.GetServices<DbConnectionInterceptor>())
        {
            if (!interceptors.Any(existing => ReferenceEquals(existing, interceptor)))
                interceptors.Add(interceptor);
        }

        foreach (var interceptor in serviceProvider.GetService<IReadOnlyList<DbConnectionInterceptor>>() ?? [])
        {
            if (!interceptors.Any(existing => ReferenceEquals(existing, interceptor)))
                interceptors.Add(interceptor);
        }

        return interceptors.ToArray();
    }

    /// <summary>
    /// EN: Wraps the connection with all interceptors registered in the service provider.
    /// PT: Encapsula a conexao com todos os interceptors registrados no service provider.
    /// </summary>
    /// <param name="connection">EN: Connection to wrap. PT: Conexao a encapsular.</param>
    /// <param name="serviceProvider">EN: Service provider used to resolve interceptors. PT: Service provider usado para resolver interceptors.</param>
    /// <returns>EN: Wrapped connection when interceptors exist; otherwise the original connection. PT: Conexao encapsulada quando houver interceptors; caso contrario, a conexao original.</returns>
    public static DbConnection WithRegisteredInterceptors(
        this DbConnection connection,
        IServiceProvider serviceProvider)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(serviceProvider, nameof(serviceProvider));

        var interceptors = ResolveRegisteredInterceptors(serviceProvider);
        return interceptors.Length == 0
            ? connection
            : DbInterceptionPipeline.Wrap(connection, interceptors);
    }
}
