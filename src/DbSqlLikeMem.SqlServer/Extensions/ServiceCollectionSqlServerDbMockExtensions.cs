using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Provides dependency injection extensions for registering SqlServerDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de SqlServerDbMock.
/// </summary>
public static class ServiceCollectionSqlServerDbMockExtensions
{
    /// <summary>
    /// EN: Registers SqlServerDbMock as a singleton service.
    /// PT: Registra SqlServerDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for SqlServerDbMock. PT: Versão opcional de dialeto para o SqlServerDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqlServerDbMockSingleton(
        this IServiceCollection services,
        Action<SqlServerDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new SqlServerDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers SqlServerDbMock as a scoped service.
    /// PT: Registra SqlServerDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for SqlServerDbMock. PT: Versão opcional de dialeto para o SqlServerDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqlServerDbMockScoped(
        this IServiceCollection services,
        Action<SqlServerDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new SqlServerDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
