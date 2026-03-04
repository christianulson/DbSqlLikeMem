using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Provides dependency injection extensions for registering MySqlDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de MySqlDbMock.
/// </summary>
public static class ServiceCollectionMySqlDbMockExtensions
{
    /// <summary>
    /// EN: Registers MySqlDbMock as a singleton service.
    /// PT: Registra MySqlDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for MySqlDbMock. PT: Versão opcional de dialeto para o MySqlDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddMySqlDbMockSingleton(
        this IServiceCollection services,
        Action<MySqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new MySqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers MySqlDbMock as a scoped service.
    /// PT: Registra MySqlDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for MySqlDbMock. PT: Versão opcional de dialeto para o MySqlDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddMySqlDbMockScoped(
        this IServiceCollection services,
        Action<MySqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new MySqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
