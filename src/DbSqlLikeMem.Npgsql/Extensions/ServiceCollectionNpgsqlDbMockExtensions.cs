using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Provides dependency injection extensions for registering NpgsqlDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de NpgsqlDbMock.
/// </summary>
public static class ServiceCollectionNpgsqlDbMockExtensions
{
    /// <summary>
    /// EN: Registers NpgsqlDbMock as a singleton service.
    /// PT: Registra NpgsqlDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for NpgsqlDbMock. PT: Versão opcional de dialeto para o NpgsqlDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddNpgsqlDbMockSingleton(
        this IServiceCollection services,
        Action<NpgsqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new NpgsqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers NpgsqlDbMock as a scoped service.
    /// PT: Registra NpgsqlDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for NpgsqlDbMock. PT: Versão opcional de dialeto para o NpgsqlDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddNpgsqlDbMockScoped(
        this IServiceCollection services,
        Action<NpgsqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new NpgsqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
