using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Provides dependency injection extensions for registering OracleDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de OracleDbMock.
/// </summary>
public static class ServiceCollectionOracleDbMockExtensions
{
    /// <summary>
    /// EN: Registers OracleDbMock as a singleton service.
    /// PT: Registra OracleDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for OracleDbMock. PT: Versão opcional de dialeto para o OracleDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddOracleDbMockSingleton(
        this IServiceCollection services,
        Action<OracleDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new OracleDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers OracleDbMock as a scoped service.
    /// PT: Registra OracleDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for OracleDbMock. PT: Versão opcional de dialeto para o OracleDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddOracleDbMockScoped(
        this IServiceCollection services,
        Action<OracleDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new OracleDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers OracleDbMock as a transient service.
    /// PT: Registra OracleDbMock como serviço transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for OracleDbMock. PT: Versão opcional de dialeto para o OracleDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddOracleDbMockTransient(
        this IServiceCollection services,
        Action<OracleDbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new OracleDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
