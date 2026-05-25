using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Auto;

/// <summary>
/// EN: Provides dependency injection extensions for registering AutoDbMock instances.
/// PT-br: Fornece extensões de injeção de dependência para registrar instâncias de AutoDbMock.
/// </summary>
public static class ServiceCollectionAutoDbMockExtensions
{
    /// <summary>
    /// EN: Registers AutoDbMock as a singleton service.
    /// PT-br: Registra AutoDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for AutoDbMock. PT-br: Versão opcional de dialeto para o AutoDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddAutoDbMockSingleton(
        this IServiceCollection services,
        Action<AutoDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new AutoDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers AutoDbMock as a scoped service.
    /// PT-br: Registra AutoDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for AutoDbMock. PT-br: Versão opcional de dialeto para o AutoDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddAutoDbMockScoped(
        this IServiceCollection services,
        Action<AutoDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new AutoDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers AutoDbMock as a transient service.
    /// PT-br: Registra AutoDbMock como serviço transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for AutoDbMock. PT-br: Versão opcional de dialeto para o AutoDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddAutoDbMockTransient(
        this IServiceCollection services,
        Action<AutoDbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new AutoDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
