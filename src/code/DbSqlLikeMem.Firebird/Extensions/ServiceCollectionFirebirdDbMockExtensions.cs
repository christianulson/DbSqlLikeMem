using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Provides dependency injection extensions for registering FirebirdDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de FirebirdDbMock.
/// </summary>
public static class ServiceCollectionFirebirdDbMockExtensions
{
    /// <summary>
    /// EN: Registers FirebirdDbMock as a singleton service.
    /// PT: Registra FirebirdDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for FirebirdDbMock. PT: Versão opcional de dialeto para o FirebirdDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddFirebirdDbMockSingleton(
        this IServiceCollection services,
        Action<FirebirdDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new FirebirdDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers FirebirdDbMock as a scoped service.
    /// PT: Registra FirebirdDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for FirebirdDbMock. PT: Versão opcional de dialeto para o FirebirdDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddFirebirdDbMockScoped(
        this IServiceCollection services,
        Action<FirebirdDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new FirebirdDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers FirebirdDbMock as a transient service.
    /// PT: Registra FirebirdDbMock como serviço transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for FirebirdDbMock. PT: Versão opcional de dialeto para o FirebirdDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddFirebirdDbMockTransient(
        this IServiceCollection services,
        Action<FirebirdDbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new FirebirdDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}

