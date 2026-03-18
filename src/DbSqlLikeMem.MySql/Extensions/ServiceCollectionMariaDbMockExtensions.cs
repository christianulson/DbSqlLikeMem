using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Provides dependency injection extensions for registering MariaDbDbMock instances.
/// PT: Fornece extensoes de injecao de dependencia para registrar instancias de MariaDbDbMock.
/// </summary>
public static class ServiceCollectionMariaDbMockExtensions
{
    /// <summary>
    /// EN: Registers MariaDbDbMock as a singleton service.
    /// PT: Registra MariaDbDbMock como servico singleton.
    /// </summary>
    public static IServiceCollection AddMariaDbMockSingleton(
        this IServiceCollection services,
        Action<MariaDbDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new MariaDbDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers MariaDbDbMock as a scoped service.
    /// PT: Registra MariaDbDbMock como servico com escopo.
    /// </summary>
    public static IServiceCollection AddMariaDbMockScoped(
        this IServiceCollection services,
        Action<MariaDbDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new MariaDbDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers MariaDbDbMock as a transient service.
    /// PT: Registra MariaDbDbMock como servico transient.
    /// </summary>
    public static IServiceCollection AddMariaDbMockTransient(
        this IServiceCollection services,
        Action<MariaDbDbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new MariaDbDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
