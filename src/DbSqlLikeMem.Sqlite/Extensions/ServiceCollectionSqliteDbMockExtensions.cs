using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Provides dependency injection extensions for registering SqliteDbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de SqliteDbMock.
/// </summary>
public static class ServiceCollectionSqliteDbMockExtensions
{
    /// <summary>
    /// EN: Registers SqliteDbMock as a singleton service.
    /// PT: Registra SqliteDbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for SqliteDbMock. PT: Versão opcional de dialeto para o SqliteDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqliteDbMockSingleton(
        this IServiceCollection services,
        Action<SqliteDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new SqliteDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers SqliteDbMock as a scoped service.
    /// PT: Registra SqliteDbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for SqliteDbMock. PT: Versão opcional de dialeto para o SqliteDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqliteDbMockScoped(
        this IServiceCollection services,
        Action<SqliteDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new SqliteDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers SqliteDbMock as a transient service.
    /// PT: Registra SqliteDbMock como serviço transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for SqliteDbMock. PT: Versão opcional de dialeto para o SqliteDbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqliteDbMockTransient(
        this IServiceCollection services,
        Action<SqliteDbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new SqliteDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
