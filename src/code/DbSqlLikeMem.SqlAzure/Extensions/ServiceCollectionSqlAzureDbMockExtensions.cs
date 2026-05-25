using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Provides dependency injection extensions for registering SqlAzureDbMock instances.
/// PT-br: Fornece extensoes de injecao de dependencia para registrar instancias de SqlAzureDbMock.
/// </summary>
public static class ServiceCollectionSqlAzureDbMockExtensions
{
    /// <summary>
    /// EN: Registers SqlAzureDbMock as a singleton service.
    /// PT-br: Registra SqlAzureDbMock como servico singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="compatibilityLevel">EN: Optional SQL Azure compatibility level. PT-br: Nível de compatibilidade SQL Azure opcional.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqlAzureDbMockSingleton(
        this IServiceCollection services,
        Action<SqlAzureDbMock>? acRegister = null,
        int? compatibilityLevel = null)
    => services.AddSingleton(_ =>
    {
        var instance = new SqlAzureDbMock(compatibilityLevel);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers SqlAzureDbMock as a scoped service.
    /// PT-br: Registra SqlAzureDbMock como servico com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="compatibilityLevel">EN: Optional SQL Azure compatibility level. PT-br: Nível de compatibilidade SQL Azure opcional.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqlAzureDbMockScoped(
        this IServiceCollection services,
        Action<SqlAzureDbMock>? acRegister = null,
        int? compatibilityLevel = null)
    => services.AddScoped(_ =>
    {
        var instance = new SqlAzureDbMock(compatibilityLevel);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers SqlAzureDbMock as a transient service.
    /// PT-br: Registra SqlAzureDbMock como servico transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT-br: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT-br: Callback opcional para configurar o mock criado.</param>
    /// <param name="compatibilityLevel">EN: Optional SQL Azure compatibility level. PT-br: Nível de compatibilidade SQL Azure opcional.</param>
    /// <returns>EN: The same service collection for chaining. PT-br: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddSqlAzureDbMockTransient(
        this IServiceCollection services,
        Action<SqlAzureDbMock>? acRegister = null,
        int? compatibilityLevel = null)
    => services.AddTransient(_ =>
    {
        var instance = new SqlAzureDbMock(compatibilityLevel);
        acRegister?.Invoke(instance);
        return instance;
    });
}
