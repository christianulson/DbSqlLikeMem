using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Provides dependency injection extensions for registering Db2DbMock instances.
/// PT: Fornece extensões de injeção de dependência para registrar instâncias de Db2DbMock.
/// </summary>
public static class ServiceCollectionDb2DbMockExtensions
{
    /// <summary>
    /// EN: Registers Db2DbMock as a singleton service.
    /// PT: Registra Db2DbMock como serviço singleton.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for Db2DbMock. PT: Versão opcional de dialeto para o Db2DbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddDb2DbMockSingleton(
        this IServiceCollection services,
        Action<Db2DbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new Db2DbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers Db2DbMock as a scoped service.
    /// PT: Registra Db2DbMock como serviço com escopo.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for Db2DbMock. PT: Versão opcional de dialeto para o Db2DbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddDb2DbMockScoped(
        this IServiceCollection services,
        Action<Db2DbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new Db2DbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Registers Db2DbMock as a transient service.
    /// PT: Registra Db2DbMock como serviço transient.
    /// </summary>
    /// <param name="services">EN: Service collection to register into. PT: Coleção de serviços para registrar.</param>
    /// <param name="acRegister">EN: Optional callback to configure the created mock. PT: Callback opcional para configurar o mock criado.</param>
    /// <param name="version">EN: Optional dialect version for Db2DbMock. PT: Versão opcional de dialeto para o Db2DbMock.</param>
    /// <returns>EN: The same service collection for chaining. PT: A mesma coleção de serviços para encadeamento.</returns>
    public static IServiceCollection AddDb2DbMockTransient(
        this IServiceCollection services,
        Action<Db2DbMock>? acRegister = null,
        int? version = null)
    => services.AddTransient(_ =>
    {
        var instance = new Db2DbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
