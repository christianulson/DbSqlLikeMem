using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem;

public static class ServiceCollectionDbMockExtensions
{
    public static IServiceCollection AddDbMockSingleton<T>(
        this IServiceCollection services,
        Action<T>? acRegister = null)
        where T : DbMock
    => services.AddSingleton(_ =>
    {
        var instance = Activator.CreateInstance<T>();
        acRegister?.Invoke(instance);
        return instance;
    });

    public static IServiceCollection AddDbMockScoped<T>(
        this IServiceCollection services,
        Action<T>? acRegister = null)
        where T : DbMock
    => services.AddScoped(_ =>
    {
        var instance = Activator.CreateInstance<T>();
        acRegister?.Invoke(instance);
        return instance;
    });
}
