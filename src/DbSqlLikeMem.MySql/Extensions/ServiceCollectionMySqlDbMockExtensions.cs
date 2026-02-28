using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.MySql;

public static class ServiceCollectionMySqlDbMockExtensions
{
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
