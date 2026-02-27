using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.SqlServer;

public static class ServiceCollectionSqlServerDbMockExtensions
{
    public static IServiceCollection AddSqlServerDbMockSingleton(
        this IServiceCollection services,
        Action<SqlServerDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new SqlServerDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    public static IServiceCollection AddSqlServerDbMockScoped<T>(
        this IServiceCollection services,
        Action<SqlServerDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new SqlServerDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
