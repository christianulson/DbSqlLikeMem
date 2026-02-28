using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Sqlite;

public static class ServiceCollectionSqliteDbMockExtensions
{
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
}
