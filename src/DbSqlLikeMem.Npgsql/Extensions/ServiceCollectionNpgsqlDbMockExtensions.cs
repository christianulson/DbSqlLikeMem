using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Npgsql;

public static class ServiceCollectionNpgsqlDbMockExtensions
{
    public static IServiceCollection AddNpgsqlDbMockSingleton(
        this IServiceCollection services,
        Action<NpgsqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new NpgsqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    public static IServiceCollection AddNpgsqlDbMockScoped<T>(
        this IServiceCollection services,
        Action<NpgsqlDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new NpgsqlDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
