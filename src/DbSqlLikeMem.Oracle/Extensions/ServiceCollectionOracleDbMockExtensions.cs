using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Oracle;

public static class ServiceCollectionOracleDbMockExtensions
{
    public static IServiceCollection AddOracleDbMockSingleton(
        this IServiceCollection services,
        Action<OracleDbMock>? acRegister = null,
        int? version = null)
    => services.AddSingleton(_ =>
    {
        var instance = new OracleDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });

    public static IServiceCollection AddOracleDbMockScoped(
        this IServiceCollection services,
        Action<OracleDbMock>? acRegister = null,
        int? version = null)
    => services.AddScoped(_ =>
    {
        var instance = new OracleDbMock(version);
        acRegister?.Invoke(instance);
        return instance;
    });
}
