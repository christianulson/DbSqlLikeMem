using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Db2;

public static class ServiceCollectionDb2DbMockExtensions
{
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
}
