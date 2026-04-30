using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Dependency injection extensions for DbMock.
/// PT: Extensoes de injecao de dependencia para DbMock.
/// </summary>
public static class ServiceCollectionDbMockExtensions
{
    /// <summary>
    /// EN: Adds a DbMock as a singleton service.
    /// PT: Adiciona um DbMock como um servico singleton.
    /// </summary>
    public static IServiceCollection AddDbMockSingleton<T>(
        this IServiceCollection services,
        Action<T>? acRegister = null)
        where T : DbMock
    => services.AddSingleton(_ =>
    {
        var instance = CreateDbMockInstance<T>(_);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Adds a DbMock as a scoped service.
    /// PT: Adiciona um DbMock como um servico de escopo.
    /// </summary>
    public static IServiceCollection AddDbMockScoped<T>(
        this IServiceCollection services,
        Action<T>? acRegister = null)
        where T : DbMock
    => services.AddScoped(_ =>
    {
        var instance = CreateDbMockInstance<T>(_);
        acRegister?.Invoke(instance);
        return instance;
    });

    /// <summary>
    /// EN: Adds a DbMock as a transient service.
    /// PT: Adiciona um DbMock como um servico transiente.
    /// </summary>
    public static IServiceCollection AddDbMockTransient<T>(
        this IServiceCollection services,
        Action<T>? acRegister = null)
        where T : DbMock
    => services.AddTransient(_ =>
    {
        var instance = CreateDbMockInstance<T>(_);
        acRegister?.Invoke(instance);
        return instance;
    });

    private static T CreateDbMockInstance<T>(IServiceProvider serviceProvider)
        where T : DbMock
    {
        var type = typeof(T);
        var constructors = type
            .GetConstructors()
            .OrderBy(static c => c.GetParameters().Length)
            .ToArray();

        foreach (var constructor in constructors)
        {
            var parameters = constructor.GetParameters();
            var args = new object?[parameters.Length];
            var canUse = true;

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                var resolved = serviceProvider.GetService(parameter.ParameterType);
                if (resolved is not null)
                {
                    args[i] = resolved;
                    continue;
                }

                if (parameter.HasDefaultValue)
                {
                    args[i] = parameter.DefaultValue;
                    continue;
                }

                canUse = false;
                break;
            }

            if (canUse)
                return (T)constructor.Invoke(args);
        }

        throw new MissingMethodException(
            $"Cannot create '{type.FullName}'. Provide a constructor with optional/default parameters or services resolvable from DI.");
    }
}
