using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Registers interception pipeline services in dependency injection.
/// PT: Registra servicos do pipeline de interceptacao na injecao de dependencia.
/// </summary>
public static class DbInterceptionServiceCollectionExtensions
{
    /// <summary>
    /// EN: Registers a shared recording interceptor and exposes it both by concrete type and pipeline interface.
    /// PT: Registra um interceptor de gravacao compartilhado e o expoe tanto pelo tipo concreto quanto pela interface do pipeline.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="recorder">EN: Optional recorder instance to reuse. PT: Instancia opcional de recorder a reutilizar.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionRecording(
        this IServiceCollection services,
        RecordingDbConnectionInterceptor? recorder = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        recorder ??= new RecordingDbConnectionInterceptor();
        services.AddSingleton(recorder);
        services.AddSingleton<DbConnectionInterceptor>(recorder);
        return services;
    }

    /// <summary>
    /// EN: Registers structured logging through the supplied text callback.
    /// PT: Registra logging estruturado por meio do callback de texto informado.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="writeLine">EN: Callback that receives formatted lines. PT: Callback que recebe linhas formatadas.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionLogging(
        this IServiceCollection services,
        Action<string> writeLine)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(writeLine, nameof(writeLine));
        var interceptor = new LoggingDbConnectionInterceptor(writeLine);
        services.AddSingleton(interceptor);
        services.AddSingleton<DbConnectionInterceptor>(interceptor);
        return services;
    }

    /// <summary>
    /// EN: Registers text-writer based interception logging.
    /// PT: Registra logging de interceptacao baseado em text writer.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="writer">EN: Writer that receives formatted lines. PT: Writer que recebe linhas formatadas.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionTextWriter(
        this IServiceCollection services,
        TextWriter writer)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(writer, nameof(writer));
        var interceptor = new TextWriterDbConnectionInterceptor(writer);
        services.AddSingleton(interceptor);
        services.AddSingleton<DbConnectionInterceptor>(interceptor);
        return services;
    }

    /// <summary>
    /// EN: Registers interception logging through <see cref="ILogger"/>.
    /// PT: Registra logging de interceptacao por meio de <see cref="ILogger"/>.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="logger">EN: Logger receiving formatted interception events. PT: Logger que recebe os eventos formatados de interceptacao.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionLogger(
        this IServiceCollection services,
        ILogger logger)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(logger, nameof(logger));
        var interceptor = new ILoggerDbConnectionInterceptor(logger);
        services.AddSingleton(interceptor);
        services.AddSingleton<DbConnectionInterceptor>(interceptor);
        return services;
    }

    /// <summary>
    /// EN: Registers built-in interceptors composed from the supplied options.
    /// PT: Registra interceptors nativos compostos a partir das opcoes informadas.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="configure">EN: Options configuration. PT: Configuracao das opcoes.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterception(
        this IServiceCollection services,
        Action<DbInterceptionOptions>? configure = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));

        var options = new DbInterceptionOptions();
        configure?.Invoke(options);

        foreach (var interceptor in options.BuildInterceptors())
        {
            services.AddSingleton(interceptor.GetType(), interceptor);
            services.AddSingleton(typeof(DbConnectionInterceptor), interceptor);
        }

        return services;
    }

    /// <summary>
    /// EN: Registers built-in interceptors composed from options configured with access to the service provider.
    /// PT: Registra interceptors nativos compostos a partir de opcoes configuradas com acesso ao service provider.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="configure">EN: Options configuration that can resolve services. PT: Configuracao das opcoes que pode resolver servicos.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterception(
        this IServiceCollection services,
        Action<IServiceProvider, DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        services.AddSingleton<IReadOnlyList<DbConnectionInterceptor>>(serviceProvider =>
        {
            var options = new DbInterceptionOptions();
            configure(serviceProvider, options);
            return options.BuildInterceptors();
        });
        return services;
    }

    /// <summary>
    /// EN: Registers a custom interceptor type as part of the interception pipeline.
    /// PT: Registra um tipo customizado de interceptor como parte do pipeline de interceptacao.
    /// </summary>
    /// <typeparam name="TInterceptor">EN: Interceptor type. PT: Tipo do interceptor.</typeparam>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbConnectionInterceptor<TInterceptor>(
        this IServiceCollection services)
        where TInterceptor : class, DbConnectionInterceptor
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        services.AddSingleton<DbConnectionInterceptor, TInterceptor>();
        return services;
    }

    /// <summary>
    /// EN: Registers an interception connection factory built from a connection delegate and explicit interceptors.
    /// PT: Registra uma factory de conexao com interceptacao criada a partir de um delegate de conexao e interceptors explicitos.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="interceptors">EN: Interceptors applied to each created connection. PT: Interceptors aplicados a cada conexao criada.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionConnectionFactory(
        this IServiceCollection services,
        Func<DbConnection> connectionFactory,
        params DbConnectionInterceptor[] interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        services.AddSingleton<IDbInterceptionConnectionFactory>(
            new DbInterceptionConnectionFactory(connectionFactory, () => interceptors));
        return services;
    }

    /// <summary>
    /// EN: Registers an interception connection factory built from a connection delegate and interception options.
    /// PT: Registra uma factory de conexao com interceptacao criada a partir de um delegate de conexao e opcoes de interceptacao.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection. PT: Delegate que cria a conexao interna.</param>
    /// <param name="configure">EN: Options configuration. PT: Configuracao das opcoes.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionConnectionFactory(
        this IServiceCollection services,
        Func<DbConnection> connectionFactory,
        Action<DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        var options = new DbInterceptionOptions();
        configure(options);
        services.AddSingleton<IDbInterceptionConnectionFactory>(
            new DbInterceptionConnectionFactory(connectionFactory, options));
        return services;
    }

    /// <summary>
    /// EN: Registers an interception connection factory that resolves its interceptors from the same service provider.
    /// PT: Registra uma factory de conexao com interceptacao que resolve seus interceptors a partir do mesmo service provider.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection using the service provider. PT: Delegate que cria a conexao interna usando o service provider.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionConnectionFactory(
        this IServiceCollection services,
        Func<IServiceProvider, DbConnection> connectionFactory)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        services.AddSingleton<IDbInterceptionConnectionFactory>(serviceProvider =>
            new DbInterceptionConnectionFactory(
                () => connectionFactory(serviceProvider),
                () => DbInterceptionServiceProviderExtensions.ResolveRegisteredInterceptors(serviceProvider)));
        return services;
    }

    /// <summary>
    /// EN: Registers an interception connection factory that builds its options from the same service provider.
    /// PT: Registra uma factory de conexao com interceptacao que monta suas opcoes a partir do mesmo service provider.
    /// </summary>
    /// <param name="services">EN: Service collection. PT: Colecao de servicos.</param>
    /// <param name="connectionFactory">EN: Delegate that creates the inner connection using the service provider. PT: Delegate que cria a conexao interna usando o service provider.</param>
    /// <param name="configure">EN: Delegate that configures interception options using the service provider. PT: Delegate que configura as opcoes de interceptacao usando o service provider.</param>
    /// <returns>EN: Same service collection. PT: Mesma colecao de servicos.</returns>
    public static IServiceCollection AddDbInterceptionConnectionFactory(
        this IServiceCollection services,
        Func<IServiceProvider, DbConnection> connectionFactory,
        Action<IServiceProvider, DbInterceptionOptions> configure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(services, nameof(services));
        ArgumentNullExceptionCompatible.ThrowIfNull(connectionFactory, nameof(connectionFactory));
        ArgumentNullExceptionCompatible.ThrowIfNull(configure, nameof(configure));
        services.AddSingleton<IDbInterceptionConnectionFactory>(serviceProvider =>
        {
            var options = new DbInterceptionOptions();
            configure(serviceProvider, options);
            return new DbInterceptionConnectionFactory(
                () => connectionFactory(serviceProvider),
                options);
        });
        return services;
    }
}
