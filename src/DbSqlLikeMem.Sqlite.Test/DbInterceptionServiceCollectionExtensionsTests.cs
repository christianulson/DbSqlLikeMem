using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates interception pipeline registration helpers for dependency injection.
/// PT: Valida os helpers de registro do pipeline de interceptacao para injecao de dependencia.
/// </summary>
public sealed class DbInterceptionServiceCollectionExtensionsTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies AddDbInterception registers built-in interceptors that can be resolved and applied to a connection.
    /// PT: Verifica se AddDbInterception registra interceptors nativos que podem ser resolvidos e aplicados a uma conexao.
    /// </summary>
    [Fact]
    public void AddDbInterception_ShouldRegisterBuiltInInterceptors()
    {
        var lines = new List<string>();
        var recorder = new RecordingDbConnectionInterceptor();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterception(options =>
        {
            options.EnableRecording = true;
            options.RecordingInterceptor = recorder;
            options.LogAction = lines.Add;
        });

        using var provider = services.BuildServiceProvider();
        using var connection = new SqliteConnectionMock(new SqliteDbMock()).WithRegisteredInterceptors(provider);

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 42";
        _ = command.ExecuteScalar();

        var interceptors = provider.GetServices<DbConnectionInterceptor>().ToArray();
        Assert.Contains(interceptors, x => ReferenceEquals(x, recorder));
        Assert.Contains(interceptors, x => x is LoggingDbConnectionInterceptor);
        Assert.Same(recorder, provider.GetRequiredService<RecordingDbConnectionInterceptor>());
        Assert.Contains(lines, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Verifies AddDbInterception can compose built-in interceptors from services already registered in the container.
    /// PT: Verifica se AddDbInterception consegue compor interceptors nativos a partir de servicos ja registrados no container.
    /// </summary>
    [Fact]
    public void AddDbInterception_WithServiceProviderOptions_ShouldRegisterBuiltInInterceptors()
    {
        var logger = new ListLogger();
        var recorder = new RecordingDbConnectionInterceptor();
        IServiceCollection services = new ServiceCollection();
        services.AddSingleton<ILogger>(logger);
        services.AddSingleton(recorder);
        services.AddDbInterception((serviceProvider, options) =>
        {
            options.EnableRecording = true;
            options.RecordingInterceptor = serviceProvider.GetRequiredService<RecordingDbConnectionInterceptor>();
            options.Logger = serviceProvider.GetRequiredService<ILogger>();
        });

        using var provider = services.BuildServiceProvider();
        using var connection = new SqliteConnectionMock(new SqliteDbMock()).WithRegisteredInterceptors(provider);

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 43";
        _ = command.ExecuteScalar();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
        Assert.Contains(logger.Messages, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Verifies AddDbConnectionInterceptor registers custom interceptors that participate in wrapping via service provider.
    /// PT: Verifica se AddDbConnectionInterceptor registra interceptors customizados que participam do wrapping via service provider.
    /// </summary>
    [Fact]
    public void AddDbConnectionInterceptor_ShouldRegisterCustomInterceptor()
    {
        IServiceCollection services = new ServiceCollection();
        services.AddDbConnectionInterceptor<CountingInterceptor>();

        using var provider = services.BuildServiceProvider();
        var interceptor = Assert.Single(provider.GetServices<DbConnectionInterceptor>());
        Assert.IsType<CountingInterceptor>(interceptor);

        using var connection = new SqliteConnectionMock(new SqliteDbMock()).WithRegisteredInterceptors(provider);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 1";
        _ = command.ExecuteScalar();

        Assert.Equal(1, ((CountingInterceptor)interceptor).ExecuteCount);
    }

    /// <summary>
    /// EN: Verifies the high-level DI helpers register recorder, logging, and text-writer interceptors with concrete resolution.
    /// PT: Verifica se os helpers de DI em alto nivel registram os interceptors de recorder, logging e text writer com resolucao concreta.
    /// </summary>
    [Fact]
    public void HighLevelHelpers_ShouldRegisterConcreteInterceptors()
    {
        var lines = new List<string>();
        var recorder = new RecordingDbConnectionInterceptor();
        using var writer = new StringWriter();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterceptionRecording(recorder);
        services.AddDbInterceptionLogging(lines.Add);
        services.AddDbInterceptionTextWriter(writer);

        using var provider = services.BuildServiceProvider();
        Assert.Same(recorder, provider.GetRequiredService<RecordingDbConnectionInterceptor>());
        Assert.NotNull(provider.GetRequiredService<LoggingDbConnectionInterceptor>());
        Assert.NotNull(provider.GetRequiredService<TextWriterDbConnectionInterceptor>());

        using var connection = new SqliteConnectionMock(new SqliteDbMock()).WithRegisteredInterceptors(provider);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 99";
        _ = command.ExecuteScalar();

        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
        Assert.Contains(lines, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        Assert.Contains("event=CommandExecuted", writer.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// EN: Verifies the ILogger helper registers the concrete interceptor and emits formatted messages.
    /// PT: Verifica se o helper de ILogger registra o interceptor concreto e emite mensagens formatadas.
    /// </summary>
    [Fact]
    public void AddDbInterceptionLogger_ShouldRegisterConcreteInterceptorAndEmitMessages()
    {
        var logger = new ListLogger();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterceptionLogger(logger);

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<ILoggerDbConnectionInterceptor>());

        using var connection = new SqliteConnectionMock(new SqliteDbMock()).WithRegisteredInterceptors(provider);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 77";
        _ = command.ExecuteScalar();

        Assert.Contains(logger.Messages, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, x => x.Contains("sql=select 77", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Verifies the DI helper can register an interception connection factory from explicit interceptors.
    /// PT: Verifica se o helper de DI consegue registrar uma factory de conexao com interceptacao a partir de interceptors explicitos.
    /// </summary>
    [Fact]
    public void AddDbInterceptionConnectionFactory_WithInterceptors_ShouldRegisterFactory()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterceptionConnectionFactory(
            () => new SqliteConnectionMock(new SqliteDbMock()),
            recorder);

        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbInterceptionConnectionFactory>();
        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 5";
        _ = command.ExecuteScalar();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Verifies the DI helper can register an interception connection factory from options.
    /// PT: Verifica se o helper de DI consegue registrar uma factory de conexao com interceptacao a partir de opcoes.
    /// </summary>
    [Fact]
    public void AddDbInterceptionConnectionFactory_WithOptions_ShouldRegisterFactory()
    {
        var lines = new List<string>();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterceptionConnectionFactory(
            () => new SqliteConnectionMock(new SqliteDbMock()),
            options => options.LogAction = lines.Add);

        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbInterceptionConnectionFactory>();
        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 6";
        _ = command.ExecuteScalar();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Contains(lines, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Verifies the service-provider-based factory overload reuses interceptors already registered in DI.
    /// PT: Verifica se a sobrecarga da factory baseada no service provider reutiliza os interceptors ja registrados no DI.
    /// </summary>
    [Fact]
    public void AddDbInterceptionConnectionFactory_WithServiceProviderDelegate_ShouldReuseRegisteredInterceptors()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        IServiceCollection services = new ServiceCollection();
        services.AddDbInterceptionRecording(recorder);
        services.AddDbInterceptionConnectionFactory(_ => new SqliteConnectionMock(new SqliteDbMock()));

        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbInterceptionConnectionFactory>();
        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 7";
        _ = command.ExecuteScalar();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Verifies the service-provider-based options overload can compose built-in interceptors from registered services.
    /// PT: Verifica se a sobrecarga de opcoes baseada no service provider consegue compor interceptors nativos a partir de servicos registrados.
    /// </summary>
    [Fact]
    public void AddDbInterceptionConnectionFactory_WithServiceProviderOptions_ShouldComposeBuiltInInterceptors()
    {
        var logger = new ListLogger();
        var recorder = new RecordingDbConnectionInterceptor();
        IServiceCollection services = new ServiceCollection();
        services.AddSingleton<ILogger>(logger);
        services.AddSingleton(recorder);
        services.AddDbInterceptionConnectionFactory(
            _ => new SqliteConnectionMock(new SqliteDbMock()),
            (serviceProvider, options) =>
            {
                options.EnableRecording = true;
                options.RecordingInterceptor = serviceProvider.GetRequiredService<RecordingDbConnectionInterceptor>();
                options.Logger = serviceProvider.GetRequiredService<ILogger>();
            });

        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbInterceptionConnectionFactory>();
        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 8";
        _ = command.ExecuteScalar();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
        Assert.Contains(logger.Messages, x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
    }

    private sealed class CountingInterceptor : DbConnectionInterceptor
    {
        public int ExecuteCount { get; private set; }

        public override void CommandExecuting(DbCommandExecutionContext context)
            => ExecuteCount++;
    }

    private sealed class ListLogger : ILogger
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Messages.Add(formatter(state, exception));
    }
}
