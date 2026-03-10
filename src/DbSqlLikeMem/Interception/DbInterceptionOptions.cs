using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Configures a composed set of interceptors for the ADO.NET interception pipeline.
/// PT: Configura um conjunto composto de interceptors para o pipeline de interceptacao ADO.NET.
/// </summary>
public sealed class DbInterceptionOptions
{
    /// <summary>
    /// EN: Custom interceptors appended after the built-in interceptors created from this options object.
    /// PT: Interceptors customizados anexados apos os interceptors nativos criados por este objeto de opcoes.
    /// </summary>
    public IList<DbConnectionInterceptor> AdditionalInterceptors { get; } = [];

    /// <summary>
    /// EN: Enables the in-memory recording interceptor.
    /// PT: Habilita o interceptor de gravacao em memoria.
    /// </summary>
    public bool EnableRecording { get; set; }

    /// <summary>
    /// EN: Optional recording interceptor instance reused when recording is enabled.
    /// PT: Instancia opcional de interceptor de gravacao reutilizada quando a gravacao estiver habilitada.
    /// </summary>
    public RecordingDbConnectionInterceptor? RecordingInterceptor { get; set; }

    /// <summary>
    /// EN: Delegate used by the structured logging interceptor when configured.
    /// PT: Delegate usado pelo interceptor de logging estruturado quando configurado.
    /// </summary>
    public Action<string>? LogAction { get; set; }

    /// <summary>
    /// EN: Logger used by the ILogger-based interceptor when configured.
    /// PT: Logger usado pelo interceptor baseado em ILogger quando configurado.
    /// </summary>
    public ILogger? Logger { get; set; }

    /// <summary>
    /// EN: Writer used by the text-writer interceptor when configured.
    /// PT: Writer usado pelo interceptor de text writer quando configurado.
    /// </summary>
    public TextWriter? TextWriter { get; set; }

    /// <summary>
    /// EN: Enables the fault-injection interceptor with the supplied configuration.
    /// PT: Habilita o interceptor de injecao de falha com a configuracao informada.
    /// </summary>
    public FaultInjectionDbConnectionInterceptor? FaultInjection { get; set; }

    /// <summary>
    /// EN: Diagnostic listener used by the diagnostic-listener interceptor when configured.
    /// PT: Diagnostic listener usado pelo interceptor de diagnostic listener quando configurado.
    /// </summary>
    public DiagnosticListener? DiagnosticListener { get; set; }

#if NET5_0_OR_GREATER
    /// <summary>
    /// EN: Activity source used by the activity-source interceptor when configured on supported target frameworks.
    /// PT: Activity source usado pelo interceptor de activity source quando configurado em target frameworks suportados.
    /// </summary>
    public ActivitySource? ActivitySource { get; set; }
#endif

    /// <summary>
    /// EN: Enables recording and optionally reuses the supplied recorder instance.
    /// PT: Habilita a gravacao e opcionalmente reutiliza a instancia de recorder informada.
    /// </summary>
    /// <param name="recorder">EN: Optional recorder instance. PT: Instancia opcional de recorder.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseRecording(RecordingDbConnectionInterceptor? recorder = null)
    {
        EnableRecording = true;
        RecordingInterceptor = recorder;
        return this;
    }

    /// <summary>
    /// EN: Enables structured logging through the supplied text callback.
    /// PT: Habilita logging estruturado por meio do callback de texto informado.
    /// </summary>
    /// <param name="writeLine">EN: Callback that receives formatted lines. PT: Callback que recebe linhas formatadas.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseLogging(Action<string> writeLine)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(writeLine, nameof(writeLine));
        LogAction = writeLine;
        return this;
    }

    /// <summary>
    /// EN: Enables text-writer based logging.
    /// PT: Habilita logging baseado em text writer.
    /// </summary>
    /// <param name="writer">EN: Writer that receives formatted lines. PT: Writer que recebe linhas formatadas.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseTextWriter(TextWriter writer)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(writer, nameof(writer));
        TextWriter = writer;
        return this;
    }

    /// <summary>
    /// EN: Enables fault injection using the supplied interceptor instance.
    /// PT: Habilita injecao de falha usando a instancia de interceptor informada.
    /// </summary>
    /// <param name="interceptor">EN: Fault-injection interceptor. PT: Interceptor de injecao de falha.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseFaultInjection(FaultInjectionDbConnectionInterceptor interceptor)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptor, nameof(interceptor));
        FaultInjection = interceptor;
        return this;
    }

    /// <summary>
    /// EN: Enables diagnostic-listener publishing.
    /// PT: Habilita publicacao por diagnostic listener.
    /// </summary>
    /// <param name="listener">EN: Diagnostic listener. PT: Diagnostic listener.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseDiagnosticListener(DiagnosticListener listener)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(listener, nameof(listener));
        DiagnosticListener = listener;
        return this;
    }

#if NET5_0_OR_GREATER
    /// <summary>
    /// EN: Enables activity publishing through the supplied activity source.
    /// PT: Habilita publicacao de activities por meio do activity source informado.
    /// </summary>
    /// <param name="activitySource">EN: Activity source. PT: Activity source.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions UseActivitySource(ActivitySource activitySource)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(activitySource, nameof(activitySource));
        ActivitySource = activitySource;
        return this;
    }
#endif

    /// <summary>
    /// EN: Appends a custom interceptor to the composed pipeline.
    /// PT: Anexa um interceptor customizado ao pipeline composto.
    /// </summary>
    /// <param name="interceptor">EN: Interceptor to append. PT: Interceptor a anexar.</param>
    /// <returns>EN: Same options instance. PT: Mesma instancia de opcoes.</returns>
    public DbInterceptionOptions AddInterceptor(DbConnectionInterceptor interceptor)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptor, nameof(interceptor));
        AdditionalInterceptors.Add(interceptor);
        return this;
    }

    /// <summary>
    /// EN: Builds the interceptors configured by this options object.
    /// PT: Constroi os interceptors configurados por este objeto de opcoes.
    /// </summary>
    /// <returns>EN: Configured interceptors. PT: Interceptors configurados.</returns>
    public DbConnectionInterceptor[] BuildInterceptors()
    {
        var interceptors = new List<DbConnectionInterceptor>();

        if (EnableRecording)
            interceptors.Add(RecordingInterceptor ?? new RecordingDbConnectionInterceptor());
        if (LogAction is not null)
            interceptors.Add(new LoggingDbConnectionInterceptor(LogAction));
        if (Logger is not null)
            interceptors.Add(new ILoggerDbConnectionInterceptor(Logger));
        if (TextWriter is not null)
            interceptors.Add(new TextWriterDbConnectionInterceptor(TextWriter));
        if (FaultInjection is not null)
            interceptors.Add(FaultInjection);
        if (DiagnosticListener is not null)
            interceptors.Add(new DiagnosticListenerDbConnectionInterceptor(DiagnosticListener));
#if NET5_0_OR_GREATER
        if (ActivitySource is not null)
            interceptors.Add(new ActivitySourceDbConnectionInterceptor(ActivitySource));
#endif

        interceptors.AddRange(AdditionalInterceptors);
        return [.. interceptors];
    }
}
