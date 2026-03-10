using Microsoft.Extensions.Logging;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Writes interception events to <see cref="ILogger"/>.
/// PT: Escreve eventos de interceptacao em <see cref="ILogger"/>.
/// </summary>
public sealed class ILoggerDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly ILogger _logger;

    /// <summary>
    /// EN: Creates an interceptor that writes interception events to the supplied logger.
    /// PT: Cria um interceptor que escreve eventos de interceptacao no logger informado.
    /// </summary>
    /// <param name="logger">EN: Logger receiving interception messages. PT: Logger que recebe as mensagens de interceptacao.</param>
    public ILoggerDbConnectionInterceptor(ILogger logger)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(logger, nameof(logger));
        _logger = logger;
    }

    /// <summary>
    /// EN: Gets the logger used by this interceptor.
    /// PT: Obtem o logger usado por este interceptor.
    /// </summary>
    public ILogger Logger => _logger;

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
        => Log(CreateConnectionEvent(DbInterceptionEventKind.ConnectionOpening, connection.State));

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection)
        => Log(CreateConnectionEvent(DbInterceptionEventKind.ConnectionOpened, connection.State));

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection)
        => Log(CreateConnectionEvent(DbInterceptionEventKind.ConnectionClosing, connection.State));

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection)
        => Log(CreateConnectionEvent(DbInterceptionEventKind.ConnectionClosed, connection.State));

    /// <inheritdoc />
    public override void CommandCreated(DbConnection connection, DbCommand command)
        => Log(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandCreated,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State,
            CommandText = command.CommandText
        });

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => Log(CreateCommandEvent(DbInterceptionEventKind.CommandExecuting, context, null, null));

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => Log(CreateCommandEvent(DbInterceptionEventKind.CommandExecuted, context, result, null));

    /// <inheritdoc />
    public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
        => Log(CreateCommandEvent(DbInterceptionEventKind.CommandFailed, context, null, exception));

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
        => Log(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = DbTransactionOperationKind.Begin,
            IsolationLevel = context.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => Log(CreateTransactionEvent(DbInterceptionEventKind.TransactionStarted, context, null));

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => Log(CreateTransactionEvent(DbInterceptionEventKind.TransactionExecuting, context, null));

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => Log(CreateTransactionEvent(DbInterceptionEventKind.TransactionExecuted, context, null));

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => Log(CreateTransactionEvent(DbInterceptionEventKind.TransactionFailed, context, exception));

    private static DbInterceptionEvent CreateConnectionEvent(
        DbInterceptionEventKind eventKind,
        ConnectionState state)
        => new()
        {
            EventKind = eventKind,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = state
        };

    private static DbInterceptionEvent CreateCommandEvent(
        DbInterceptionEventKind eventKind,
        DbCommandExecutionContext context,
        object? result,
        Exception? exception)
        => new()
        {
            EventKind = eventKind,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind,
            Result = result,
            Exception = exception
        };

    private static DbInterceptionEvent CreateTransactionEvent(
        DbInterceptionEventKind eventKind,
        DbTransactionInterceptionContext context,
        Exception? exception)
        => new()
        {
            EventKind = eventKind,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel,
            Exception = exception
        };

    private void Log(DbInterceptionEvent interceptionEvent)
    {
        var message = DbInterceptionEventFormatter.Format(interceptionEvent);
        if (interceptionEvent.Exception is not null)
            _logger.LogError(interceptionEvent.Exception, "{Message}", message);
        else
            _logger.LogInformation("{Message}", message);
    }
}
