namespace DbSqlLikeMem;

/// <summary>
/// EN: Emits structured text messages for intercepted connection, command, and transaction events.
/// PT: Emite mensagens de texto estruturadas para eventos interceptados de conexao, comando e transacao.
/// </summary>
public sealed class LoggingDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly Action<string> _writeLine;

    /// <summary>
    /// EN: Creates a logging interceptor that writes messages through the supplied delegate.
    /// PT: Cria um interceptor de logging que escreve mensagens pelo delegate informado.
    /// </summary>
    /// <param name="writeLine">EN: Output delegate. PT: Delegate de saida.</param>
    public LoggingDbConnectionInterceptor(Action<string> writeLine)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(writeLine, nameof(writeLine));
        _writeLine = writeLine;
    }

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpening,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpened,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosing,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void CommandCreated(DbConnection connection, DbCommand command)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandCreated,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State,
            CommandText = command.CommandText
        });

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind
        });

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandExecuted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind,
            Result = result,
            PerformanceMetrics = TryGetPerformanceMetrics(context.Connection)
        });

    /// <inheritdoc />
    public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandFailed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind,
            PerformanceMetrics = TryGetPerformanceMetrics(context.Connection),
            Exception = exception
        });

    private static string? TryGetPerformanceMetrics(DbConnection connection)
        => connection is DbConnectionMockBase mock
            ? mock.Metrics.FormatPerformancePhases()
            : null;

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = DbTransactionOperationKind.Begin,
            IsolationLevel = context.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => Write(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionFailed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel,
            Exception = exception
        });

    private void Write(DbInterceptionEvent interceptionEvent)
        => _writeLine(DbInterceptionEventFormatter.Format(interceptionEvent));
}
