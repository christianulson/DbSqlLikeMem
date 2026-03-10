using System.Diagnostics;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Publishes interception events through <see cref="DiagnosticListener"/>.
/// PT: Publica eventos de interceptacao por meio de <see cref="DiagnosticListener"/>.
/// </summary>
public sealed class DiagnosticListenerDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly DiagnosticListener _listener;

    /// <summary>
    /// EN: Creates an interceptor that writes to the default interception diagnostic listener.
    /// PT: Cria um interceptor que escreve no diagnostic listener padrao da interceptacao.
    /// </summary>
    public DiagnosticListenerDbConnectionInterceptor()
        : this(new DiagnosticListener(DbInterceptionDiagnosticNames.ListenerName))
    {
    }

    /// <summary>
    /// EN: Creates an interceptor that writes to the supplied diagnostic listener.
    /// PT: Cria um interceptor que escreve no diagnostic listener informado.
    /// </summary>
    /// <param name="listener">EN: Diagnostic listener used to publish events. PT: Diagnostic listener usado para publicar eventos.</param>
    public DiagnosticListenerDbConnectionInterceptor(DiagnosticListener listener)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(listener, nameof(listener));
        _listener = listener;
    }

    /// <summary>
    /// EN: Gets the underlying diagnostic listener.
    /// PT: Obtem o diagnostic listener subjacente.
    /// </summary>
    public DiagnosticListener Listener => _listener;

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
        => Write(DbInterceptionDiagnosticNames.ConnectionOpening, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpening,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection)
        => Write(DbInterceptionDiagnosticNames.ConnectionOpened, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpened,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection)
        => Write(DbInterceptionDiagnosticNames.ConnectionClosing, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosing,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection)
        => Write(DbInterceptionDiagnosticNames.ConnectionClosed, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void CommandCreated(DbConnection connection, DbCommand command)
        => Write(DbInterceptionDiagnosticNames.CommandCreated, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandCreated,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State,
            CommandText = command.CommandText
        });

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => Write(DbInterceptionDiagnosticNames.CommandExecuting, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind
        });

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => Write(DbInterceptionDiagnosticNames.CommandExecuted, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandExecuted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind,
            Result = result
        });

    /// <inheritdoc />
    public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
        => Write(DbInterceptionDiagnosticNames.CommandFailed, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandFailed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind,
            Exception = exception
        });

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
        => Write(DbInterceptionDiagnosticNames.TransactionStarting, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = DbTransactionOperationKind.Begin,
            IsolationLevel = context.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => Write(DbInterceptionDiagnosticNames.TransactionStarted, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => Write(DbInterceptionDiagnosticNames.TransactionExecuting, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => Write(DbInterceptionDiagnosticNames.TransactionExecuted, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => Write(DbInterceptionDiagnosticNames.TransactionFailed, new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionFailed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel,
            Exception = exception
        });

    private void Write(string eventName, DbInterceptionEvent interceptionEvent)
    {
        if (_listener.IsEnabled(eventName))
            _listener.Write(eventName, interceptionEvent);
    }
}
