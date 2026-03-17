namespace DbSqlLikeMem;

/// <summary>
/// EN: Records interception events in memory for diagnostics, tests, and lightweight tracing.
/// PT: Registra eventos de interceptacao em memoria para diagnostico, testes e tracing leve.
/// </summary>
public sealed class RecordingDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly List<DbInterceptionEvent> _events = [];
    private readonly object _gate = new();

    /// <summary>
    /// EN: Gets the events captured by this interceptor.
    /// PT: Obtem os eventos capturados por este interceptor.
    /// </summary>
    public IReadOnlyList<DbInterceptionEvent> Events
    {
        get
        {
            lock (_gate)
                return [.. _events];
        }
    }

    /// <summary>
    /// EN: Clears the recorded event history.
    /// PT: Limpa o historico de eventos registrados.
    /// </summary>
    public void Clear()
    {
        lock (_gate)
            _events.Clear();
    }

    /// <summary>
    /// EN: Returns the recorded event history already formatted as structured text lines.
    /// PT: Retorna o historico de eventos registrados ja formatado como linhas de texto estruturadas.
    /// </summary>
    /// <returns>EN: Formatted event lines. PT: Linhas de evento formatadas.</returns>
    public IReadOnlyList<string> GetFormattedEvents()
    {
        lock (_gate)
            return [.. _events.Select(DbInterceptionEventFormatter.Format)];
    }

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpening,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionOpened,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosing,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.ConnectionClosed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State
        });

    /// <inheritdoc />
    public override void CommandCreated(DbConnection connection, DbCommand command)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandCreated,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = connection.State,
            CommandText = command.CommandText
        });

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            CommandText = context.Command.CommandText,
            CommandExecutionKind = context.ExecutionKind
        });

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => Add(new DbInterceptionEvent
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
        => Add(new DbInterceptionEvent
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
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            IsolationLevel = context.IsolationLevel,
            TransactionOperationKind = DbTransactionOperationKind.Begin
        });

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionStarted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuting,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionExecuted,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel
        });

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => Add(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.TransactionFailed,
            TimestampUtc = DateTimeOffset.UtcNow,
            ConnectionState = context.Connection.State,
            TransactionOperationKind = context.OperationKind,
            IsolationLevel = context.Transaction.IsolationLevel,
            Exception = exception
        });

    private void Add(DbInterceptionEvent interceptionEvent)
    {
        lock (_gate)
            _events.Add(interceptionEvent);
    }
}
