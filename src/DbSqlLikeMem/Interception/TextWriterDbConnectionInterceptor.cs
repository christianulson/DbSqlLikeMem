namespace DbSqlLikeMem;

/// <summary>
/// EN: Writes formatted interception events to a <see cref="TextWriter"/>.
/// PT: Escreve eventos de interceptacao formatados em um <see cref="TextWriter"/>.
/// </summary>
public sealed class TextWriterDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly TextWriter _writer;

    /// <summary>
    /// EN: Creates an interceptor that writes one formatted line per event to the supplied writer.
    /// PT: Cria um interceptor que escreve uma linha formatada por evento no writer informado.
    /// </summary>
    /// <param name="writer">EN: Writer that receives the formatted events. PT: Writer que recebe os eventos formatados.</param>
    public TextWriterDbConnectionInterceptor(TextWriter writer)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(writer, nameof(writer));
        _writer = writer;
    }

    /// <summary>
    /// EN: Gets the writer used by this interceptor.
    /// PT: Obtem o writer usado por este interceptor.
    /// </summary>
    public TextWriter Writer => _writer;

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

    private static string? TryGetPerformanceMetrics(DbConnection connection)
        => connection is DbConnectionMockBase mock
            ? mock.Metrics.FormatPerformancePhases()
            : null;

    private void Write(DbInterceptionEvent interceptionEvent)
        => _writer.WriteLine(DbInterceptionEventFormatter.Format(interceptionEvent));
}
