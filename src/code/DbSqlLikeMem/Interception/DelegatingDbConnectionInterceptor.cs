namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements <see cref="DbConnectionInterceptor"/> with delegates for lightweight composition.
/// PT: Implementa <see cref="DbConnectionInterceptor"/> com delegates para composicao leve.
/// </summary>
public sealed class DelegatingDbConnectionInterceptor : DbConnectionInterceptor
{
    /// <summary>
    /// EN: Delegate executed before a wrapped connection opens.
    /// PT: Delegate executado antes de uma conexao encapsulada abrir.
    /// </summary>
    public Action<DbConnection>? OnConnectionOpening { get; init; }

    /// <summary>
    /// EN: Delegate executed after a wrapped connection opens successfully.
    /// PT: Delegate executado apos uma conexao encapsulada abrir com sucesso.
    /// </summary>
    public Action<DbConnection>? OnConnectionOpened { get; init; }

    /// <summary>
    /// EN: Delegate executed before a wrapped connection closes.
    /// PT: Delegate executado antes de uma conexao encapsulada fechar.
    /// </summary>
    public Action<DbConnection>? OnConnectionClosing { get; init; }

    /// <summary>
    /// EN: Delegate executed after a wrapped connection closes successfully.
    /// PT: Delegate executado apos uma conexao encapsulada fechar com sucesso.
    /// </summary>
    public Action<DbConnection>? OnConnectionClosed { get; init; }

    /// <summary>
    /// EN: Delegate executed when a wrapped command is created.
    /// PT: Delegate executado quando um comando encapsulado e criado.
    /// </summary>
    public Action<DbConnection, DbCommand>? OnCommandCreated { get; init; }

    /// <summary>
    /// EN: Delegate executed before a wrapped command runs.
    /// PT: Delegate executado antes de um comando encapsulado executar.
    /// </summary>
    public Action<DbCommandExecutionContext>? OnCommandExecuting { get; init; }

    /// <summary>
    /// EN: Delegate executed after a wrapped command runs successfully.
    /// PT: Delegate executado apos um comando encapsulado executar com sucesso.
    /// </summary>
    public Action<DbCommandExecutionContext, object?>? OnCommandExecuted { get; init; }

    /// <summary>
    /// EN: Delegate executed when a wrapped command fails.
    /// PT: Delegate executado quando um comando encapsulado falha.
    /// </summary>
    public Action<DbCommandExecutionContext, Exception>? OnCommandFailed { get; init; }

    /// <summary>
    /// EN: Delegate executed before a transaction starts.
    /// PT: Delegate executado antes de uma transacao iniciar.
    /// </summary>
    public Action<DbTransactionStartingContext>? OnTransactionStarting { get; init; }

    /// <summary>
    /// EN: Delegate executed after a transaction starts successfully.
    /// PT: Delegate executado apos uma transacao iniciar com sucesso.
    /// </summary>
    public Action<DbTransactionInterceptionContext>? OnTransactionStarted { get; init; }

    /// <summary>
    /// EN: Delegate executed before a transaction operation runs.
    /// PT: Delegate executado antes de uma operacao transacional executar.
    /// </summary>
    public Action<DbTransactionInterceptionContext>? OnTransactionExecuting { get; init; }

    /// <summary>
    /// EN: Delegate executed after a transaction operation runs successfully.
    /// PT: Delegate executado apos uma operacao transacional executar com sucesso.
    /// </summary>
    public Action<DbTransactionInterceptionContext>? OnTransactionExecuted { get; init; }

    /// <summary>
    /// EN: Delegate executed when a transaction operation fails.
    /// PT: Delegate executado quando uma operacao transacional falha.
    /// </summary>
    public Action<DbTransactionInterceptionContext, Exception>? OnTransactionFailed { get; init; }

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection) => OnConnectionOpening?.Invoke(connection);

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection) => OnConnectionOpened?.Invoke(connection);

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection) => OnConnectionClosing?.Invoke(connection);

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection) => OnConnectionClosed?.Invoke(connection);

    /// <inheritdoc />
    public override void CommandCreated(DbConnection connection, DbCommand command)
        => OnCommandCreated?.Invoke(connection, command);

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => OnCommandExecuting?.Invoke(context);

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => OnCommandExecuted?.Invoke(context, result);

    /// <inheritdoc />
    public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
        => OnCommandFailed?.Invoke(context, exception);

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
        => OnTransactionStarting?.Invoke(context);

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => OnTransactionStarted?.Invoke(context);

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => OnTransactionExecuting?.Invoke(context);

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => OnTransactionExecuted?.Invoke(context);

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => OnTransactionFailed?.Invoke(context, exception);
}
