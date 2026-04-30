namespace DbSqlLikeMem;

/// <summary>
/// EN: Adds optional latency and deterministic fault injection to intercepted ADO.NET operations.
/// PT: Adiciona latencia opcional e injecao deterministica de falhas a operacoes ADO.NET interceptadas.
/// </summary>
public sealed class FaultInjectionDbConnectionInterceptor : DbConnectionInterceptor
{
    /// <summary>
    /// EN: Fixed latency applied before eligible operations run.
    /// PT: Latencia fixa aplicada antes da execucao de operacoes elegiveis.
    /// </summary>
    public TimeSpan Latency { get; init; }

    /// <summary>
    /// EN: Optional predicate that decides whether connection open should fail.
    /// PT: Predicado opcional que decide se a abertura da conexao deve falhar.
    /// </summary>
    public Func<DbConnection, bool>? ShouldFailConnectionOpen { get; init; }

    /// <summary>
    /// EN: Optional predicate that decides whether command execution should fail.
    /// PT: Predicado opcional que decide se a execucao do comando deve falhar.
    /// </summary>
    public Func<DbCommandExecutionContext, bool>? ShouldFailCommand { get; init; }

    /// <summary>
    /// EN: Optional predicate that decides whether transaction start should fail.
    /// PT: Predicado opcional que decide se o inicio da transacao deve falhar.
    /// </summary>
    public Func<DbTransactionStartingContext, bool>? ShouldFailTransactionStart { get; init; }

    /// <summary>
    /// EN: Optional predicate that decides whether commit or rollback should fail.
    /// PT: Predicado opcional que decide se commit ou rollback devem falhar.
    /// </summary>
    public Func<DbTransactionInterceptionContext, bool>? ShouldFailTransactionOperation { get; init; }

    /// <summary>
    /// EN: Factory used to create the injected connection-open exception.
    /// PT: Fabrica usada para criar a excecao injetada de abertura de conexao.
    /// </summary>
    public Func<DbConnection, Exception>? ConnectionOpenExceptionFactory { get; init; }

    /// <summary>
    /// EN: Factory used to create the injected command exception.
    /// PT: Fabrica usada para criar a excecao injetada de comando.
    /// </summary>
    public Func<DbCommandExecutionContext, Exception>? CommandExceptionFactory { get; init; }

    /// <summary>
    /// EN: Factory used to create the injected transaction-start exception.
    /// PT: Fabrica usada para criar a excecao injetada de inicio de transacao.
    /// </summary>
    public Func<DbTransactionStartingContext, Exception>? TransactionStartExceptionFactory { get; init; }

    /// <summary>
    /// EN: Factory used to create the injected transaction-operation exception.
    /// PT: Fabrica usada para criar a excecao injetada de operacao transacional.
    /// </summary>
    public Func<DbTransactionInterceptionContext, Exception>? TransactionOperationExceptionFactory { get; init; }

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
    {
        ApplyLatency();
        if (ShouldFailConnectionOpen?.Invoke(connection) == true)
            throw (ConnectionOpenExceptionFactory?.Invoke(connection)
                ?? new IOException("Injected connection open failure."));
    }

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
    {
        ApplyLatency();
        if (ShouldFailCommand?.Invoke(context) == true)
            throw (CommandExceptionFactory?.Invoke(context)
                ?? new IOException("Injected command execution failure."));
    }

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
    {
        ApplyLatency();
        if (ShouldFailTransactionStart?.Invoke(context) == true)
            throw (TransactionStartExceptionFactory?.Invoke(context)
                ?? new IOException("Injected transaction start failure."));
    }

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
    {
        ApplyLatency();
        if (ShouldFailTransactionOperation?.Invoke(context) == true)
            throw (TransactionOperationExceptionFactory?.Invoke(context)
                ?? new IOException("Injected transaction operation failure."));
    }

    private void ApplyLatency()
    {
        if (Latency > TimeSpan.Zero)
            Thread.Sleep(Latency);
    }
}
