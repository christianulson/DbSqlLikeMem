namespace DbSqlLikeMem;

/// <summary>
/// EN: Represents one event captured by the ADO.NET interception pipeline.
/// PT: Representa um evento capturado pelo pipeline de interceptacao ADO.NET.
/// </summary>
public sealed class DbInterceptionEvent
{
    /// <summary>
    /// EN: Event kind recorded by the interceptor.
    /// PT: Tipo de evento registrado pelo interceptor.
    /// </summary>
    public DbInterceptionEventKind EventKind { get; init; }

    /// <summary>
    /// EN: UTC timestamp when the event was recorded.
    /// PT: Timestamp UTC em que o evento foi registrado.
    /// </summary>
    public DateTimeOffset TimestampUtc { get; init; }

    /// <summary>
    /// EN: Current connection state when the event was recorded.
    /// PT: Estado atual da conexao quando o evento foi registrado.
    /// </summary>
    public ConnectionState ConnectionState { get; init; }

    /// <summary>
    /// EN: Command text associated with the event when available.
    /// PT: Texto do comando associado ao evento quando disponivel.
    /// </summary>
    public string? CommandText { get; init; }

    /// <summary>
    /// EN: Command execution mode associated with the event when available.
    /// PT: Modo de execucao de comando associado ao evento quando disponivel.
    /// </summary>
    public DbCommandExecutionKind? CommandExecutionKind { get; init; }

    /// <summary>
    /// EN: Transaction operation associated with the event when available.
    /// PT: Operacao transacional associada ao evento quando disponivel.
    /// </summary>
    public DbTransactionOperationKind? TransactionOperationKind { get; init; }

    /// <summary>
    /// EN: Isolation level associated with the event when available.
    /// PT: Nivel de isolamento associado ao evento quando disponivel.
    /// </summary>
    public IsolationLevel? IsolationLevel { get; init; }

    /// <summary>
    /// EN: Result object captured for successful command execution events.
    /// PT: Objeto de resultado capturado para eventos de execucao bem-sucedida de comando.
    /// </summary>
    public object? Result { get; init; }

    /// <summary>
    /// EN: Exception captured for failure events.
    /// PT: Excecao capturada para eventos de falha.
    /// </summary>
    public Exception? Exception { get; init; }
}
