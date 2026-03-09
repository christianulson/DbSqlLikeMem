namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the diagnostic source and event names emitted by the interception pipeline.
/// PT: Define os nomes de source e eventos de diagnostico emitidos pelo pipeline de interceptacao.
/// </summary>
public static class DbInterceptionDiagnosticNames
{
    /// <summary>
    /// EN: Default diagnostic listener name used by the interception pipeline.
    /// PT: Nome padrao do diagnostic listener usado pelo pipeline de interceptacao.
    /// </summary>
    public const string ListenerName = "DbSqlLikeMem.Interception";

    /// <summary>
    /// EN: Event name for connection opening.
    /// PT: Nome do evento para abertura de conexao.
    /// </summary>
    public const string ConnectionOpening = ListenerName + ".ConnectionOpening";

    /// <summary>
    /// EN: Event name for connection opened.
    /// PT: Nome do evento para conexao aberta.
    /// </summary>
    public const string ConnectionOpened = ListenerName + ".ConnectionOpened";

    /// <summary>
    /// EN: Event name for connection closing.
    /// PT: Nome do evento para fechamento de conexao.
    /// </summary>
    public const string ConnectionClosing = ListenerName + ".ConnectionClosing";

    /// <summary>
    /// EN: Event name for connection closed.
    /// PT: Nome do evento para conexao fechada.
    /// </summary>
    public const string ConnectionClosed = ListenerName + ".ConnectionClosed";

    /// <summary>
    /// EN: Event name for command creation.
    /// PT: Nome do evento para criacao de comando.
    /// </summary>
    public const string CommandCreated = ListenerName + ".CommandCreated";

    /// <summary>
    /// EN: Event name for command executing.
    /// PT: Nome do evento para execucao de comando iniciada.
    /// </summary>
    public const string CommandExecuting = ListenerName + ".CommandExecuting";

    /// <summary>
    /// EN: Event name for command executed.
    /// PT: Nome do evento para execucao de comando concluida.
    /// </summary>
    public const string CommandExecuted = ListenerName + ".CommandExecuted";

    /// <summary>
    /// EN: Event name for command failure.
    /// PT: Nome do evento para falha de comando.
    /// </summary>
    public const string CommandFailed = ListenerName + ".CommandFailed";

    /// <summary>
    /// EN: Event name for transaction starting.
    /// PT: Nome do evento para inicio de transacao solicitado.
    /// </summary>
    public const string TransactionStarting = ListenerName + ".TransactionStarting";

    /// <summary>
    /// EN: Event name for transaction started.
    /// PT: Nome do evento para transacao iniciada.
    /// </summary>
    public const string TransactionStarted = ListenerName + ".TransactionStarted";

    /// <summary>
    /// EN: Event name for transaction executing.
    /// PT: Nome do evento para operacao transacional iniciada.
    /// </summary>
    public const string TransactionExecuting = ListenerName + ".TransactionExecuting";

    /// <summary>
    /// EN: Event name for transaction executed.
    /// PT: Nome do evento para operacao transacional concluida.
    /// </summary>
    public const string TransactionExecuted = ListenerName + ".TransactionExecuted";

    /// <summary>
    /// EN: Event name for transaction failure.
    /// PT: Nome do evento para falha de operacao transacional.
    /// </summary>
    public const string TransactionFailed = ListenerName + ".TransactionFailed";
}
