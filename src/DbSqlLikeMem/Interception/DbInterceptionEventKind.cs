namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which interception event was recorded by the ADO.NET pipeline.
/// PT: Identifica qual evento de interceptacao foi registrado pelo pipeline ADO.NET.
/// </summary>
public enum DbInterceptionEventKind
{
    /// <summary>
    /// EN: Connection opening started.
    /// PT: A abertura da conexao foi iniciada.
    /// </summary>
    ConnectionOpening,

    /// <summary>
    /// EN: Connection opening completed.
    /// PT: A abertura da conexao foi concluida.
    /// </summary>
    ConnectionOpened,

    /// <summary>
    /// EN: Connection closing started.
    /// PT: O fechamento da conexao foi iniciado.
    /// </summary>
    ConnectionClosing,

    /// <summary>
    /// EN: Connection closing completed.
    /// PT: O fechamento da conexao foi concluido.
    /// </summary>
    ConnectionClosed,

    /// <summary>
    /// EN: A command was created.
    /// PT: Um comando foi criado.
    /// </summary>
    CommandCreated,

    /// <summary>
    /// EN: A command execution started.
    /// PT: A execucao de um comando foi iniciada.
    /// </summary>
    CommandExecuting,

    /// <summary>
    /// EN: A command execution completed.
    /// PT: A execucao de um comando foi concluida.
    /// </summary>
    CommandExecuted,

    /// <summary>
    /// EN: A command execution failed.
    /// PT: A execucao de um comando falhou.
    /// </summary>
    CommandFailed,

    /// <summary>
    /// EN: A transaction start was requested.
    /// PT: O inicio de uma transacao foi solicitado.
    /// </summary>
    TransactionStarting,

    /// <summary>
    /// EN: A transaction started successfully.
    /// PT: Uma transacao iniciou com sucesso.
    /// </summary>
    TransactionStarted,

    /// <summary>
    /// EN: A transaction operation started.
    /// PT: Uma operacao transacional foi iniciada.
    /// </summary>
    TransactionExecuting,

    /// <summary>
    /// EN: A transaction operation completed.
    /// PT: Uma operacao transacional foi concluida.
    /// </summary>
    TransactionExecuted,

    /// <summary>
    /// EN: A transaction operation failed.
    /// PT: Uma operacao transacional falhou.
    /// </summary>
    TransactionFailed
}
