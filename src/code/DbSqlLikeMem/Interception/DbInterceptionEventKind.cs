namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which interception event was recorded by the ADO.NET pipeline.
/// PT-br: Identifica qual evento de interceptacao foi registrado pelo pipeline ADO.NET.
/// </summary>
public enum DbInterceptionEventKind
{
    /// <summary>
    /// EN: Connection opening started.
    /// PT-br: A abertura da conexao foi iniciada.
    /// </summary>
    ConnectionOpening,

    /// <summary>
    /// EN: Connection opening completed.
    /// PT-br: A abertura da conexao foi concluida.
    /// </summary>
    ConnectionOpened,

    /// <summary>
    /// EN: Connection closing started.
    /// PT-br: O fechamento da conexao foi iniciado.
    /// </summary>
    ConnectionClosing,

    /// <summary>
    /// EN: Connection closing completed.
    /// PT-br: O fechamento da conexao foi concluido.
    /// </summary>
    ConnectionClosed,

    /// <summary>
    /// EN: A command was created.
    /// PT-br: Um comando foi criado.
    /// </summary>
    CommandCreated,

    /// <summary>
    /// EN: A command execution started.
    /// PT-br: A execucao de um comando foi iniciada.
    /// </summary>
    CommandExecuting,

    /// <summary>
    /// EN: A command execution completed.
    /// PT-br: A execucao de um comando foi concluida.
    /// </summary>
    CommandExecuted,

    /// <summary>
    /// EN: A command execution failed.
    /// PT-br: A execucao de um comando falhou.
    /// </summary>
    CommandFailed,

    /// <summary>
    /// EN: A transaction start was requested.
    /// PT-br: O inicio de uma transacao foi solicitado.
    /// </summary>
    TransactionStarting,

    /// <summary>
    /// EN: A transaction started successfully.
    /// PT-br: Uma transacao iniciou com sucesso.
    /// </summary>
    TransactionStarted,

    /// <summary>
    /// EN: A transaction operation started.
    /// PT-br: Uma operacao transacional foi iniciada.
    /// </summary>
    TransactionExecuting,

    /// <summary>
    /// EN: A transaction operation completed.
    /// PT-br: Uma operacao transacional foi concluida.
    /// </summary>
    TransactionExecuted,

    /// <summary>
    /// EN: A transaction operation failed.
    /// PT-br: Uma operacao transacional falhou.
    /// </summary>
    TransactionFailed
}
