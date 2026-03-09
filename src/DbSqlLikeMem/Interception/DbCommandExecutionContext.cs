namespace DbSqlLikeMem;

/// <summary>
/// EN: Carries the command instance and execution mode seen by an interception callback.
/// PT: Transporta a instancia do comando e o modo de execucao vistos por um callback de interceptacao.
/// </summary>
public sealed class DbCommandExecutionContext(
    DbConnection connection,
    DbCommand command,
    DbCommandExecutionKind executionKind)
{
    /// <summary>
    /// EN: Connection associated with the intercepted command.
    /// PT: Conexao associada ao comando interceptado.
    /// </summary>
    public DbConnection Connection { get; } = connection;

    /// <summary>
    /// EN: Command being intercepted.
    /// PT: Comando sendo interceptado.
    /// </summary>
    public DbCommand Command { get; } = command;

    /// <summary>
    /// EN: Execution mode requested for the command.
    /// PT: Modo de execucao solicitado para o comando.
    /// </summary>
    public DbCommandExecutionKind ExecutionKind { get; } = executionKind;
}
