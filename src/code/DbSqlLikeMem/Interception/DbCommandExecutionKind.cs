namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which ADO.NET command execution path is being intercepted.
/// PT: Identifica qual caminho de execucao de comando ADO.NET esta sendo interceptado.
/// </summary>
public enum DbCommandExecutionKind
{
    /// <summary>
    /// EN: Executes a non-query command.
    /// PT: Executa um comando non-query.
    /// </summary>
    NonQuery,

    /// <summary>
    /// EN: Executes a scalar command.
    /// PT: Executa um comando scalar.
    /// </summary>
    Scalar,

    /// <summary>
    /// EN: Executes a reader command.
    /// PT: Executa um comando reader.
    /// </summary>
    Reader
}
