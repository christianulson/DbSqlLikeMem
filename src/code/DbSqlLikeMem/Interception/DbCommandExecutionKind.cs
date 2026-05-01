namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which ADO.NET command execution path is being intercepted.
/// PT-br: Identifica qual caminho de execucao de comando ADO.NET esta sendo interceptado.
/// </summary>
public enum DbCommandExecutionKind
{
    /// <summary>
    /// EN: Executes a non-query command.
    /// PT-br: Executa um comando non-query.
    /// </summary>
    NonQuery,

    /// <summary>
    /// EN: Executes a scalar command.
    /// PT-br: Executa um comando scalar.
    /// </summary>
    Scalar,

    /// <summary>
    /// EN: Executes a reader command.
    /// PT-br: Executa um comando reader.
    /// </summary>
    Reader
}
