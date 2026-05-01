namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the activity source and activity names emitted by the interception pipeline.
/// PT-br: Define os nomes de source e activities emitidos pelo pipeline de interceptacao.
/// </summary>
public static class DbInterceptionActivityNames
{
    /// <summary>
    /// EN: Default activity source name used by the interception pipeline.
    /// PT-br: Nome padrao do activity source usado pelo pipeline de interceptacao.
    /// </summary>
    public const string SourceName = "DbSqlLikeMem.Interception";

    /// <summary>
    /// EN: Activity name for connection open.
    /// PT-br: Nome da activity para abertura de conexao.
    /// </summary>
    public const string ConnectionOpen = SourceName + ".ConnectionOpen";

    /// <summary>
    /// EN: Activity name for connection close.
    /// PT-br: Nome da activity para fechamento de conexao.
    /// </summary>
    public const string ConnectionClose = SourceName + ".ConnectionClose";

    /// <summary>
    /// EN: Activity name for command execution.
    /// PT-br: Nome da activity para execucao de comando.
    /// </summary>
    public const string Command = SourceName + ".Command";

    /// <summary>
    /// EN: Activity name for transaction begin.
    /// PT-br: Nome da activity para inicio de transacao.
    /// </summary>
    public const string TransactionBegin = SourceName + ".TransactionBegin";

    /// <summary>
    /// EN: Activity name for transaction operations such as commit and rollback.
    /// PT-br: Nome da activity para operacoes transacionais como commit e rollback.
    /// </summary>
    public const string TransactionOperation = SourceName + ".TransactionOperation";
}
