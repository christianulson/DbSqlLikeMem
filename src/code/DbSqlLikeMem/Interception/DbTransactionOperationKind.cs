namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which transaction lifecycle operation is being intercepted.
/// PT-br: Identifica qual operacao do ciclo de vida transacional esta sendo interceptada.
/// </summary>
public enum DbTransactionOperationKind
{
    /// <summary>
    /// EN: Begins a transaction.
    /// PT-br: Inicia uma transacao.
    /// </summary>
    Begin,

    /// <summary>
    /// EN: Commits a transaction.
    /// PT-br: Confirma uma transacao.
    /// </summary>
    Commit,

    /// <summary>
    /// EN: Rolls a transaction back.
    /// PT-br: Desfaz uma transacao.
    /// </summary>
    Rollback
}
