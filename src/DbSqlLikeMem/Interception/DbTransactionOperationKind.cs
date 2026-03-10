namespace DbSqlLikeMem;

/// <summary>
/// EN: Identifies which transaction lifecycle operation is being intercepted.
/// PT: Identifica qual operacao do ciclo de vida transacional esta sendo interceptada.
/// </summary>
public enum DbTransactionOperationKind
{
    /// <summary>
    /// EN: Begins a transaction.
    /// PT: Inicia uma transacao.
    /// </summary>
    Begin,

    /// <summary>
    /// EN: Commits a transaction.
    /// PT: Confirma uma transacao.
    /// </summary>
    Commit,

    /// <summary>
    /// EN: Rolls a transaction back.
    /// PT: Desfaz uma transacao.
    /// </summary>
    Rollback
}
