namespace DbSqlLikeMem;

/// <summary>
/// EN: Carries the connection, transaction, and operation observed by a transaction interception callback.
/// PT: Transporta a conexao, a transacao e a operacao observadas por um callback de interceptacao transacional.
/// </summary>
public sealed class DbTransactionInterceptionContext(
    DbConnection connection,
    DbTransaction transaction,
    DbTransactionOperationKind operationKind)
{
    /// <summary>
    /// EN: Connection associated with the transaction.
    /// PT: Conexao associada a transacao.
    /// </summary>
    public DbConnection Connection { get; } = connection;

    /// <summary>
    /// EN: Transaction being intercepted.
    /// PT: Transacao sendo interceptada.
    /// </summary>
    public DbTransaction Transaction { get; } = transaction;

    /// <summary>
    /// EN: Transaction operation being intercepted.
    /// PT: Operacao transacional sendo interceptada.
    /// </summary>
    public DbTransactionOperationKind OperationKind { get; } = operationKind;
}
