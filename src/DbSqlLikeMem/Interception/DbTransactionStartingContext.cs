namespace DbSqlLikeMem;

/// <summary>
/// EN: Carries the connection and isolation level used when a transaction is about to start.
/// PT: Transporta a conexao e o nivel de isolamento usados quando uma transacao esta prestes a iniciar.
/// </summary>
public sealed class DbTransactionStartingContext(
    DbConnection connection,
    IsolationLevel isolationLevel)
{
    /// <summary>
    /// EN: Connection starting the transaction.
    /// PT: Conexao que esta iniciando a transacao.
    /// </summary>
    public DbConnection Connection { get; } = connection;

    /// <summary>
    /// EN: Isolation level requested for the transaction.
    /// PT: Nivel de isolamento solicitado para a transacao.
    /// </summary>
    public IsolationLevel IsolationLevel { get; } = isolationLevel;
}
