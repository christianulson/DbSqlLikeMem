namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides the common transaction lifecycle shared by provider-specific mock transactions.
/// PT: Fornece o ciclo de vida comum de transacao compartilhado pelas transacoes simuladas especificas de provedor.
/// </summary>
/// <typeparam name="TConnection">EN: Provider-specific connection type. PT: Tipo de conexao especifico do provedor.</typeparam>
public abstract class DbTransactionMockBase<TConnection>(
    TConnection connection,
    IsolationLevel? isolationLevel = null) : DbTransaction
    where TConnection : DbConnectionMockBase
{
    private bool _disposed;

    /// <summary>
    /// EN: Gets the provider-specific connection associated with this transaction.
    /// PT: Obtem a conexao especifica do provedor associada a esta transacao.
    /// </summary>
    protected new TConnection Connection { get; } = connection;

    /// <inheritdoc />
    protected override DbConnection? DbConnection => Connection;

    /// <inheritdoc />
    public override IsolationLevel IsolationLevel
        => isolationLevel ?? IsolationLevel.Unspecified;

    /// <inheritdoc />
    public override void Commit()
    {
        lock (Connection.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Committed");
            Connection.CommitTransaction();
        }
    }

    /// <inheritdoc />
    public override void Rollback()
    {
        lock (Connection.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Rolled Back");
            Connection.RollbackTransaction();
        }
    }

    #if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Save(string savepointName)
    #else
    /// <summary>
    /// EN: Creates a savepoint in the active transaction.
    /// PT: Cria um savepoint na transacao ativa.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Save(string savepointName)
    #endif
    {
        lock (Connection.Db.SyncRoot)
            Connection.CreateSavepoint(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Rollback(string savepointName)
    #else
    /// <summary>
    /// EN: Rolls back to a named savepoint.
    /// PT: Executa rollback para um savepoint nomeado.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Rollback(string savepointName)
    #endif
    {
        lock (Connection.Db.SyncRoot)
            Connection.RollbackTransaction(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Release(string savepointName)
    #else
    /// <summary>
    /// EN: Releases a named savepoint.
    /// PT: Libera um savepoint nomeado.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Release(string savepointName)
    #endif
    {
        lock (Connection.Db.SyncRoot)
            Connection.ReleaseSavepoint(savepointName);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
            _disposed = true;

        base.Dispose(disposing);
    }
}
