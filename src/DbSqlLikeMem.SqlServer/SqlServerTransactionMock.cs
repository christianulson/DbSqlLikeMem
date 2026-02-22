using System.Diagnostics;

namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents Sql Server Transaction Mock.
/// PT: Representa Sql Server Transaction simulado.
/// </summary>
public sealed class SqlServerTransactionMock(
    SqlServerConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransaction
{
    private bool disposedValue;

    /// <summary>
    /// EN: Gets or sets db connection.
    /// PT: Obtém ou define db conexão.
    /// </summary>
    protected override DbConnection? DbConnection => cnn;

    /// <summary>
    /// EN: Represents Isolation Level.
    /// PT: Representa Isolation Level.
    /// </summary>
    public override IsolationLevel IsolationLevel
        => isolationLevel ?? IsolationLevel.Unspecified;

    /// <summary>
    /// EN: Commits the current transaction.
    /// PT: Confirma a transação atual.
    /// </summary>
    public override void Commit()
    {
        lock (cnn.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Committed");
            cnn.CommitTransaction();
        }
    }

    /// <summary>
    /// EN: Rolls back the current transaction.
    /// PT: Reverte a transação atual.
    /// </summary>
    public override void Rollback()
    {
        lock (cnn.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Rolled Back");
            cnn.RollbackTransaction();
        }
    }

    #if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Creates a transaction savepoint.
    /// PT: Cria um ponto de salvamento da transação.
    /// </summary>
    public override void Save(string savepointName)
#else
    /// <summary>
    /// EN: Creates a transaction savepoint.
    /// PT: Cria um ponto de salvamento da transação.
    /// </summary>
    public void Save(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.CreateSavepoint(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Rolls back the current transaction.
    /// PT: Reverte a transação atual.
    /// </summary>
    public override void Rollback(string savepointName)
#else
    /// <summary>
    /// EN: Rolls back the current transaction.
    /// PT: Reverte a transação atual.
    /// </summary>
    public void Rollback(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.RollbackTransaction(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Releases a previously created savepoint.
    /// PT: Libera um ponto de salvamento criado anteriormente.
    /// </summary>
    public override void Release(string savepointName)
#else
    /// <summary>
    /// EN: Releases a previously created savepoint.
    /// PT: Libera um ponto de salvamento criado anteriormente.
    /// </summary>
    public void Release(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.ReleaseSavepoint(savepointName);
    }

    /// <summary>
    /// EN: Releases resources used by this instance.
    /// PT: Libera os recursos usados por esta instância.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
