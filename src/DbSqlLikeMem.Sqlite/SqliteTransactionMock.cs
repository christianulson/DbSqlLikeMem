using System.Diagnostics;

namespace DbSqlLikeMem.Sqlite;
/// <summary>
/// EN: Represents Sqlite Transaction Mock.
/// PT: Representa a transação simulada do SQLite.
/// </summary>
public class SqliteTransactionMock(
        SqliteConnectionMock cnn,
        IsolationLevel? isolationLevel = null
    ) : DbTransaction
{
    private bool disposedValue;

    /// <summary>
    /// EN: Gets the database connection associated with this transaction.
    /// PT: Obtém a conexão de banco de dados associada a esta transação.
    /// </summary>
    protected override DbConnection? DbConnection => cnn;

    /// <summary>
    /// EN: Gets the isolation level configured for this transaction.
    /// PT: Obtém o nível de isolamento configurado para esta transação.
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
    /// EN: Rolls back the current transaction to the specified savepoint.
    /// PT: Reverte a transação atual até o ponto de salvamento informado.
    /// </summary>
    public override void Rollback(string savepointName)
#else
    /// <summary>
    /// EN: Rolls back the current transaction to the specified savepoint.
    /// PT: Reverte a transação atual até o ponto de salvamento informado.
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
            if (disposing)
#pragma warning disable S1135 // Track uses of "TODO" tags
            {
                // TODO: dispose managed state (managed objects)
            }
#pragma warning restore S1135 // Track uses of "TODO" tags


#pragma warning disable S1135 // Track uses of "TODO" tags
            // TODO: free unmanaged resources (unmanaged objects) and override finalizer

#pragma warning disable S1135 // Track uses of "TODO" tags
            // TODO: set large fields to null
            disposedValue = true;
#pragma warning restore S1135 // Track uses of "TODO" tags
#pragma warning restore S1135 // Track uses of "TODO" tags
        }
        base.Dispose(disposing);
    }


#pragma warning disable S1135 // Track uses of "TODO" tags
    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~SqliteTransactionMock()

#pragma warning disable S125 // Sections of code should not be commented out
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

}
