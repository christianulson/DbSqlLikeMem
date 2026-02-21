using System.Diagnostics;

namespace DbSqlLikeMem.Sqlite;
/// <summary>
/// EN: Summary for SqliteTransactionMock.
/// PT: Resumo para SqliteTransactionMock.
/// </summary>
public class SqliteTransactionMock(
        SqliteConnectionMock cnn,
        IsolationLevel? isolationLevel = null
    ) : DbTransaction
{
    private bool disposedValue;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    protected override DbConnection? DbConnection => cnn;

    /// <summary>
    /// EN: Summary for IsolationLevel.
    /// PT: Resumo para IsolationLevel.
    /// </summary>
    public override IsolationLevel IsolationLevel
        => isolationLevel ?? IsolationLevel.Unspecified;

    /// <summary>
    /// EN: Summary for Commit.
    /// PT: Resumo para Commit.
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
    /// EN: Summary for Rollback.
    /// PT: Resumo para Rollback.
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
    /// EN: Summary for Save.
    /// PT: Resumo para Save.
    /// </summary>
    public override void Save(string savepointName)
#else
    /// <summary>
    /// EN: Summary for Save.
    /// PT: Resumo para Save.
    /// </summary>
    public void Save(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.CreateSavepoint(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for Rollback.
    /// PT: Resumo para Rollback.
    /// </summary>
    public override void Rollback(string savepointName)
#else
    /// <summary>
    /// EN: Summary for Rollback.
    /// PT: Resumo para Rollback.
    /// </summary>
    public void Rollback(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.RollbackTransaction(savepointName);
    }

    #if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for Release.
    /// PT: Resumo para Release.
    /// </summary>
    public override void Release(string savepointName)
#else
    /// <summary>
    /// EN: Summary for Release.
    /// PT: Resumo para Release.
    /// </summary>
    public void Release(string savepointName)
#endif
    {
        lock (cnn.Db.SyncRoot)
            cnn.ReleaseSavepoint(savepointName);
    }

    /// <summary>
    /// EN: Summary for Dispose.
    /// PT: Resumo para Dispose.
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
