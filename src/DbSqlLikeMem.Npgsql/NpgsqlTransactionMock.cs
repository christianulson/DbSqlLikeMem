using System.Data.Common;
using System.Diagnostics;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Summary for NpgsqlTransactionMock.
/// PT: Resumo para NpgsqlTransactionMock.
/// </summary>
public sealed class NpgsqlTransactionMock(
    NpgsqlConnectionMock cnn,
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
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
