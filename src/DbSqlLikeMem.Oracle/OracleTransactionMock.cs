using System.Data.Common;
using System.Diagnostics;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class OracleTransactionMock(
    OracleConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransaction
{
    private bool disposedValue;

    /// <summary>
    /// EN: Gets the connection associated with this transaction.
    /// PT: Obtém a conexão associada a esta transação.
    /// </summary>
    protected override DbConnection? DbConnection => cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override IsolationLevel IsolationLevel
        => isolationLevel ?? IsolationLevel.Unspecified;

    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
    /// </summary>
    public override void Rollback()
    {
        lock (cnn.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Rolled Back");
            cnn.RollbackTransaction();
        }
    }

    /// <summary>
    /// EN: Creates a savepoint in the active transaction.
    /// PT: Cria um savepoint na transação ativa.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Save(string savepointName)
    {
        lock (cnn.Db.SyncRoot)
            cnn.CreateSavepoint(savepointName);
    }

    /// <summary>
    /// EN: Rolls back to a named savepoint.
    /// PT: Executa rollback para um savepoint nomeado.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Rollback(string savepointName)
    {
        lock (cnn.Db.SyncRoot)
            cnn.RollbackTransaction(savepointName);
    }

    /// <summary>
    /// EN: Releases a named savepoint.
    /// PT: Libera um savepoint nomeado.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Release(string savepointName)
    {
        lock (cnn.Db.SyncRoot)
            cnn.ReleaseSavepoint(savepointName);
    }

    /// <summary>
    /// EN: Disposes the transaction resources.
    /// PT: Descarta os recursos da transação.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
