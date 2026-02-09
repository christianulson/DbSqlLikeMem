using System.Data.Common;
using System.Diagnostics;

namespace DbSqlLikeMem.Oracle;

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

    public override IsolationLevel IsolationLevel
        => isolationLevel ?? IsolationLevel.Unspecified;

    public override void Commit()
    {
        lock (cnn.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Committed");
            cnn.CommitTransaction();
        }
    }

    public override void Rollback()
    {
        lock (cnn.Db.SyncRoot)
        {
            Debug.WriteLine("Transaction Rolled Back");
            cnn.RollbackTransaction();
        }
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
