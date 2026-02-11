using System.Diagnostics;

namespace DbSqlLikeMem.MySql;
/// <summary>
/// EN: Mock transaction for MySQL connections.
/// PT: Mock de transação para conexões MySQL.
/// </summary>
public class MySqlTransactionMock(
        MySqlConnectionMock cnn,
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
    /// EN: Disposes the transaction resources.
    /// PT: Descarta os recursos da transação.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
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
    // ~MySqlTransactionMock()

#pragma warning disable S125 // Sections of code should not be commented out
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

}
