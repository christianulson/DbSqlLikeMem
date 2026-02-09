using System.Data.Common;
using System.Diagnostics;

namespace DbSqlLikeMem.SqlServer;

public sealed class SqlServerTransactionMock(
    SqlServerConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransaction
{
    private bool disposedValue;

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

    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
