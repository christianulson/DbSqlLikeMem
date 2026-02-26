using System.Diagnostics;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents Npgsql Transaction Mock.
/// PT: Representa a transação simulada do Npgsql.
/// </summary>
public sealed class NpgsqlTransactionMock(
    NpgsqlConnectionMock cnn,
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
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
