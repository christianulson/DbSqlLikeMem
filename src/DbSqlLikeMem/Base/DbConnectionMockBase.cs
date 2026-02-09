using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;

/// <summary>
/// Generic in-memory DbConnection mock with:
/// - table registry by name
/// - optional thread-safety via SyncRoot
/// - transaction backup/restore semantics (best-effort via reflection: Backup/Restore/ClearBackup)
/// - simulated latency / drop
/// </summary>
public abstract class DbConnectionMockBase(
    DbMock db,
    string? defaultDatabase = null
    ) : DbConnection
{
    private bool _disposed;

    /// <summary>Underlying table map. Names are normalized on access.</summary>
    public DbMock Db { get; } = db;

    public DbMetrics Metrics { get; } = new();

    public int SimulatedLatencyMs { get; set; }
    public double DropProbability { get; set; }

    [AllowNull]
    public override string ConnectionString { get; set; } = "";

    public override int ConnectionTimeout { get; } = 1;

    private string _database = defaultDatabase ?? db.GetSchemaName(null);
    public override string Database => _database;

    private ConnectionState _state = ConnectionState.Closed;
    public override ConnectionState State => _state;

    private readonly string _dataSource = "";
    public override string DataSource => _dataSource;

    protected string _serverVersion = "";
    public override string ServerVersion => _serverVersion;

    protected DbTransaction? CurrentTransaction { get; private set; }

    #region Table
    
    public ITableMock AddTable(
        string tableName,
        IColumnDictionary? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
        => Db.AddTable(
            tableName,
            columns,
            rows,
            schemaName ?? Database);

    public ITableMock GetTable(
        string tableName,
        string? schemaName = null)
    => Db.GetTable(
        tableName,
        schemaName ?? Database);

    public bool TryGetTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    => Db.TryGetTable(
        tableName,
        out tb,
        schemaName ?? Database);

    public IReadOnlyList<ITableMock> ListTables(
        string? schemaName = null)
        => Db.ListTables(schemaName ?? Database);

    #endregion

    #region View

    internal void AddView(
        SqlCreateViewQuery query)
        => Db.AddView(
            query,
            query.Table?.DbName ?? Database);

    internal SqlSelectQuery GetView(
        string viewName,
        string? schemaName = null)
        => Db.GetView(
            viewName,
            schemaName ?? Database);

    internal bool TryGetView(
        string viewName,
        out SqlSelectQuery? vw,
        string? schemaName = null)
        => Db.TryGetView(
            viewName,
            out vw,
            schemaName ?? Database);

    #endregion

    #region Procedures

    public void AddProdecure(
        string procName,
        ProcedureDef pr,
        string? schemaName = null)
        => Db.AddProdecure(
            procName,
            pr, 
            schemaName ?? Database);

    public bool TryGetProcedure(
        string procName,
        out ProcedureDef? pr,
        string? schemaName = null)
        => Db.TryGetProcedure(
            procName,
            out pr,
            schemaName ?? Database);

    #endregion

    protected override DbTransaction BeginDbTransaction(
        IsolationLevel isolationLevel)
    {
        if (!Db.ThreadSafe)
            return BeginTransactionCore();

        lock (Db.SyncRoot)
            return BeginTransactionCore();
    }

    private DbTransaction BeginTransactionCore()
    {
        CurrentTransaction = CreateTransaction();
        Db.BackupAllTablesBestEffort();
        return CurrentTransaction;
    }

    protected abstract DbTransaction CreateTransaction();

    public override void ChangeDatabase(string databaseName)
        => _database = databaseName;

    public override void Close()
        => _state = ConnectionState.Closed;

    protected override DbCommand CreateDbCommand()
        => CreateDbCommandCore(CurrentTransaction);

    protected abstract DbCommand CreateDbCommandCore(
        DbTransaction? transaction);

    public override void Open()
    {
        _state = ConnectionState.Open;
        Debug.WriteLine("Opended");
    }

    public void CommitTransaction()
    {
        if (!Db.ThreadSafe)
        {
            CommitCore();
            return;
        }

        lock (Db.SyncRoot)
            CommitCore();
    }

    private void CommitCore()
    {
        Debug.WriteLine("Transaction Committed");
        Db.ClearBackupAllTablesBestEffort();
        CurrentTransaction = null;
    }

    public void RollbackTransaction()
    {
        if (!Db.ThreadSafe)
        {
            RollbackCore();
            return;
        }

        lock (Db.SyncRoot)
            RollbackCore();
    }

    private void RollbackCore()
    {
        Debug.WriteLine("Transaction Rolled Back");
        Db.RestoreAllTablesBestEffort();
        CurrentTransaction = null;
    }

    internal void MaybeDelayOrDrop()
    {
#pragma warning disable CA5394 // Do not use insecure randomness
        if (DropProbability > 0 && Random.Shared.NextDouble() < DropProbability)
            throw new IOException("Simulated network drop");
#pragma warning restore CA5394 // Do not use insecure randomness

        if (SimulatedLatencyMs > 0)
            Thread.Sleep(SimulatedLatencyMs);
    }

    internal abstract Exception NewException(string message, int code);

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            base.Dispose(disposing);
            return;
        }

        if (disposing)
        {
            CurrentTransaction?.Dispose();
        }

        _disposed = true;
        base.Dispose(disposing);
    }
}
