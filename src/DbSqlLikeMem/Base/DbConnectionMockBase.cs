using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;

/// <summary>
/// Conexão mock em memória com suporte a tabelas, transações e latência simulada.
/// </summary>
public abstract class DbConnectionMockBase(
    DbMock db,
    string? defaultDatabase = null
    ) : DbConnection
{
    private bool _disposed;

    /// <summary>
    /// Banco em memória associado a esta conexão.
    /// </summary>
    public DbMock Db { get; } = db;

    /// <summary>
    /// Métricas de uso e desempenho da conexão.
    /// </summary>
    public DbMetrics Metrics { get; } = new();

    /// <summary>
    /// Latência simulada em milissegundos para cada operação.
    /// </summary>
    public int SimulatedLatencyMs { get; set; }
    /// <summary>
    /// Probabilidade de falha simulada de rede (0 a 1).
    /// </summary>
    public double DropProbability { get; set; }

    [AllowNull]
    /// <summary>
    /// String de conexão simulada.
    /// </summary>
    public override string ConnectionString { get; set; } = "";

    /// <summary>
    /// Tempo limite de conexão simulado.
    /// </summary>
    public override int ConnectionTimeout { get; } = 1;

    private string _database = defaultDatabase ?? db.GetSchemaName(null);
    /// <summary>
    /// Nome do banco/schema atual.
    /// </summary>
    public override string Database => _database;

    private ConnectionState _state = ConnectionState.Closed;
    /// <summary>
    /// Estado atual da conexão.
    /// </summary>
    public override ConnectionState State => _state;

    private readonly string _dataSource = "";
    /// <summary>
    /// Fonte de dados simulada.
    /// </summary>
    public override string DataSource => _dataSource;

    protected string _serverVersion = "";
    /// <summary>
    /// Versão do servidor simulada.
    /// </summary>
    public override string ServerVersion => _serverVersion;

    protected DbTransaction? CurrentTransaction { get; private set; }

    #region Table
    
    /// <summary>
    /// Cria e adiciona uma tabela ao banco.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="columns">Colunas da tabela.</param>
    /// <param name="rows">Linhas iniciais.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>Tabela criada.</returns>
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

    /// <summary>
    /// Obtém uma tabela pelo nome, lançando erro se não existir.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>Tabela encontrada.</returns>
    public ITableMock GetTable(
        string tableName,
        string? schemaName = null)
    => Db.GetTable(
        tableName,
        schemaName ?? Database);

    /// <summary>
    /// Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="tb">Tabela encontrada, se houver.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>True se existir.</returns>
    public bool TryGetTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    => Db.TryGetTable(
        tableName,
        out tb,
        schemaName ?? Database);

    /// <summary>
    /// Lista as tabelas do schema atual.
    /// </summary>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>Lista de tabelas.</returns>
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

    /// <summary>
    /// Registra um procedimento armazenado.
    /// </summary>
    /// <param name="procName">Nome do procedimento.</param>
    /// <param name="pr">Definição do procedimento.</param>
    /// <param name="schemaName">Schema alvo.</param>
    public void AddProdecure(
        string procName,
        ProcedureDef pr,
        string? schemaName = null)
        => Db.AddProdecure(
            procName,
            pr, 
            schemaName ?? Database);

    /// <summary>
    /// Tenta obter um procedimento armazenado.
    /// </summary>
    /// <param name="procName">Nome do procedimento.</param>
    /// <param name="pr">Procedimento encontrado, se houver.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>True se existir.</returns>
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

    /// <summary>
    /// Altera o database/schema atual da conexão.
    /// </summary>
    /// <param name="databaseName">Nome do database.</param>
    public override void ChangeDatabase(string databaseName)
        => _database = databaseName;

    /// <summary>
    /// Fecha a conexão simulada.
    /// </summary>
    public override void Close()
        => _state = ConnectionState.Closed;

    protected override DbCommand CreateDbCommand()
        => CreateDbCommandCore(CurrentTransaction);

    protected abstract DbCommand CreateDbCommandCore(
        DbTransaction? transaction);

    /// <summary>
    /// Abre a conexão simulada.
    /// </summary>
    public override void Open()
    {
        _state = ConnectionState.Open;
        Debug.WriteLine("Opended");
    }

    /// <summary>
    /// Confirma a transação atual, liberando backups.
    /// </summary>
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

    /// <summary>
    /// Desfaz a transação atual restaurando o backup.
    /// </summary>
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
