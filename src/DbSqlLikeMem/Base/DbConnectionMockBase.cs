using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;

/// <summary>
/// EN: In-memory mock connection with table support, transactions, and simulated latency.
/// PT: Conexão mock em memória com suporte a tabelas, transações e latência simulada.
/// </summary>
public abstract class DbConnectionMockBase(
    DbMock db,
    string? defaultDatabase = null
    ) : DbConnection
{
    private bool _disposed;

    /// <summary>
    /// EN: In-memory database associated with this connection.
    /// PT: Banco em memória associado a esta conexão.
    /// </summary>
    public DbMock Db { get; } = db;

    /// <summary>
    /// EN: Connection usage and performance metrics.
    /// PT: Métricas de uso e desempenho da conexão.
    /// </summary>
    public DbMetrics Metrics { get; } = new();

    /// <summary>
    /// EN: Simulated latency in milliseconds for each operation.
    /// PT: Latência simulada em milissegundos para cada operação.
    /// </summary>
    public int SimulatedLatencyMs { get; set; }
    /// <summary>
    /// EN: Probability of simulated network failure (0 to 1).
    /// PT: Probabilidade de falha simulada de rede (0 a 1).
    /// </summary>
    public double DropProbability { get; set; }

    /// <summary>
    /// EN: Simulated connection string.
    /// PT: String de conexão simulada.
    /// </summary>
    [AllowNull]
    public override string ConnectionString { get; set; } = "";

    /// <summary>
    /// EN: Simulated connection timeout.
    /// PT: Tempo limite de conexão simulado.
    /// </summary>
    public override int ConnectionTimeout { get; } = 1;

    private string _database = defaultDatabase ?? db.GetSchemaName(null);
    /// <summary>
    /// EN: Current database/schema name.
    /// PT: Nome do banco/schema atual.
    /// </summary>
    public override string Database => _database;

    private ConnectionState _state = ConnectionState.Closed;
    /// <summary>
    /// EN: Current connection state.
    /// PT: Estado atual da conexão.
    /// </summary>
    public override ConnectionState State => _state;

    private readonly string _dataSource = "";
    /// <summary>
    /// EN: Simulated data source.
    /// PT: Fonte de dados simulada.
    /// </summary>
    public override string DataSource => _dataSource;

    /// <summary>
    /// EN: Backing field for the simulated server version.
    /// PT: Campo de suporte para a versão simulada do servidor.
    /// </summary>
    protected string _serverVersion = "";
    /// <summary>
    /// EN: Simulated server version.
    /// PT: Versão do servidor simulada.
    /// </summary>
    public override string ServerVersion => _serverVersion;

    /// <summary>
    /// EN: Current active transaction, if any.
    /// PT: Transação ativa atual, se houver.
    /// </summary>
    protected DbTransaction? CurrentTransaction { get; private set; }

    private readonly Dictionary<string, ITableMock> _temporaryTables =
        new(StringComparer.OrdinalIgnoreCase);

    #region Table

    private string BuildTemporaryTableKey(
        string tableName,
        string? schemaName)
    {
        var schema = Db.GetSchemaName(schemaName ?? Database);
        return $"{schema}:{tableName.NormalizeName()}";
    }

    private IEnumerable<ITableMock> ListTemporaryTables(
        string? schemaName = null)
    {
        var schema = Db.GetSchemaName(schemaName ?? Database);
        var prefix = $"{schema}:";
        return _temporaryTables
            .Where(entry => entry.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Value);
    }

    /// <summary>
    /// EN: Creates and adds a temporary table to the connection scope.
    /// PT: Cria e adiciona uma tabela temporária no escopo da conexão.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Created table. PT: Tabela criada.</returns>
    public ITableMock AddTemporaryTable(
        string tableName,
        IColumnDictionary? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        var schemaKey = Db.GetSchemaName(schemaName ?? Database);
        var key = BuildTemporaryTableKey(tableName, schemaKey);
        if (!Db.TryGetValue(schemaKey, out var schemaMock) || schemaMock == null)
            schemaMock = Db.CreateSchema(schemaKey);
        var schema = (SchemaMock)schemaMock;
        var table = schema.CreateTableInstance(tableName, columns ?? new ColumnDictionary(), rows);
        _temporaryTables.Add(key, table);
        return table;
    }

    /// <summary>
    /// EN: Tries to get a temporary table by name.
    /// PT: Tenta obter uma tabela temporária pelo nome.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="tb">EN: Found table, if any. PT: Tabela encontrada, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool TryGetTemporaryTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
        => _temporaryTables.TryGetValue(
            BuildTemporaryTableKey(tableName, schemaName),
            out tb);

    internal bool TryGetGlobalTemporaryTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
        => Db.TryGetGlobalTemporaryTable(tableName, out tb, schemaName);
    
    /// <summary>
    /// EN: Creates and adds a table to the database.
    /// PT: Cria e adiciona uma tabela ao banco.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Created table. PT: Tabela criada.</returns>
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
    /// EN: Gets a table by name, throwing if it does not exist.
    /// PT: Obtém uma tabela pelo nome, lançando erro se não existir.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Found table. PT: Tabela encontrada.</returns>
    public ITableMock GetTable(
        string tableName,
        string? schemaName = null)
    {
        if (TryGetTemporaryTable(tableName, out var tb, schemaName)
            && tb != null)
        {
            return tb;
        }

        if (TryGetGlobalTemporaryTable(tableName, out tb, schemaName)
            && tb != null)
        {
            return tb;
        }

        return Db.GetTable(
            tableName,
            schemaName ?? Database);
    }

    /// <summary>
    /// EN: Tries to get a table by name.
    /// PT: Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="tb">EN: Found table, if any. PT: Tabela encontrada, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool TryGetTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    {
        if (TryGetTemporaryTable(tableName, out tb, schemaName)
            && tb != null)
        {
            return true;
        }

        if (TryGetGlobalTemporaryTable(tableName, out tb, schemaName)
            && tb != null)
        {
            return true;
        }

        return Db.TryGetTable(
            tableName,
            out tb,
            schemaName ?? Database);
    }

    /// <summary>
    /// EN: Lists the tables in the current schema.
    /// PT: Lista as tabelas do schema atual.
    /// </summary>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Table list. PT: Lista de tabelas.</returns>
    public IReadOnlyList<ITableMock> ListTables(
        string? schemaName = null)
    {
        var tables = Db.ListTables(schemaName ?? Database).ToList();
        tables.AddRange(Db.ListGlobalTemporaryTables(schemaName ?? Database));
        tables.AddRange(ListTemporaryTables(schemaName));
        return tables.AsReadOnly();
    }

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
    /// EN: Registers a stored procedure.
    /// PT: Registra um procedimento armazenado.
    /// </summary>
    /// <param name="procName">EN: Procedure name. PT: Nome do procedimento.</param>
    /// <param name="pr">EN: Procedure definition. PT: Definição do procedimento.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddProdecure(
        string procName,
        ProcedureDef pr,
        string? schemaName = null)
        => Db.AddProdecure(
            procName,
            pr, 
            schemaName ?? Database);

    /// <summary>
    /// EN: Tries to get a stored procedure.
    /// PT: Tenta obter um procedimento armazenado.
    /// </summary>
    /// <param name="procName">EN: Procedure name. PT: Nome do procedimento.</param>
    /// <param name="pr">EN: Found procedure, if any. PT: Procedimento encontrado, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool TryGetProcedure(
        string procName,
        out ProcedureDef? pr,
        string? schemaName = null)
        => Db.TryGetProcedure(
            procName,
            out pr,
            schemaName ?? Database);

    #endregion

    /// <summary>
    /// EN: Begins a database transaction with the specified isolation level.
    /// PT: Inicia uma transação com o nível de isolamento especificado.
    /// </summary>
    /// <param name="isolationLevel">EN: Isolation level. PT: Nível de isolamento.</param>
    /// <returns>EN: Created transaction. PT: Transação criada.</returns>
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

    /// <summary>
    /// EN: Creates a provider-specific transaction instance.
    /// PT: Cria uma instância de transação específica do provedor.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected abstract DbTransaction CreateTransaction();

    /// <summary>
    /// EN: Changes the current database/schema of the connection.
    /// PT: Altera o database/schema atual da conexão.
    /// </summary>
    /// <param name="databaseName">EN: Database name. PT: Nome do database.</param>
    public override void ChangeDatabase(string databaseName)
        => _database = databaseName;

    /// <summary>
    /// EN: Closes the simulated connection.
    /// PT: Fecha a conexão simulada.
    /// </summary>
    public override void Close()
        => _state = ConnectionState.Closed;

    /// <summary>
    /// EN: Creates a command associated with the current transaction.
    /// PT: Cria um comando associado à transação atual.
    /// </summary>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommand()
        => CreateDbCommandCore(CurrentTransaction);

    /// <summary>
    /// EN: Creates a provider-specific command tied to a transaction.
    /// PT: Cria um comando específico do provedor atrelado a uma transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected abstract DbCommand CreateDbCommandCore(
        DbTransaction? transaction);

    /// <summary>
    /// EN: Opens the simulated connection.
    /// PT: Abre a conexão simulada.
    /// </summary>
    public override void Open()
    {
        _state = ConnectionState.Open;
        Debug.WriteLine("Opended");
    }

    /// <summary>
    /// EN: Commits the current transaction, releasing backups.
    /// PT: Confirma a transação atual, liberando backups.
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
    /// EN: Rolls back the current transaction restoring the backup.
    /// PT: Desfaz a transação atual restaurando o backup.
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

    /// <summary>
    /// EN: Disposes the connection and associated resources.
    /// PT: Descarta a conexão e os recursos associados.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
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
            _temporaryTables.Clear();
        }

        _disposed = true;
        base.Dispose(disposing);
    }
}
