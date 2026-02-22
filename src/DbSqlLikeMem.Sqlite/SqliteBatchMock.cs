using DbConnection = System.Data.Common.DbConnection;
using DbTransaction = System.Data.Common.DbTransaction;
using DbDataReader = System.Data.Common.DbDataReader;
using DbParameterCollection = System.Data.Common.DbParameterCollection;
using DbParameter = System.Data.Common.DbParameter;

#if NET6_0_OR_GREATER
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
using DbBatchCommandCollection = System.Data.Common.DbBatchCommandCollection;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents the Sqlite Batch Mock type used by provider mocks.
/// PT: Representa o tipo Sqlite lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqliteBatchMock : DbBatch
{
    private SqliteConnectionMock? connection;
    private SqliteTransactionMock? transaction;

    /// <summary>
    /// EN: Represents a provider-specific batch mock that executes commands against the in-memory database.
    /// PT: Representa um simulado de lote específico do provedor que executa comandos no banco em memória.
    /// </summary>
    public SqliteBatchMock() => BatchCommands = new SqliteBatchCommandCollectionMock();

    /// <summary>
    /// EN: Represents a provider-specific batch mock that executes commands against the in-memory database.
    /// PT: Representa um simulado de lote específico do provedor que executa comandos no banco em memória.
    /// </summary>
    public SqliteBatchMock(SqliteConnectionMock connection, SqliteTransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Gets or sets the connection used to execute batch commands.
    /// PT: Obtém ou define a conexão usada para executar comandos em lote.
    /// </summary>
    public new SqliteConnectionMock? Connection
    {
        get => connection;
        set => connection = value;
    }

    /// <summary>
    /// EN: Gets or sets the connection used to execute batch commands.
    /// PT: Obtém ou define a conexão usada para executar comandos em lote.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (SqliteConnectionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the transaction associated with batch execution.
    /// PT: Obtém ou define a transação associada à execução em lote.
    /// </summary>
    public new SqliteTransactionMock? Transaction
    {
        get => transaction;
        set => transaction = value;
    }

    /// <summary>
    /// EN: Gets or sets the transaction associated with batch execution.
    /// PT: Obtém ou define a transação associada à execução em lote.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set => transaction = (SqliteTransactionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the command timeout, in seconds, applied to each batch command.
    /// PT: Obtém ou define o tempo limite do comando, em segundos, aplicado a cada comando do lote.
    /// </summary>
    public override int Timeout { get; set; }

    /// <summary>
    /// EN: Gets the batch command collection executed by this batch.
    /// PT: Obtém a coleção de comandos de lote executada por este lote.
    /// </summary>
    public new SqliteBatchCommandCollectionMock BatchCommands { get; }

    /// <summary>
    /// EN: Gets the batch command collection executed by this batch.
    /// PT: Obtém a coleção de comandos de lote executada por este lote.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <summary>
    /// EN: Cancels batch execution by rolling back the active transaction.
    /// PT: Cancela a execução do lote revertendo a transação ativa.
    /// </summary>
    public override void Cancel() => Transaction?.Rollback();

    /// <summary>
    /// EN: Execute Non Query for the current batch state.
    /// PT: Execute Non consulta para o estado atual do lote.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var affected = 0;
        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new SqliteCommandMock(Connection, Transaction)
            {
                CommandText = batchCommand.CommandText,
                CommandType = batchCommand.CommandType,
                CommandTimeout = Timeout
            };

            foreach (DbParameter parameter in batchCommand.Parameters)
                command.Parameters.Add(parameter);

            affected += command.ExecuteNonQuery();
        }

        return affected;
    }

    /// <summary>
    /// EN: Execute Db Data Reader for the current batch state.
    /// PT: Execute Db Data leitor para o estado atual do lote.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var tables = new List<TableResultMock>();

        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new SqliteCommandMock(Connection, Transaction)
            {
                CommandText = batchCommand.CommandText,
                CommandType = batchCommand.CommandType,
                CommandTimeout = Timeout
            };

            foreach (DbParameter parameter in batchCommand.Parameters)
                command.Parameters.Add(parameter);

            try
            {
                using var reader = command.ExecuteReader(behavior);
                do
                {
                    var rows = new List<object[]>();
                    while (reader.Read())
                    {
                        var row = new object[reader.FieldCount];
                        reader.GetValues(row);
                        rows.Add(row);
                    }

                    if (reader.FieldCount > 0)
                        tables.Add(CreateTableResult(rows, reader));
                } while (reader.NextResult());
            }
            catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
            {
                command.ExecuteNonQuery();
            }
        }

        return new SqliteDataReaderMock(tables);
    }

    private static TableResultMock CreateTableResult(IReadOnlyCollection<object[]> rows, IDataRecord schemaRecord)
    {
        var table = new TableResultMock();

        for (var col = 0; col < schemaRecord.FieldCount; col++)
        {
            table.Columns.Add(new TableResultColMock(
                tableAlias: string.Empty,
                columnAlias: schemaRecord.GetName(col),
                columnName: schemaRecord.GetName(col),
                columIndex: col,
                dbType: schemaRecord.GetFieldType(col).ConvertTypeToDbType(),
                isNullable: true));
        }

        foreach (var row in rows)
        {
            var rowData = new Dictionary<int, object?>();
            for (var col = 0; col < row.Length; col++)
                rowData[col] = row[col] == DBNull.Value ? null : row[col];
            table.Add(rowData);
        }

        return table;
    }

    /// <summary>
    /// EN: Execute Scalar for the current batch state.
    /// PT: Execute Scalar para o estado atual do lote.
    /// </summary>
    public override object? ExecuteScalar()
    {
        if (BatchCommands.Count == 0)
            return null;

        var first = BatchCommands.Commands[0];
        using var command = new SqliteCommandMock(Connection, Transaction)
        {
            CommandText = first.CommandText,
            CommandType = first.CommandType,
            CommandTimeout = Timeout
        };

        foreach (DbParameter parameter in first.Parameters)
            command.Parameters.Add(parameter);

        return command.ExecuteScalar();
    }

    /// <summary>
    /// EN: Execute Non Query Async for the current batch state.
    /// PT: Execute Non consulta Async para o estado atual do lote.
    /// </summary>
    public override System.Threading.Tasks.Task<int> ExecuteNonQueryAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteNonQuery());

    /// <summary>
    /// EN: Execute Db Data Reader Async for the current batch state.
    /// PT: Execute Db Data leitor Async para o estado atual do lote.
    /// </summary>
    protected override System.Threading.Tasks.Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult<DbDataReader>(ExecuteDbDataReader(behavior));

    /// <summary>
    /// EN: Execute Scalar Async for the current batch state.
    /// PT: Execute Scalar Async para o estado atual do lote.
    /// </summary>
    public override System.Threading.Tasks.Task<object?> ExecuteScalarAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteScalar());

    /// <summary>
    /// EN: Executes prepare async.
    /// PT: Executa prepare async.
    /// </summary>
    public override System.Threading.Tasks.Task PrepareAsync(System.Threading.CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// EN: Executes prepare.
    /// PT: Executa prepare.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Creates a new db batch command instance.
    /// PT: Cria uma nova instância de comando de lote do banco.
    /// </summary>
    protected override DbBatchCommand CreateDbBatchCommand() => new SqliteBatchCommandMock();
}

/// <summary>
/// EN: Represents the Sqlite Batch Command Mock type used by provider mocks.
/// PT: Representa o tipo Sqlite comando em lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqliteBatchCommandMock : DbBatchCommand, ISqliteCommandMock
{
    private readonly SqliteCommandMock command = new();

    /// <summary>
    /// EN: Executes command text.
    /// PT: Executa comando text.
    /// </summary>
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// EN: Executes command type.
    /// PT: Executa comando type.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Executes 0.
    /// PT: Executa 0.
    /// </summary>
    private int recordsAffected = 0;

    /// <summary>
    /// EN: Gets records affected.
    /// PT: Obtém records affected.
    /// </summary>
    public override int RecordsAffected => recordsAffected;

    /// <summary>
    /// EN: Gets db parameter collection.
    /// PT: Obtém parâmetro de banco collection.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Represents the Sqlite Batch Command Collection Mock type used by provider mocks.
/// PT: Representa o tipo Sqlite coleção de comandos de lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqliteBatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<SqliteBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Gets count.
    /// PT: Obtém count.
    /// </summary>
    public override int Count => Commands.Count;

    /// <summary>
    /// EN: Gets is read only.
    /// PT: Obtém is read only.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// EN: Add operation for batch commands.
    /// PT: Operação de add para comandos em lote.
    /// </summary>
    public override void Add(DbBatchCommand item)
    {
        if (item is SqliteBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Clear operation for batch commands.
    /// PT: Operação de clear para comandos em lote.
    /// </summary>
    public override void Clear() => Commands.Clear();

    /// <summary>
    /// EN: Contains operation for batch commands.
    /// PT: Operação de contains para comandos em lote.
    /// </summary>
    public override bool Contains(DbBatchCommand item) => Commands.Contains((SqliteBatchCommandMock)item);

    /// <summary>
    /// EN: Copy To operation for batch commands.
    /// PT: Operação de copy to para comandos em lote.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Returns enumerator.
    /// PT: Retorna enumerador.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();

    /// <summary>
    /// EN: Index Of operation for batch commands.
    /// PT: Operação de index of para comandos em lote.
    /// </summary>
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((SqliteBatchCommandMock)item);

    /// <summary>
    /// EN: Insert operation for batch commands.
    /// PT: Operação de insert para comandos em lote.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (SqliteBatchCommandMock)item);

    /// <summary>
    /// EN: Remove operation for batch commands.
    /// PT: Operação de remove para comandos em lote.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((SqliteBatchCommandMock)item);

    /// <summary>
    /// EN: Remove At operation for batch commands.
    /// PT: Operação de remove at para comandos em lote.
    /// </summary>
    public override void RemoveAt(int index) => Commands.RemoveAt(index);

    /// <summary>
    /// EN: Returns batch command.
    /// PT: Retorna comando em lote.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];

    /// <summary>
    /// EN: Updates batch command.
    /// PT: Atualiza comando em lote.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (SqliteBatchCommandMock)batchCommand;
}
#endif
