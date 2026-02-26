namespace DbSqlLikeMem.Oracle;

#if NET6_0_OR_GREATER
/// <summary>
/// EN: Represents an Oracle batch mock that executes commands against the in-memory database.
/// PT: Representa um simulado de lote Oracle que executa comandos no banco em memória.
/// </summary>
public sealed class OracleBatchMock : DbBatch
{
    private OracleConnectionMock? connection;
    private OracleTransactionMock? transaction;

    /// <summary>
    /// EN: Initializes an Oracle batch mock with an empty command collection.
    /// PT: Inicializa um simulado de lote Oracle com uma coleção de comandos vazia.
    /// </summary>
    public OracleBatchMock() => BatchCommands = new OracleBatchCommandCollectionMock();

    /// <summary>
    /// EN: Initializes an Oracle batch mock bound to a connection and an optional transaction.
    /// PT: Inicializa um simulado de lote Oracle vinculado a uma conexão e a uma transação opcional.
    /// </summary>
    public OracleBatchMock(OracleConnectionMock connection, OracleTransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Gets or sets the connection used to execute batch commands.
    /// PT: Obtém ou define a conexão usada para executar comandos em lote.
    /// </summary>
    public new OracleConnectionMock? Connection
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
        set => connection = (OracleConnectionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the transaction associated with batch execution.
    /// PT: Obtém ou define a transação associada à execução em lote.
    /// </summary>
    public new OracleTransactionMock? Transaction
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
        set => transaction = (OracleTransactionMock?)value;
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
    public new OracleBatchCommandCollectionMock BatchCommands { get; }

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
    /// EN: Executes all batch commands and returns the total number of affected rows.
    /// PT: Executa todos os comandos do lote e retorna o total de linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var affected = 0;
        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new OracleCommandMock(Connection, Transaction)
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
    /// EN: Executes batch commands and returns a data reader over produced result sets.
    /// PT: Executa os comandos em lote e retorna um leitor de dados com os resultados produzidos.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var tables = new List<TableResultMock>();

        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new OracleCommandMock(Connection, Transaction)
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

        return new OracleDataReaderMock(tables);
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
    /// EN: Executes the first batch command and returns its scalar result.
    /// PT: Executa o primeiro comando do lote e retorna seu resultado escalar.
    /// </summary>
    public override object? ExecuteScalar()
    {
        if (BatchCommands.Count == 0)
            return null;

        var first = BatchCommands.Commands[0];
        using var command = new OracleCommandMock(Connection, Transaction)
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
    /// EN: Asynchronously executes all batch commands and returns affected rows.
    /// PT: Executa todos os comandos em lote de forma assíncrona e retorna as linhas afetadas.
    /// </summary>
    public override System.Threading.Tasks.Task<int> ExecuteNonQueryAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteNonQuery());

    /// <summary>
    /// EN: Asynchronously executes batch commands and returns a data reader.
    /// PT: Executa os comandos em lote de forma assíncrona e retorna um leitor de dados.
    /// </summary>
    protected override System.Threading.Tasks.Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult<DbDataReader>(ExecuteDbDataReader(behavior));

    /// <summary>
    /// EN: Asynchronously executes the first batch command and returns its scalar result.
    /// PT: Executa o primeiro comando do lote de forma assíncrona e retorna seu resultado escalar.
    /// </summary>
    public override System.Threading.Tasks.Task<object?> ExecuteScalarAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteScalar());

    /// <summary>
    /// EN: Completes immediately because this mock does not require server-side preparation.
    /// PT: Conclui imediatamente porque este simulado não requer preparação no servidor.
    /// </summary>
    public override System.Threading.Tasks.Task PrepareAsync(System.Threading.CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// EN: Performs no action because prepared statements are not simulated by this mock.
    /// PT: Não realiza ação porque instruções preparadas não são simuladas por este simulado.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Creates a new Oracle batch command mock instance.
    /// PT: Cria uma nova instância de simulado de comando em lote Oracle.
    /// </summary>
    protected override DbBatchCommand CreateDbBatchCommand() => new OracleBatchCommandMock();
}

/// <summary>
/// EN: Represents a single command entry executed by OracleBatchMock.
/// PT: Representa uma entrada de comando executada por OracleBatchMock.
/// </summary>
public sealed class OracleBatchCommandMock : DbBatchCommand, IOracleCommandMock
{
    private readonly OracleCommandMock command = new();

    /// <summary>
    /// EN: Gets or sets the SQL text executed by this batch command.
    /// PT: Obtém ou define o texto SQL executado por este comando em lote.
    /// </summary>
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// EN: Gets or sets how the command text is interpreted.
    /// PT: Obtém ou define como o texto do comando é interpretado.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Stores the affected row count reported by the command execution.
    /// PT: Armazena a contagem de linhas afetadas reportada pela execução do comando.
    /// </summary>
    private int recordsAffected = 0;

    /// <summary>
    /// EN: Gets the number of rows affected by this batch command.
    /// PT: Obtém o número de linhas afetadas por este comando em lote.
    /// </summary>
    public override int RecordsAffected => recordsAffected;

    /// <summary>
    /// EN: Gets the underlying parameter collection used by this batch command.
    /// PT: Obtém a coleção de parâmetros subjacente usada por este comando em lote.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Represents the collection that stores commands executed by OracleBatchMock.
/// PT: Representa a coleção que armazena os comandos executados por OracleBatchMock.
/// </summary>
public sealed class OracleBatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<OracleBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Gets the number of commands currently stored in the batch collection.
    /// PT: Obtém o número de comandos atualmente armazenados na coleção de lote.
    /// </summary>
    public override int Count => Commands.Count;

    /// <summary>
    /// EN: Gets whether the batch command collection is read-only is supported.
    /// PT: Obtém se a coleção de comandos de lote é somente leitura.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// EN: Adds a command to the batch collection.
    /// PT: Adiciona um comando à coleção de lote.
    /// </summary>
    public override void Add(DbBatchCommand item)
    {
        if (item is OracleBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Removes all commands from the batch collection.
    /// PT: Remove todos os comandos da coleção de lote.
    /// </summary>
    public override void Clear() => Commands.Clear();

    /// <summary>
    /// EN: Determines whether a command exists in the batch collection.
    /// PT: Determina se um comando existe na coleção de lote.
    /// </summary>
    public override bool Contains(DbBatchCommand item) => Commands.Contains((OracleBatchCommandMock)item);

    /// <summary>
    /// EN: Copies commands to an array starting at the specified index.
    /// PT: Copia os comandos para um array a partir do índice especificado.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Returns an enumerator for iterating over batch commands.
    /// PT: Retorna um enumerador para iterar sobre os comandos em lote.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();

    /// <summary>
    /// EN: Returns the zero-based index of a command in the batch collection.
    /// PT: Retorna o índice baseado em zero de um comando na coleção de lote.
    /// </summary>
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((OracleBatchCommandMock)item);

    /// <summary>
    /// EN: Inserts a command at the specified index in the collection.
    /// PT: Insere um comando no índice especificado da coleção.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (OracleBatchCommandMock)item);

    /// <summary>
    /// EN: Removes the specified command from the batch collection.
    /// PT: Remove o comando especificado da coleção de lote.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((OracleBatchCommandMock)item);

    /// <summary>
    /// EN: Removes the command at the specified index.
    /// PT: Remove o comando no índice especificado.
    /// </summary>
    public override void RemoveAt(int index) => Commands.RemoveAt(index);

    /// <summary>
    /// EN: Returns the batch command stored at the specified index.
    /// PT: Retorna o comando em lote armazenado no índice especificado.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];

    /// <summary>
    /// EN: Replaces the batch command stored at the specified index.
    /// PT: Substitui o comando em lote armazenado no índice especificado.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (OracleBatchCommandMock)batchCommand;
}
#endif
