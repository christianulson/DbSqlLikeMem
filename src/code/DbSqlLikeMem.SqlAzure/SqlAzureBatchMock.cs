#if NET6_0_OR_GREATER
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
using DbBatchCommandCollection = System.Data.Common.DbBatchCommandCollection;
using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents the Sql Azure Batch Mock type used by provider mocks.
/// PT: Representa o tipo Sql Azure lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureBatchMock : DbBatch
{
    private SqlAzureConnectionMock? connection;
    private SqlServerTransactionMock? transaction;

    /// <summary>
    /// EN: Creates an empty SQL Azure batch mock with an empty command collection.
    /// PT: Cria um lote simulado do SQL Azure vazio com colecao de comandos vazia.
    /// </summary>
    public SqlAzureBatchMock() => BatchCommands = [];

    /// <summary>
    /// EN: Creates a SQL Azure batch mock bound to a connection and optional transaction.
    /// PT: Cria um lote simulado do SQL Azure associado a uma conexao e transacao opcional.
    /// </summary>
    public SqlAzureBatchMock(SqlAzureConnectionMock connection, SqlServerTransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure connection used by this batch.
    /// PT: Obtem ou define a conexao tipada do SQL Azure usada por este lote.
    /// </summary>
    public new SqlAzureConnectionMock? Connection
    {
        get => connection;
        set => connection = value;
    }

    /// <summary>
    /// EN: Gets or sets the base database connection used by this batch.
    /// PT: Obtem ou define a conexao de banco base usada por este lote.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (SqlAzureConnectionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the typed transaction used by this batch.
    /// PT: Obtem ou define a transacao tipada usada por este lote.
    /// </summary>
    public new SqlServerTransactionMock? Transaction
    {
        get => transaction;
        set => transaction = value;
    }

    /// <summary>
    /// EN: Gets or sets the base transaction used by this batch.
    /// PT: Obtem ou define a transacao base usada por este lote.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set => transaction = (SqlServerTransactionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the command timeout applied to batch commands.
    /// PT: Obtem ou define o timeout de comando aplicado aos comandos do lote.
    /// </summary>
    public override int Timeout { get; set; }

    /// <summary>
    /// EN: Gets the typed command collection held by this batch.
    /// PT: Obtem a colecao tipada de comandos mantida por este lote.
    /// </summary>
    public new SqlAzureBatchCommandCollectionMock BatchCommands { get; }

    /// <summary>
    /// EN: Gets the base batch command collection for framework consumption.
    /// PT: Obtem a colecao base de comandos em lote para consumo do framework.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <summary>
    /// EN: Cancels the current batch by rolling back the active transaction when available.
    /// PT: Cancela o lote atual fazendo rollback da transacao ativa quando disponivel.
    /// </summary>
    public override void Cancel() => Transaction?.Rollback();

    /// <summary>
    /// EN: Executes all batch commands as non-query operations and returns total affected rows.
    /// PT: Executa todos os comandos do lote como operacoes sem consulta e retorna o total de linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteNonQueryCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand);
    }

    /// <summary>
    /// EN: Executes all batch commands and returns a combined data reader for result sets.
    /// PT: Executa todos os comandos do lote e retorna um data reader combinado para os conjuntos de resultados.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteReaderCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand,
            behavior,
            static tables => new SqlAzureDataReaderMock(tables));
    }

    /// <summary>
    /// EN: Executes the first batch command and returns its scalar result.
    /// PT: Executa o primeiro comando do lote e retorna seu resultado escalar.
    /// </summary>
    public override object? ExecuteScalar()
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchScalarExecutionRunner.ExecuteFirstScalar(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand);
    }

    /// <summary>
    /// EN: Asynchronously executes all batch commands as non-query operations.
    /// PT: Executa assincronamente todos os comandos do lote como operacoes sem consulta.
    /// </summary>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchAsyncExecutionRunner
            .ExecuteNonQueryCommandsAsync(
                connection,
                BatchCommands.Commands,
                CreateExecutableCommand,
                cancellationToken)
;
    }

    /// <summary>
    /// EN: Asynchronously executes all batch commands and returns a combined data reader.
    /// PT: Executa assincronamente todos os comandos do lote e retorna um data reader combinado.
    /// </summary>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchAsyncExecutionRunner
            .ExecuteReaderCommandsAsync(
                connection,
                BatchCommands.Commands,
                CreateExecutableCommand,
                behavior,
                static tables => (DbDataReader)new SqlAzureDataReaderMock(tables),
                cancellationToken)
;
    }

    /// <summary>
    /// EN: Asynchronously executes the first batch command and returns its scalar result.
    /// PT: Executa assincronamente o primeiro comando do lote e retorna seu resultado escalar.
    /// </summary>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchScalarExecutionRunner.ExecuteFirstScalarAsync(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand,
            cancellationToken);
    }

    private SqlAzureCommandMock CreateExecutableCommand(SqlAzureBatchCommandMock batchCommand)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchCommandFactory.Create(
            connection,
            () => new SqlAzureCommandMock(connection, Transaction),
            batchCommand,
            Timeout);
    }

    /// <summary>
    /// EN: Asynchronously prepares the batch for execution.
    /// PT: Prepara assincronamente o lote para execucao.
    /// </summary>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// EN: Prepares the batch for execution.
    /// PT: Prepara o lote para execucao.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Creates a new typed SQL Azure batch command instance.
    /// PT: Cria uma nova instancia tipada de comando em lote do SQL Azure.
    /// </summary>
    protected override DbBatchCommand CreateDbBatchCommand() => new SqlAzureBatchCommandMock();
}

/// <summary>
/// EN: Represents the Sql Azure Batch Command Mock type used by provider mocks.
/// PT: Representa o tipo Sql Azure comando em lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureBatchCommandMock : DbBatchCommand
{
    private readonly SqlAzureCommandMock command = new();
    private int recordsAffected = 0;

    /// <summary>
    /// EN: Gets or sets the SQL text executed by this batch command.
    /// PT: Obtem ou define o texto SQL executado por este comando em lote.
    /// </summary>
    public override string CommandText { get; set; } = string.Empty;
    /// <summary>
    /// EN: Gets or sets the command type used by this batch command.
    /// PT: Obtem ou define o tipo de comando usado por este comando em lote.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;
    /// <summary>
    /// EN: Gets the number of records affected by the command execution.
    /// PT: Obtem o numero de registros afetados pela execucao do comando.
    /// </summary>
    public override int RecordsAffected => recordsAffected;
    /// <summary>
    /// EN: Gets the parameter collection associated with this batch command.
    /// PT: Obtem a colecao de parametros associada a este comando em lote.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Represents the Sql Azure Batch Command Collection Mock type used by provider mocks.
/// PT: Representa o tipo Sql Azure coleção de comandos de lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureBatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<SqlAzureBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Gets the number of commands currently stored in the collection.
    /// PT: Obtem o numero de comandos atualmente armazenados na colecao.
    /// </summary>
    public override int Count => Commands.Count;
    /// <summary>
    /// EN: Gets whether the collection is read-only.
    /// PT: Obtem se a colecao e somente leitura.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// EN: Adds a batch command to the collection when it is SQL Azure compatible.
    /// PT: Adiciona um comando em lote a colecao quando ele e compativel com SQL Azure.
    /// </summary>
    public override void Add(DbBatchCommand item)
    {
        if (item is SqlAzureBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Removes all commands from the collection.
    /// PT: Remove todos os comandos da colecao.
    /// </summary>
    public override void Clear() => Commands.Clear();
    /// <summary>
    /// EN: Checks whether the specified command exists in the collection.
    /// PT: Verifica se o comando especificado existe na colecao.
    /// </summary>
    public override bool Contains(DbBatchCommand item) => Commands.Contains((SqlAzureBatchCommandMock)item);

    /// <summary>
    /// EN: Copies collection commands to an array starting at the target index.
    /// PT: Copia os comandos da colecao para um array a partir do indice de destino.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Returns an enumerator for iterating over the commands in the collection.
    /// PT: Retorna um enumerador para iterar pelos comandos da colecao.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();
    /// <summary>
    /// EN: Returns the index of the specified command in the collection.
    /// PT: Retorna o indice do comando especificado na colecao.
    /// </summary>
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Inserts a command into the collection at the specified index.
    /// PT: Insere um comando na colecao no indice especificado.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Removes the specified command from the collection.
    /// PT: Remove o comando especificado da colecao.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Removes the command at the specified index from the collection.
    /// PT: Remove da colecao o comando no indice especificado.
    /// </summary>
    public override void RemoveAt(int index) => Commands.RemoveAt(index);
    /// <summary>
    /// EN: Gets the command located at the specified index.
    /// PT: Obtem o comando localizado no indice especificado.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];
    /// <summary>
    /// EN: Replaces the command at the specified index with a new command instance.
    /// PT: Substitui o comando no indice especificado por uma nova instancia de comando.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (SqlAzureBatchCommandMock)batchCommand;
}
#endif
