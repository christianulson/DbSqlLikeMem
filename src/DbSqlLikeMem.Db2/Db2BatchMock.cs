namespace DbSqlLikeMem.Db2;

#if NET6_0_OR_GREATER
/// <summary>
/// EN: Represents the Db2 Batch Mock type used by provider mocks.
/// PT: Representa o tipo Db2 lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2BatchMock : DbBatch
{
    private Db2ConnectionMock? connection;
    private Db2TransactionMock? transaction;

    /// <summary>
    /// EN: Represents a provider-specific batch mock that executes commands against the in-memory database.
    /// PT: Representa um simulado de lote específico do provedor que executa comandos no banco em memória.
    /// </summary>
    public Db2BatchMock() => BatchCommands = [];

    /// <summary>
    /// EN: Represents a provider-specific batch mock that executes commands against the in-memory database.
    /// PT: Representa um simulado de lote específico do provedor que executa comandos no banco em memória.
    /// </summary>
    public Db2BatchMock(Db2ConnectionMock connection, Db2TransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Gets or sets the connection used to execute batch commands.
    /// PT: Obtém ou define a conexão usada para executar comandos em lote.
    /// </summary>
    public new Db2ConnectionMock? Connection
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
        set => connection = (Db2ConnectionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the transaction associated with batch execution.
    /// PT: Obtém ou define a transação associada à execução em lote.
    /// </summary>
    public new Db2TransactionMock? Transaction
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
        set => transaction = (Db2TransactionMock?)value;
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
    public new Db2BatchCommandCollectionMock BatchCommands { get; }

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
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteNonQueryCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand);
    }

    /// <summary>
    /// EN: Execute Db Data Reader for the current batch state.
    /// PT: Execute Db Data leitor para o estado atual do lote.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteReaderCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand,
            behavior,
            static tables => new Db2DataReaderMock(tables));
    }

    /// <summary>
    /// EN: Execute Scalar for the current batch state.
    /// PT: Execute Scalar para o estado atual do lote.
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
    /// EN: Execute Non Query Async for the current batch state.
    /// PT: Execute Non consulta Async para o estado atual do lote.
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
    /// EN: Execute Db Data Reader Async for the current batch state.
    /// PT: Execute Db Data leitor Async para o estado atual do lote.
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
                static tables => (DbDataReader)new Db2DataReaderMock(tables),
                cancellationToken)
;
    }

    /// <summary>
    /// EN: Execute Scalar Async for the current batch state.
    /// PT: Execute Scalar Async para o estado atual do lote.
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

    private Db2CommandMock CreateExecutableCommand(Db2BatchCommandMock batchCommand)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchCommandFactory.Create(
            connection,
            () => new Db2CommandMock(connection, Transaction),
            batchCommand,
            Timeout);
    }

    /// <summary>
    /// EN: Executes prepare async.
    /// PT: Executa prepare async.
    /// </summary>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
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
    protected override DbBatchCommand CreateDbBatchCommand() => new Db2BatchCommandMock();
}

/// <summary>
/// EN: Represents the Db2 Batch Command Mock type used by provider mocks.
/// PT: Representa o tipo Db2 comando em lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2BatchCommandMock : DbBatchCommand, IDb2CommandMock
{
    private readonly Db2CommandMock command = new();

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
/// EN: Represents the Db2 Batch Command Collection Mock type used by provider mocks.
/// PT: Representa o tipo Db2 coleção de comandos de lote simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2BatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<Db2BatchCommandMock> Commands { get; } = [];

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
        if (item is Db2BatchCommandMock b)
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
    public override bool Contains(DbBatchCommand item) => Commands.Contains((Db2BatchCommandMock)item);

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
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Insert operation for batch commands.
    /// PT: Operação de insert para comandos em lote.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Remove operation for batch commands.
    /// PT: Operação de remove para comandos em lote.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((Db2BatchCommandMock)item);

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
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (Db2BatchCommandMock)batchCommand;
}
#endif
