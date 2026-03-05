namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Represents a MySQL batch mock that executes commands against the in-memory database.
/// PT: Representa um simulado de lote MySQL que executa comandos no banco em memória.
/// </summary>
public sealed class MySqlBatchMock :
#if NET6_0_OR_GREATER
    DbBatch,
#endif
    IDisposable
{
    /// <summary>
    /// EN: Represents a MySQL batch mock that executes commands against the in-memory database.
    /// PT: Representa um simulado de lote MySQL que executa comandos no banco em memória.
    /// </summary>
    public MySqlBatchMock()
        : this(null, null)
    {
    }

    /// <summary>
    /// EN: Initializes a batch bound to a connection and an optional transaction.
    /// PT: Inicializa um lote vinculado a uma conexão e a uma transação opcional.
    /// </summary>
    public MySqlBatchMock(
        MySqlConnectionMock? connection,
        MySqlTransactionMock? transaction = null)
    {
        Connection = connection;
        Transaction = transaction;
        BatchCommands = new MySqlBatchCommandCollectionMock();
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets or sets the connection used by this batch execution.
    /// PT: Obtém ou define a conexão usada por esta execução em lote.
    /// </summary>
    public new MySqlConnectionMock? Connection { get; set; }
    /// <summary>
    /// EN: Gets or sets the base connection reference for this batch.
    /// PT: Obtém ou define a referência de conexão base para este lote.
    /// </summary>
    protected override DbConnection? DbConnection { get => Connection; set => Connection = (MySqlConnectionMock?)value; }
    /// <summary>
    /// EN: Gets or sets the transaction associated with this batch.
    /// PT: Obtém ou define a transação associada a este lote.
    /// </summary>
    public new MySqlTransactionMock? Transaction { get; set; }
    /// <summary>
    /// EN: Gets or sets the base transaction reference for this batch.
    /// PT: Obtém ou define a referência de transação base para este lote.
    /// </summary>
    protected override DbTransaction? DbTransaction { get => Transaction; set => Transaction = (MySqlTransactionMock?)value; }
#else
    /// <summary>
    /// EN: Gets or sets the connection used by this batch execution.
    /// PT: Obtém ou define a conexão usada por esta execução em lote.
    /// </summary>
    public MySqlConnectionMock? Connection { get; set; }
    /// <summary>
    /// EN: Gets or sets the transaction associated with this batch.
    /// PT: Obtém ou define a transação associada a este lote.
    /// </summary>
    public MySqlTransactionMock? Transaction { get; set; }
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets the typed collection of commands executed by this batch.
    /// PT: Obtém a coleção tipada de comandos executados por este lote.
    /// </summary>
    public new MySqlBatchCommandCollectionMock BatchCommands { get; }
    /// <summary>
    /// EN: Gets the base batch command collection view.
    /// PT: Obtém a visão base da coleção de comandos de lote.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;
#else
    /// <summary>
    /// EN: Gets the typed collection of commands executed by this batch.
    /// PT: Obtém a coleção tipada de comandos executados por este lote.
    /// </summary>
    public MySqlBatchCommandCollectionMock BatchCommands { get; }
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Executes the batch and returns a typed MySqlDataReaderMock.
    /// PT: Executa o lote e retorna um MySqlDataReaderMock tipado.
    /// </summary>
    public new MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#else
    /// <summary>
    /// EN: Executes the batch and returns a typed MySqlDataReaderMock.
    /// PT: Executa o lote e retorna um MySqlDataReaderMock tipado.
    /// </summary>
    public MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#endif
        (MySqlDataReaderMock) ExecuteDbDataReader(commandBehavior);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Asynchronously executes the batch and returns a typed MySqlDataReaderMock.
    /// PT: Executa o lote de forma assíncrona e retorna um MySqlDataReaderMock tipado.
    /// </summary>
    public new async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// EN: Asynchronously executes the batch and returns a typed MySqlDataReaderMock.
    /// PT: Executa o lote de forma assíncrona e retorna um MySqlDataReaderMock tipado.
    /// </summary>
    public async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#endif
        (MySqlDataReaderMock)await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

    //// TODO: new ExecuteReaderAsync(CommandBehavior)

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Executes all commands and returns a reader over their result sets.
    /// PT: Executa todos os comandos e retorna um leitor sobre seus conjuntos de resultados.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
#else
    [SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Matches .NET 6.0 override")]
    private DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
#endif
    {
        //this.ResetCommandTimeout();
#pragma warning disable CA2012 // OK to read .Result because the ValueTask is completed
        return ExecuteReaderCoreAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
#pragma warning restore CA2012
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Asynchronously executes all commands and returns a data reader.
    /// PT: Executa todos os comandos de forma assíncrona e retorna um leitor de dados.
    /// </summary>
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
#else
    private async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
#endif
    {
        //this.ResetCommandTimeout();
        //using var registration = ((ICancellableCommand)this).RegisterCancel(cancellationToken);
        return await ExecuteReaderCoreAsync(behavior,
            //AsyncIOBehavior, 
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<DbDataReader> ExecuteReaderCoreAsync(CommandBehavior behavior,
        //IOBehavior ioBehavior, 
        CancellationToken cancellationToken)
    {
        if (!IsValid(out var exception))
            throw exception!;

        cancellationToken.ThrowIfCancellationRequested();

        CurrentCommandBehavior = behavior;
        foreach (MySqlBatchCommandMock batchCommand in BatchCommands)
            batchCommand.Batch = this;

        var tables = await BatchAsyncExecutionRunner
            .ExecuteReaderCommandsAsync(
                Connection!,
                BatchCommands.Commands,
                CreateExecutableCommand,
                behavior,
                cancellationToken)
            .ConfigureAwait(false);
        return new MySqlDataReaderMock(tables);

        //var payloadCreator = IsPrepared ? SingleCommandPayloadCreator.Instance :
        //    ConcatenatedCommandPayloadCreator.Instance;
        //return executor.ExecuteReaderAsync(behavior, cancellationToken);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Executes all commands and returns the total affected rows.
    /// PT: Executa todos os comandos e retorna o total de linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery() =>
#else
    /// <summary>
    /// EN: Executes all commands and returns the total affected rows.
    /// PT: Executa todos os comandos e retorna o total de linhas afetadas.
    /// </summary>
    public int ExecuteNonQuery() =>
#endif
        DbExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Executes the first command and returns its scalar result.
    /// PT: Executa o primeiro comando e retorna seu resultado escalar.
    /// </summary>
    public override object? ExecuteScalar() =>
#else
    /// <summary>
    /// EN: Executes the first command and returns its scalar result.
    /// PT: Executa o primeiro comando e retorna seu resultado escalar.
    /// </summary>
    public object? ExecuteScalar() =>
#endif
        ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Asynchronously executes all commands and returns the affected row count.
    /// PT: Executa todos os comandos de forma assíncrona e retorna a contagem de linhas afetadas.
    /// </summary>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// EN: Asynchronously executes all commands and returns the affected row count.
    /// PT: Executa todos os comandos de forma assíncrona e retorna a contagem de linhas afetadas.
    /// </summary>
    public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteNonQueryAsync(cancellationToken);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Asynchronously executes the first command and returns its scalar result.
    /// PT: Executa o primeiro comando de forma assíncrona e retorna seu resultado escalar.
    /// </summary>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// EN: Asynchronously executes the first command and returns its scalar result.
    /// PT: Executa o primeiro comando de forma assíncrona e retorna seu resultado escalar.
    /// </summary>
    public Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteScalarAsync(cancellationToken);

    /// <summary>
    /// EN: Gets or sets the timeout applied to each command in the batch.
    /// PT: Obtém ou define o tempo limite aplicado a cada comando no lote.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
        int Timeout
    {
        get => m_timeout;
        set
        {
            m_timeout = value;
        }
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Performs no action because this mock does not use prepared statements.
    /// PT: Não realiza ação porque este simulado não usa instruções preparadas.
    /// </summary>
    public override void Prepare()
#else
    /// <summary>
    /// EN: Performs no action because this mock does not use prepared statements.
    /// PT: Não realiza ação porque este simulado não usa instruções preparadas.
    /// </summary>
    public void Prepare()
#endif
    {
        if (!NeedsPrepare(out var exception))
        {
            if (exception is not null)
                throw exception;
            return;
        }

        DoPrepareAsync(default).GetAwaiter().GetResult();
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Completes immediately because this mock does not require server-side preparation.
    /// PT: Conclui imediatamente porque este simulado não exige preparação no servidor.
    /// </summary>
    public override Task PrepareAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// EN: Completes immediately because this mock does not require server-side preparation.
    /// PT: Conclui imediatamente porque este simulado não exige preparação no servidor.
    /// </summary>
    public Task PrepareAsync(CancellationToken cancellationToken = default) =>
#endif
        DbPrepareAsync(cancellationToken);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Cancels execution by rolling back the current transaction when available.
    /// PT: Cancela a execução revertendo a transação atual quando disponível.
    /// </summary>
    public override void Cancel()
#else
    /// <summary>
    /// EN: Cancels execution by rolling back the current transaction when available.
    /// PT: Cancela a execução revertendo a transação atual quando disponível.
    /// </summary>
    public void Cancel()
#endif
    { }//    Connection?.Cancel(this, m_commandId, true);

#if NET6_0_OR_GREATER
	/// <summary>
	/// EN: Creates a new MySqlBatchCommandMock instance.
	/// PT: Cria uma nova instância de MySqlBatchCommandMock.
	/// </summary>
	protected override DbBatchCommand CreateDbBatchCommand() => new MySqlBatchCommandMock();
#endif

#if NET6_0_OR_GREATER
	/// <summary>
	/// EN: Releases resources associated with this batch.
	/// PT: Libera os recursos associados a este lote.
	/// </summary>
	public override void Dispose()
#else
    /// <summary>
    /// EN: Releases resources associated with this batch.
    /// PT: Libera os recursos associados a este lote.
    /// </summary>
    public void Dispose()
#endif
    {
        m_isDisposed = true;
#if NET6_0_OR_GREATER
		base.Dispose();
#endif
    }

    internal CommandBehavior CurrentCommandBehavior { get; set; }

    //int ICancellableCommand.CommandId => m_commandId;
    //int ICancellableCommand.CommandTimeout => Timeout;
    //int? ICancellableCommand.EffectiveCommandTimeout { get; set; }
    //int ICancellableCommand.CancelAttemptCount { get; set; }

    //CancellationTokenRegistration ICancellableCommand.RegisterCancel(CancellationToken cancellationToken)
    //{
    //    if (!cancellationToken.CanBeCanceled)
    //        return default;

    //    m_cancelAction ??= Cancel;
    //    return cancellationToken.Register(m_cancelAction);
    //}

    //void ICancellableCommand.SetTimeout(int milliseconds)
    //{
    //    Volatile.Write(ref m_commandTimedOut, false);

    //    if (m_cancelTimerId != 0)
    //        TimerQueue.Instance.Remove(m_cancelTimerId);

    //    if (milliseconds != Constants.InfiniteTimeout)
    //    {
    //        m_cancelForCommandTimeoutAction ??= CancelCommandForTimeout;
    //        m_cancelTimerId = TimerQueue.Instance.Add(milliseconds, m_cancelForCommandTimeoutAction);
    //    }
    //}

    //bool ICancellableCommand.IsTimedOut => Volatile.Read(ref m_commandTimedOut);

    private void CancelCommandForTimeout()
    {
        Volatile.Write(ref m_commandTimedOut, true);
        Cancel();
    }

    private async Task<int> DbExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (!IsValid(out var exception))
            throw exception!;

        cancellationToken.ThrowIfCancellationRequested();
        return await BatchAsyncExecutionRunner
            .ExecuteNonQueryCommandsAsync(
                Connection!,
                BatchCommands.Commands,
                CreateExecutableCommand,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<object?> DbExecuteScalarAsync(CancellationToken cancellationToken)
    {
        if (!IsValid(out var exception))
            throw exception!;

        cancellationToken.ThrowIfCancellationRequested();
        return await BatchScalarExecutionRunner
            .ExecuteFirstScalarAsync(
                Connection!,
                BatchCommands.Commands,
                CreateExecutableCommand,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private MySqlCommandMock CreateExecutableCommand(MySqlBatchCommandMock batchCommand)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchCommandFactory.Create(
            connection,
            () =>
            {
                var command = (MySqlCommandMock)connection.CreateCommand();
                command.Transaction = Transaction;
                return command;
            },
            batchCommand,
            Timeout,
            static (command, source, timeout) =>
            {
                command.CommandType = source.CommandType;
                command.CommandText = source.CommandText;
                command.CommandTimeout = timeout;

                foreach (MySqlParameter parameter in source.Parameters)
                    command.Parameters.Add(new MySqlParameter(parameter.ParameterName, parameter.MySqlDbType)
                    {
                        Value = parameter.Value,
                    });
            });
    }

    private bool IsValid(
#if !NET48
        [NotNullWhen(false)]
#endif
    out Exception? exception)
    {
        exception = ValidateBatchState(allowConnectingState: true);
        if (exception is null)
            exception = GetExceptionForInvalidCommands();

        return exception is null;
    }

    private bool NeedsPrepare(out Exception? exception)
    {
        exception = ValidateBatchState(allowConnectingState: false);
        if (exception is null)
            exception = GetExceptionForInvalidCommands();

        return exception is null;// && !Connection!.IgnorePrepare;
    }

    private Exception? ValidateBatchState(bool allowConnectingState)
    {
        if (m_isDisposed)
            return new ObjectDisposedException(GetType().Name);

        if (Connection is null)
            return new InvalidOperationException(SqlExceptionMessages.BatchConnectionRequired());

        var invalidConnectionState = BatchExecutionGuards.GetInvalidConnectionStateException(Connection, allowConnectingState);
        if (invalidConnectionState is not null)
            return invalidConnectionState;

        //if (!Connection.IgnoreCommandTransaction && Transaction != Connection.CurrentTransaction)
        //    return new InvalidOperationException("The transaction associated with this batch is not the connection's active transaction; see https://mysqlconnector.net/trans");
        //if (Connection.HasActiveReader)
        //    return new InvalidOperationException("Cannot call Prepare when there is an open DataReader for this command; it must be closed first.");

        try
        {
            BatchExecutionGuards.RequireAtLeastOneCommand(BatchCommands.Count);
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }

        return null;
    }

    private InvalidOperationException? GetExceptionForInvalidCommands()
    {
        foreach (var command in BatchCommands)
        {
            if (command is null)
                return new InvalidOperationException(SqlExceptionMessages.BatchCommandsMustNotContainNull());
            if (string.IsNullOrWhiteSpace(command.CommandText))
                return new InvalidOperationException(SqlExceptionMessages.BatchCommandTextRequired());
        }
        return null;
    }

    private Task DbPrepareAsync(CancellationToken cancellationToken)
    {
        if (!NeedsPrepare(out var exception))
            return exception is null ? Task.CompletedTask : Task.FromException(exception);

        return DoPrepareAsync(cancellationToken);
    }

    private async Task DoPrepareAsync(CancellationToken cancellationToken)
    {
        foreach (IMySqlCommandMock batchCommand in BatchCommands)
        {
            if (batchCommand.CommandType != CommandType.Text)
                throw new NotSupportedException(SqlExceptionMessages.MySqlBatchPrepareOnlyTextSupported());
            ((MySqlBatchCommandMock)batchCommand).Batch = this;

            // don't prepare the same SQL twice
            //if (Connection!.Session.TryGetPreparedStatement(batchCommand.CommandText!) is null)
            //    await Connection.Session.PrepareAsync(batchCommand, cancellationToken).ConfigureAwait(false);
        }
    }

    //private bool IsPrepared
    //{
    //    get
    //    {
    //        foreach (var command in BatchCommands)
    //        {
    //            if (Connection!.Session.TryGetPreparedStatement(command!.CommandText!) is null)
    //                return false;
    //        }
    //        return true;
    //    }
    //}

    private bool m_isDisposed;
    private int m_timeout;
    private bool m_commandTimedOut;
}
