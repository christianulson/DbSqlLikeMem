namespace DbSqlLikeMem.MySql;

public sealed class MySqlBatchMock :
#if NET6_0_OR_GREATER
    DbBatch,
#endif
    IDisposable
{
    public MySqlBatchMock()
        : this(null, null)
    {
    }

    public MySqlBatchMock(
        MySqlConnectionMock? connection,
        MySqlTransactionMock? transaction = null)
    {
        Connection = connection;
        Transaction = transaction;
        BatchCommands = new();
    }

#if NET6_0_OR_GREATER
    public new MySqlConnectionMock? Connection { get; set; }
    protected override DbConnection? DbConnection { get => Connection; set => Connection = (MySqlConnectionMock?)value; }
    public new MySqlTransactionMock? Transaction { get; set; }
    protected override DbTransaction? DbTransaction { get => Transaction; set => Transaction = (MySqlTransactionMock?)value; }
#else
    public MySqlConnectionMock? Connection { get; set; }
    public MySqlTransactionMock? Transaction { get; set; }
#endif

    /// <summary>
    /// The collection of commands that will be executed in the batch.
    /// </summary>
#if NET6_0_OR_GREATER
    public new MySqlBatchCommandCollectionMock BatchCommands { get; }
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;
#else
    public MySqlBatchCommandCollectionMock BatchCommands { get; }
#endif

    /// <summary>
    /// Executes all the commands in the batch, returning a <see cref="MySqlDataReader"/> that can iterate
    /// over the result sets. If multiple resultsets are returned, use <see cref="MySqlDataReader.NextResult"/>
    /// to access them.
    /// </summary>
#if NET6_0_OR_GREATER
    public new MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#else
    public MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#endif
        (MySqlDataReaderMock) ExecuteDbDataReader(commandBehavior);

    /// <summary>
    /// Executes all the commands in the batch, returning a <see cref="MySqlDataReader"/> that can iterate
    /// over the result sets. If multiple resultsets are returned, use <see cref="MySqlDataReader.NextResultAsync(CancellationToken)"/>
    /// to access them.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="Task{MySqlDataReaderMock}"/> containing the result of the asynchronous operation.</returns>
#if NET6_0_OR_GREATER
    public new async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#else
    public async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#endif
        (MySqlDataReaderMock)await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

    //// TODO: new ExecuteReaderAsync(CommandBehavior)

#if NET6_0_OR_GREATER
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
#else
    [SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Matches .NET 6.0 override")]
    private DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
#endif
    {
        //this.ResetCommandTimeout();
#pragma warning disable CA2012 // OK to read .Result because the ValueTask is completed
        return ExecuteReaderAsync(behavior, CancellationToken.None).Result;
#pragma warning restore CA2012
    }

#if NET6_0_OR_GREATER
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
#else
    private async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
#endif
    {
        //this.ResetCommandTimeout();
        //using var registration = ((ICancellableCommand)this).RegisterCancel(cancellationToken);
        return await ExecuteReaderAsync(behavior,
            //AsyncIOBehavior, 
            cancellationToken).ConfigureAwait(false);
    }

    private ValueTask<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior,
        //IOBehavior ioBehavior, 
        CancellationToken cancellationToken)
    {
        //if (!IsValid(out var exception))
        //    return ValueTaskExtensions.FromException<MySqlDataReaderMock>(exception);

        CurrentCommandBehavior = behavior;
        foreach (MySqlBatchCommandMock batchCommand in BatchCommands)
            batchCommand.Batch = this;

        var executor = Connection.CreateCommand();
        throw new NotImplementedException();

        //var payloadCreator = IsPrepared ? SingleCommandPayloadCreator.Instance :
        //    ConcatenatedCommandPayloadCreator.Instance;
        //return executor.ExecuteReaderAsync(behavior, cancellationToken);
    }

#if NET6_0_OR_GREATER
    public override int ExecuteNonQuery() =>
#else
    public int ExecuteNonQuery() =>
#endif
        DbExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    public override object? ExecuteScalar() =>
#else
    public object? ExecuteScalar() =>
#endif
        ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#else
    public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteNonQueryAsync(cancellationToken);

#if NET6_0_OR_GREATER
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#else
    public Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteScalarAsync(cancellationToken);

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
    public override void Prepare()
#else
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
    public override Task PrepareAsync(CancellationToken cancellationToken = default) =>
#else
    public Task PrepareAsync(CancellationToken cancellationToken = default) =>
#endif
        DbPrepareAsync(cancellationToken);

#if NET6_0_OR_GREATER
    public override void Cancel()
#else
    public void Cancel()
#endif
    { }//    Connection?.Cancel(this, m_commandId, true);

#if NET6_0_OR_GREATER
	protected override DbBatchCommand CreateDbBatchCommand() => new MySqlBatchCommandMock();
#endif

#if NET6_0_OR_GREATER
	public override void Dispose()
#else
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
        using var reader = await ExecuteReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        do
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {}
        } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
        return reader.RecordsAffected;
    }

    private async Task<object?> DbExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var hasSetResult = false;
        object? result = null;
        using var reader = await ExecuteReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        do
        {
            var hasResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (!hasSetResult)
            {
                if (hasResult)
                    result = reader.GetValue(0);
                hasSetResult = true;
            }
        } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
        return result;
    }

    private bool IsValid(
#if !NET48
        [NotNullWhen(false)]
#endif
    out Exception? exception)
    {
        if (m_isDisposed)
            exception = new ObjectDisposedException(GetType().Name);
        else if (Connection is null)
            exception = new InvalidOperationException("Connection property must be non-null.");
        else if (Connection.State is not ConnectionState.Open and not ConnectionState.Connecting)
            exception = new InvalidOperationException($"Connection must be Open; current state is {Connection.State}");
        //else if (!Connection.IgnoreCommandTransaction && Transaction != Connection.CurrentTransaction)
        //    exception = new InvalidOperationException("The transaction associated with this batch is not the connection's active transaction; see https://mysqlconnector.net/trans");
        else if (BatchCommands.Count == 0)
            exception = new InvalidOperationException("BatchCommands must contain a command");
        else
            exception = GetExceptionForInvalidCommands();

        return exception is null;
    }

    private bool NeedsPrepare(out Exception? exception)
    {
        if (m_isDisposed)
            exception = new ObjectDisposedException(GetType().Name);
        else if (Connection is null)
            exception = new InvalidOperationException("Connection property must be non-null.");
        else if (Connection.State != ConnectionState.Open)
            exception = new InvalidOperationException($"Connection must be Open; current state is {Connection.State}");
        else if (BatchCommands.Count == 0)
            exception = new InvalidOperationException("BatchCommands must contain a command");
        //else if (Connection.HasActiveReader)
        //    exception = new InvalidOperationException("Cannot call Prepare when there is an open DataReader for this command; it must be closed first.");
        else
            exception = GetExceptionForInvalidCommands();

        return exception is null;// && !Connection!.IgnorePrepare;
    }

    private InvalidOperationException? GetExceptionForInvalidCommands()
    {
        foreach (var command in BatchCommands)
        {
            if (command is null)
                return new InvalidOperationException("BatchCommands must not contain null");
            if (string.IsNullOrWhiteSpace(command.CommandText))
                return new InvalidOperationException("CommandText must be specified on each batch command");
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
                throw new NotSupportedException("Only CommandType.Text is currently supported by MySqlBatch.Prepare");
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