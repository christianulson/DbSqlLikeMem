namespace DbSqlLikeMem.MySql;

/// <summary>
/// MySQL mock type used to emulate provider behavior for tests.
/// Tipo de mock MySQL usado para emular o comportamento do provedor em testes.
/// </summary>
public sealed class MySqlBatchMock :
#if NET6_0_OR_GREATER
    DbBatch,
#endif
    IDisposable
{
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlBatchMock()
        : this(null, null)
    {
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlBatchMock(
        MySqlConnectionMock? connection,
        MySqlTransactionMock? transaction = null)
    {
        Connection = connection;
        Transaction = transaction;
        BatchCommands = new();
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlConnectionMock? Connection { get; set; }
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override DbConnection? DbConnection { get => Connection; set => Connection = (MySqlConnectionMock?)value; }
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlTransactionMock? Transaction { get; set; }
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override DbTransaction? DbTransaction { get => Transaction; set => Transaction = (MySqlTransactionMock?)value; }
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlConnectionMock? Connection { get; set; }
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlTransactionMock? Transaction { get; set; }
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlBatchCommandCollectionMock BatchCommands { get; }
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlBatchCommandCollectionMock BatchCommands { get; }
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlDataReaderMock ExecuteReader(CommandBehavior commandBehavior = CommandBehavior.Default) =>
#endif
        (MySqlDataReaderMock) ExecuteDbDataReader(commandBehavior);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public async Task<MySqlDataReaderMock> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
#endif
        (MySqlDataReaderMock)await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

    //// TODO: new ExecuteReaderAsync(CommandBehavior)

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
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
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
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

    private new ValueTask<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior,
        //IOBehavior ioBehavior, 
        CancellationToken cancellationToken)
    {
        if (!IsValid(out var exception))
            return ValueTask.FromException<DbDataReader>(exception!);

        cancellationToken.ThrowIfCancellationRequested();

        CurrentCommandBehavior = behavior;
        foreach (MySqlBatchCommandMock batchCommand in BatchCommands)
            batchCommand.Batch = this;

        var tables = new List<TableResultMock>();
        foreach (var batchCommand in BatchCommands)
        {
            using var command = CreateExecutableCommand(batchCommand);

            try
            {
                using var reader = command.ExecuteReader();
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
                        tables.Add(new TableResultMock(rows));
                } while (reader.NextResult());
            }
            catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
            {
                command.ExecuteNonQuery();
            }
        }

        return ValueTask.FromResult<DbDataReader>(new MySqlDataReaderMock(tables));

        //var payloadCreator = IsPrepared ? SingleCommandPayloadCreator.Instance :
        //    ConcatenatedCommandPayloadCreator.Instance;
        //return executor.ExecuteReaderAsync(behavior, cancellationToken);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override int ExecuteNonQuery() =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public int ExecuteNonQuery() =>
#endif
        DbExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override object? ExecuteScalar() =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public object? ExecuteScalar() =>
#endif
        ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteNonQueryAsync(cancellationToken);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) =>
#endif
        DbExecuteScalarAsync(cancellationToken);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
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
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override void Prepare()
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
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
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override Task PrepareAsync(CancellationToken cancellationToken = default) =>
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task PrepareAsync(CancellationToken cancellationToken = default) =>
#endif
        DbPrepareAsync(cancellationToken);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override void Cancel()
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public void Cancel()
#endif
    { }//    Connection?.Cancel(this, m_commandId, true);

#if NET6_0_OR_GREATER
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	protected override DbBatchCommand CreateDbBatchCommand() => new MySqlBatchCommandMock();
#endif

#if NET6_0_OR_GREATER
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	public override void Dispose()
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
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

        var total = 0;
        foreach (var batchCommand in BatchCommands)
        {
            using var command = CreateExecutableCommand(batchCommand);
            total += await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return total;
    }

    private async Task<object?> DbExecuteScalarAsync(CancellationToken cancellationToken)
    {
        if (!IsValid(out var exception))
            throw exception!;

        cancellationToken.ThrowIfCancellationRequested();
        if (BatchCommands.Count == 0)
            return null;

        using var command = CreateExecutableCommand(BatchCommands[0]);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    private MySqlCommandMock CreateExecutableCommand(MySqlBatchCommandMock batchCommand)
    {
        var command = Connection!.CreateCommand();
        command.Transaction = Transaction;
        command.CommandType = batchCommand.CommandType;
        command.CommandText = batchCommand.CommandText;
        command.CommandTimeout = Timeout;

        foreach (MySqlParameter parameter in batchCommand.Parameters)
            command.Parameters.Add(parameter.ParameterName, parameter.MySqlDbType).Value = parameter.Value;

        return command;
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
