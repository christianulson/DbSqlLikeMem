namespace DbSqlLikeMem;

/// <summary>
/// EN: Wraps a <see cref="DbCommand"/> and dispatches interception callbacks around command execution.
/// PT: Encapsula um <see cref="DbCommand"/> e despacha callbacks de interceptacao em torno da execucao do comando.
/// </summary>
public sealed class InterceptingDbCommand : DbCommand
{
    private readonly InterceptingDbConnection _connection;
    private readonly DbCommand _innerCommand;
    private readonly IReadOnlyList<DbConnectionInterceptor> _interceptors;
    private DbTransaction? _transaction;

    internal InterceptingDbCommand(
        InterceptingDbConnection connection,
        DbCommand innerCommand,
        IReadOnlyList<DbConnectionInterceptor> interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(innerCommand, nameof(innerCommand));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        _connection = connection;
        _innerCommand = innerCommand;
        _interceptors = interceptors;
    }

    /// <summary>
    /// EN: Gets the inner command wrapped by this interception command.
    /// PT: Obtem o comando interno encapsulado por este comando de interceptacao.
    /// </summary>
    public DbCommand InnerCommand => _innerCommand;

    /// <inheritdoc />
    public override string CommandText
    {
        get => _innerCommand.CommandText;
        set => _innerCommand.CommandText = value;
    }

    /// <inheritdoc />
    public override int CommandTimeout
    {
        get => _innerCommand.CommandTimeout;
        set => _innerCommand.CommandTimeout = value;
    }

    /// <inheritdoc />
    public override CommandType CommandType
    {
        get => _innerCommand.CommandType;
        set => _innerCommand.CommandType = value;
    }

    /// <inheritdoc />
    public override bool DesignTimeVisible
    {
        get => _innerCommand.DesignTimeVisible;
        set => _innerCommand.DesignTimeVisible = value;
    }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource
    {
        get => _innerCommand.UpdatedRowSource;
        set => _innerCommand.UpdatedRowSource = value;
    }

    /// <inheritdoc />
    protected override DbConnection DbConnection
    {
        get => _connection;
        set => _innerCommand.Connection = value is InterceptingDbConnection intercepted
            ? intercepted.InnerConnection
            : value;
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection => _innerCommand.Parameters;

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction
    {
        get => _transaction ?? _innerCommand.Transaction;
        set
        {
            _transaction = value;
            _innerCommand.Transaction = value is InterceptingDbTransaction intercepted
                ? intercepted.InnerTransaction
                : value;
        }
    }

    /// <inheritdoc />
    public override void Cancel() => _innerCommand.Cancel();

    /// <inheritdoc />
    public override int ExecuteNonQuery()
        => ExecuteWithInterception(
            DbCommandExecutionKind.NonQuery,
            static command => command.ExecuteNonQuery());

    /// <inheritdoc />
    public override object? ExecuteScalar()
        => ExecuteWithInterception(
            DbCommandExecutionKind.Scalar,
            static command => command.ExecuteScalar());

    /// <inheritdoc />
    public override void Prepare() => _innerCommand.Prepare();

    /// <inheritdoc />
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        => ExecuteWithInterceptionAsync(
            DbCommandExecutionKind.NonQuery,
            command => command.ExecuteNonQueryAsync(cancellationToken));

    /// <inheritdoc />
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        => ExecuteWithInterceptionAsync(
            DbCommandExecutionKind.Scalar,
            command => command.ExecuteScalarAsync(cancellationToken));

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => _innerCommand.CreateParameter();

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteWithInterception(
            DbCommandExecutionKind.Reader,
            command => command.ExecuteReader(behavior));

    /// <inheritdoc />
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
        => ExecuteWithInterceptionAsync(
            DbCommandExecutionKind.Reader,
            command => command.ExecuteReaderAsync(behavior, cancellationToken));

    private T ExecuteWithInterception<T>(
        DbCommandExecutionKind executionKind,
        Func<DbCommand, T> executor)
    {
        var context = new DbCommandExecutionContext(_connection, this, executionKind);

        foreach (var interceptor in _interceptors)
            interceptor.CommandExecuting(context);

        try
        {
            var result = executor(_innerCommand);

            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].CommandExecuted(context, result);

            return result;
        }
        catch (Exception ex)
        {
            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].CommandFailed(context, ex);

            throw;
        }
    }

    private async Task<T> ExecuteWithInterceptionAsync<T>(
        DbCommandExecutionKind executionKind,
        Func<DbCommand, Task<T>> executor)
    {
        var context = new DbCommandExecutionContext(_connection, this, executionKind);

        foreach (var interceptor in _interceptors)
            interceptor.CommandExecuting(context);

        try
        {
            var result = await executor(_innerCommand).ConfigureAwait(false);

            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].CommandExecuted(context, result);

            return result;
        }
        catch (Exception ex)
        {
            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].CommandFailed(context, ex);

            throw;
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _innerCommand.Dispose();

        base.Dispose(disposing);
    }
}
