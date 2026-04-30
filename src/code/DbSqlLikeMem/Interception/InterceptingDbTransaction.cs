namespace DbSqlLikeMem;

/// <summary>
/// EN: Wraps a <see cref="DbTransaction"/> returned by an intercepted connection.
/// PT: Encapsula uma <see cref="DbTransaction"/> retornada por uma conexao interceptada.
/// </summary>
public sealed class InterceptingDbTransaction : DbTransaction
{
    private readonly InterceptingDbConnection _connection;
    private readonly DbTransaction _innerTransaction;
    private readonly IReadOnlyList<DbConnectionInterceptor> _interceptors;

    internal InterceptingDbTransaction(
        InterceptingDbConnection connection,
        DbTransaction innerTransaction,
        IReadOnlyList<DbConnectionInterceptor> interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(innerTransaction, nameof(innerTransaction));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        _connection = connection;
        _innerTransaction = innerTransaction;
        _interceptors = interceptors;
    }

    /// <summary>
    /// EN: Gets the inner transaction wrapped by this interception transaction.
    /// PT: Obtem a transacao interna encapsulada por esta transacao de interceptacao.
    /// </summary>
    public DbTransaction InnerTransaction => _innerTransaction;

    /// <inheritdoc />
    public override IsolationLevel IsolationLevel => _innerTransaction.IsolationLevel;

    /// <inheritdoc />
    protected override DbConnection DbConnection => _connection;

    /// <inheritdoc />
    public override void Commit()
        => ExecuteWithInterception(
            DbTransactionOperationKind.Commit,
            static transaction => transaction.Commit());

    /// <inheritdoc />
    public override void Rollback()
        => ExecuteWithInterception(
            DbTransactionOperationKind.Rollback,
            static transaction => transaction.Rollback());

#if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Save(string savepointName)
#else
    /// <summary>
    /// EN: Creates a savepoint on the wrapped transaction.
    /// PT: Cria um savepoint na transacao encapsulada.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Save(string savepointName)
#endif
        => InvokeSavepointOperation("Save", savepointName);

#if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Rollback(string savepointName)
#else
    /// <summary>
    /// EN: Rolls back the wrapped transaction to a savepoint.
    /// PT: Executa rollback da transacao encapsulada para um savepoint.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Rollback(string savepointName)
#endif
        => InvokeSavepointOperation("Rollback", savepointName);

#if NET6_0_OR_GREATER
    /// <inheritdoc />
    public override void Release(string savepointName)
#else
    /// <summary>
    /// EN: Releases a savepoint on the wrapped transaction.
    /// PT: Libera um savepoint na transacao encapsulada.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name. PT: Nome do savepoint.</param>
    public void Release(string savepointName)
#endif
        => InvokeSavepointOperation("Release", savepointName);

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _innerTransaction.Dispose();

        base.Dispose(disposing);
    }

    private void ExecuteWithInterception(
        DbTransactionOperationKind operationKind,
        Action<DbTransaction> executor)
    {
        var context = new DbTransactionInterceptionContext(_connection, this, operationKind);

        foreach (var interceptor in _interceptors)
            interceptor.TransactionExecuting(context);

        try
        {
            executor(_innerTransaction);

            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].TransactionExecuted(context);
        }
        catch (Exception ex)
        {
            for (var i = _interceptors.Count - 1; i >= 0; i--)
                _interceptors[i].TransactionFailed(context, ex);

            throw;
        }
    }

    private void InvokeSavepointOperation(string methodName, string savepointName)
    {
        if (string.IsNullOrWhiteSpace(savepointName))
            throw new ArgumentException("Savepoint name cannot be empty.", nameof(savepointName));

        try
        {
            _innerTransaction.GetType().InvokeMember(
                methodName,
                BindingFlags.InvokeMethod | BindingFlags.Public | BindingFlags.Instance,
                binder: null,
                _innerTransaction,
                [savepointName]);
        }
        catch (MissingMethodException ex)
        {
            throw new NotSupportedException($"Savepoint operation '{methodName}' is not supported by the wrapped transaction.", ex);
        }
    }

}
