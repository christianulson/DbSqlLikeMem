using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Wraps a <see cref="DbConnection"/> and dispatches interception callbacks around its lifecycle and commands.
/// PT: Encapsula uma <see cref="DbConnection"/> e despacha callbacks de interceptacao em torno de seu ciclo de vida e comandos.
/// </summary>
public sealed class InterceptingDbConnection : DbConnection
{
    private readonly DbConnection _innerConnection;
    private readonly IReadOnlyList<DbConnectionInterceptor> _interceptors;

    /// <summary>
    /// EN: Creates a wrapped connection that dispatches the supplied interceptors.
    /// PT: Cria uma conexao encapsulada que despacha os interceptors informados.
    /// </summary>
    /// <param name="innerConnection">EN: Inner connection. PT: Conexao interna.</param>
    /// <param name="interceptors">EN: Interceptors applied by the wrapper. PT: Interceptors aplicados pelo wrapper.</param>
    public InterceptingDbConnection(
        DbConnection innerConnection,
        IReadOnlyList<DbConnectionInterceptor> interceptors)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(innerConnection, nameof(innerConnection));
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        _innerConnection = innerConnection;
        _interceptors = interceptors;
    }

    /// <summary>
    /// EN: Gets the inner connection wrapped by this interception pipeline instance.
    /// PT: Obtem a conexao interna encapsulada por esta instancia do pipeline de interceptacao.
    /// </summary>
    public DbConnection InnerConnection => _innerConnection;

    /// <inheritdoc />
    [AllowNull]
    public override string ConnectionString
    {
        get => _innerConnection.ConnectionString;
        set => _innerConnection.ConnectionString = value;
    }

    /// <inheritdoc />
    public override string Database => _innerConnection.Database;

    /// <inheritdoc />
    public override string DataSource => _innerConnection.DataSource;

    /// <inheritdoc />
    public override string ServerVersion => _innerConnection.ServerVersion;

    /// <inheritdoc />
    public override ConnectionState State => _innerConnection.State;

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName) => _innerConnection.ChangeDatabase(databaseName);

    /// <inheritdoc />
    public override void Close()
    {
        foreach (var interceptor in _interceptors)
            interceptor.ConnectionClosing(this);

        _innerConnection.Close();

        for (var i = _interceptors.Count - 1; i >= 0; i--)
            _interceptors[i].ConnectionClosed(this);
    }

    /// <inheritdoc />
    public override void Open()
    {
        foreach (var interceptor in _interceptors)
            interceptor.ConnectionOpening(this);

        _innerConnection.Open();

        for (var i = _interceptors.Count - 1; i >= 0; i--)
            _interceptors[i].ConnectionOpened(this);
    }

    /// <inheritdoc />
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        foreach (var interceptor in _interceptors)
            interceptor.ConnectionOpening(this);

        await _innerConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        for (var i = _interceptors.Count - 1; i >= 0; i--)
            _interceptors[i].ConnectionOpened(this);
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        var startingContext = new DbTransactionStartingContext(this, isolationLevel);
        foreach (var interceptor in _interceptors)
            interceptor.TransactionStarting(startingContext);

        var transaction = new InterceptingDbTransaction(
            this,
            _innerConnection.BeginTransaction(isolationLevel),
            _interceptors);
        var startedContext = new DbTransactionInterceptionContext(this, transaction, DbTransactionOperationKind.Begin);

        for (var i = _interceptors.Count - 1; i >= 0; i--)
            _interceptors[i].TransactionStarted(startedContext);

        return transaction;
    }

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand()
    {
        var innerCommand = _innerConnection.CreateCommand();
        var command = new InterceptingDbCommand(this, innerCommand, _interceptors);
        foreach (var interceptor in _interceptors)
            interceptor.CommandCreated(this, command);

        return command;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _innerConnection.Dispose();

        base.Dispose(disposing);
    }
}
