namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents Sql Server Connection Mock.
/// PT: Representa Sql Server conexão simulada.
/// </summary>
public sealed class SqlServerConnectionMock
    : DbConnectionMockBase
{
    static SqlServerConnectionMock()
    {
        SqlServerAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Represents Sql Server Connection Mock.
    /// PT: Representa Sql Server conexão simulada.
    /// </summary>
    public SqlServerConnectionMock(
       SqlServerDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Server {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: Executes new exception.
    /// PT: Executa new exception.
    /// </summary>
    protected override bool SupportsReleaseSavepoint => false;

    internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
