namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Summary for SqlServerConnectionMock.
/// PT: Resumo para SqlServerConnectionMock.
/// </summary>
public sealed class SqlServerConnectionMock
    : DbConnectionMockBase
{
    static SqlServerConnectionMock()
    {
        SqlServerAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Summary for SqlServerConnectionMock.
    /// PT: Resumo para SqlServerConnectionMock.
    /// </summary>
    public SqlServerConnectionMock(
       SqlServerDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Server {Db.Version}";
    }

    /// <summary>
    /// EN: Summary for CreateTransaction.
    /// PT: Resumo para CreateTransaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Summary for CreateDbCommandCore.
    /// PT: Resumo para CreateDbCommandCore.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    protected override bool SupportsReleaseSavepoint => false;

    internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
