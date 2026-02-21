namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Summary for SqliteConnectionMock.
/// PT: Resumo para SqliteConnectionMock.
/// </summary>
public sealed class SqliteConnectionMock
    : DbConnectionMockBase
{
    static SqliteConnectionMock()
    {
        SqliteAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Summary for SqliteConnectionMock.
    /// PT: Resumo para SqliteConnectionMock.
    /// </summary>
    public SqliteConnectionMock(
       SqliteDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQLite {Db.Version}";
    }

    /// <summary>
    /// EN: Summary for CreateTransaction.
    /// PT: Resumo para CreateTransaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqliteTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Summary for CreateDbCommandCore.
    /// PT: Resumo para CreateDbCommandCore.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqliteCommandMock(this, transaction as SqliteTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqliteMockException(message, code);
}
