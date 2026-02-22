namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents Sqlite Connection Mock.
/// PT: Representa uma conexão simulada do Sqlite.
/// </summary>
public sealed class SqliteConnectionMock
    : DbConnectionMockBase
{
    static SqliteConnectionMock()
    {
        SqliteAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Represents Sqlite Connection Mock.
    /// PT: Representa uma conexão simulada do Sqlite.
    /// </summary>
    public SqliteConnectionMock(
       SqliteDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQLite {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqliteTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqliteCommandMock(this, transaction as SqliteTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqliteMockException(message, code);
}
