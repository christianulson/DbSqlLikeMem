using SQLitePCL;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents Sqlite Connection Mock.
/// PT-br: Representa uma conexão simulada do Sqlite.
/// </summary>
public sealed class SqliteConnectionMock
    : DbConnectionMockBase
{
    static SqliteConnectionMock()
    {
        Batteries_V2.Init();
        SqliteAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Creates a SQLite connection mock with default in-memory database settings.
    /// PT-br: Cria uma conexao simulada do SQLite com configuracoes padrao de banco em memoria.
    /// </summary>
    public SqliteConnectionMock() : this(null, null)
    {
    }

    /// <summary>
    /// EN: Represents Sqlite Connection Mock.
    /// PT-br: Representa uma conexão simulada do Sqlite.
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
    /// PT-br: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqliteTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT-br: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqliteCommandMock(this, transaction as SqliteTransactionMock);

    /// <summary>
    /// EN: Creates the SQLite-specific mock exception used by this connection.
    /// PT-br: Cria a excecao simulada especifica do SQLite usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new SqliteMockException(message, code);
}
