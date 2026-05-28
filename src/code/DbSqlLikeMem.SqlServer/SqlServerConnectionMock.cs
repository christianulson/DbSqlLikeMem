namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents Sql Server Connection Mock.
/// PT-br: Representa Sql Server conexão simulada.
/// </summary>
public class SqlServerConnectionMock
    : DbConnectionMockBase
{
    static SqlServerConnectionMock()
    {
        SqlServerAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Represents Sql Server Connection Mock.
    /// PT-br: Representa Sql Server conexão simulada.
    /// </summary>
    public SqlServerConnectionMock(
       SqlServerDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Server {Db.Version}";
    }

    /// <inheritdoc />
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <inheritdoc />
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: Indicates whether the provider supports releasing savepoints.
    /// PT-br: Indica se o provedor suporta liberar savepoints.
    /// </summary>
    protected override bool SupportsReleaseSavepoint => false;

    /// <summary>
    /// EN: Creates the SQL Server-specific mock exception used by this connection.
    /// PT-br: Cria a excecao simulada especifica do SQL Server usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
