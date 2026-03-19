using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents Sql Server Connection Mock.
/// PT: Representa Sql Server conexão simulada.
/// </summary>
public class SqlAzureConnectionMock : SqlServerConnectionMock
{
    /// <summary>
    /// EN: Creates a SQL Azure connection mock with optional in-memory database and default database name.
    /// PT: Cria uma conexao simulada do SQL Azure com banco em memoria opcional e nome de banco padrao.
    /// </summary>
    public SqlAzureConnectionMock(
       SqlAzureDbMock? db = null,
       string? defaultDatabase = null
   ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Azure {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a transaction instance bound to this SQL Azure mock connection.
    /// PT: Cria uma instancia de transacao vinculada a esta conexao simulada do SQL Azure.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates the core SQL Azure command instance for this connection and optional transaction.
    /// PT: Cria a instancia principal de comando SQL Azure para esta conexao e transacao opcional.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlAzureCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: Creates the SQL Azure-specific mock exception used by this connection.
    /// PT: Cria a excecao simulada especifica do SQL Azure usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new SqlAzureMockException(message, code);
}