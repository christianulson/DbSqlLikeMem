using System.Data.Common;

namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// SqlServer mock connection. Hoje é um wrapper/alias do MySqlConnectionMock,
/// reaproveitando o mesmo engine em memória. Serve para isolar o ponto de troca
/// quando você implementar um executor/strategies específicos do SQL Server.
/// </summary>
public sealed class SqlServerConnectionMock
    : DbConnectionMockBase
{
    static SqlServerConnectionMock()
    {
        SqlServerAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqlServerConnectionMock(
       SqlServerDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Server {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a SQL Server transaction mock.
    /// PT: Cria um mock de transação SQL Server.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a SQL Server command mock for the transaction.
    /// PT: Cria um mock de comando SQL Server para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: SQL Server mock does not support RELEASE SAVEPOINT syntax.
    /// PT: O mock SQL Server não suporta sintaxe RELEASE SAVEPOINT.
    /// </summary>
    protected override bool SupportsReleaseSavepoint => false;

    internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
