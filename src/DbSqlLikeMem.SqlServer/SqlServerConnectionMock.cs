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

    public SqlServerConnectionMock(
       SqlServerDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"SQL Server {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a SQL Server transaction mock.
    /// PT: Cria um mock de transação SQL Server.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction()
        => new SqlServerTransactionMock(this);

    /// <summary>
    /// EN: Creates a SQL Server command mock for the transaction.
    /// PT: Cria um mock de comando SQL Server para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
