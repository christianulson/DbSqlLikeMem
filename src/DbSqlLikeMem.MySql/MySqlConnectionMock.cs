using System.Data.Common;

namespace DbSqlLikeMem.MySql;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class MySqlConnectionMock
    : DbConnectionMockBase
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public MySqlConnectionMock(
       MySqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"MySQL {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a MySQL transaction mock.
    /// PT: Cria um mock de transação MySQL.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction()
        => new MySqlTransactionMock(this);

    /// <summary>
    /// EN: Creates a MySQL command mock for the transaction.
    /// PT: Cria um mock de comando MySQL para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new MySqlCommandMock(this, transaction as MySqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new MySqlMockException(message, code);
}
