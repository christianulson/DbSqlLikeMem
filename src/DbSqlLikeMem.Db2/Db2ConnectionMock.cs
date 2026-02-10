using System.Data.Common;

namespace DbSqlLikeMem.Db2;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class Db2ConnectionMock
    : DbConnectionMockBase
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Db2ConnectionMock(
       Db2DbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"DB2 {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a DB2 transaction mock.
    /// PT: Cria um mock de transação DB2.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction()
        => new Db2TransactionMock(this);

    /// <summary>
    /// EN: Creates a DB2 command mock for the transaction.
    /// PT: Cria um mock de comando DB2 para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new Db2CommandMock(this, transaction as Db2TransactionMock);

    internal override Exception NewException(string message, int code)
        => new Db2MockException(message, code);
}
