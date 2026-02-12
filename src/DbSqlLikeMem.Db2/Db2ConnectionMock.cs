using Dapper;
using System.Data.Common;

namespace DbSqlLikeMem.Db2;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class Db2ConnectionMock
    : DbConnectionMockBase
{
    static Db2ConnectionMock()
    {
        // O ODP.NET rejeita DbType.Guid, então mapeamos Guid para Object no fluxo Dapper.
        SqlMapper.AddTypeMap(typeof(Guid), DbType.Object);
        SqlMapper.AddTypeMap(typeof(Guid?), DbType.Object);
        Db2AstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Db2ConnectionMock(
       Db2DbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"DB2 {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a DB2 transaction mock.
    /// PT: Cria um mock de transação DB2.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new Db2TransactionMock(this, isolationLevel);

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
