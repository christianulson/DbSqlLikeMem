using System.Data.Common;
using Dapper;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Oracle mock connection. Hoje é um wrapper/alias do MySqlConnectionMock,
/// reaproveitando o mesmo engine em memória. Serve para isolar o ponto de troca
/// quando você implementar um executor/strategies específicos do Oracle.
/// </summary>
public class OracleConnectionMock
    : DbConnectionMockBase
{
    static OracleConnectionMock()
    {
        // O ODP.NET rejeita DbType.Guid, então mapeamos Guid para Object no fluxo Dapper.
        SqlMapper.AddTypeMap(typeof(Guid), DbType.Object);
        SqlMapper.AddTypeMap(typeof(Guid?), DbType.Object);
        OracleAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public OracleConnectionMock(
       OracleDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"Oracle {Db.Version}";
    }

    /// <summary>
    /// EN: Creates an Oracle transaction mock.
    /// PT: Cria um mock de transação Oracle.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new OracleTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates an Oracle command mock for the transaction.
    /// PT: Cria um mock de comando Oracle para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new OracleCommandMock(this, transaction as OracleTransactionMock);

    internal override Exception NewException(string message, int code)
        => new OracleMockException(message, code);
}
