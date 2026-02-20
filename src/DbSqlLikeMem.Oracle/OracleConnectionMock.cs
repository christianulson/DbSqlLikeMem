using System.Data.Common;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Summary for OracleConnectionMock.
/// PT: Resumo para OracleConnectionMock.
/// </summary>
public class OracleConnectionMock
    : DbConnectionMockBase
{
    static OracleConnectionMock()
    {
        // O ODP.NET rejeita DbType.Guid, então mapeamos Guid para Object no fluxo Dapper.
        DapperLateBinding.AddTypeMap(typeof(Guid), DbType.Object);
        DapperLateBinding.AddTypeMap(typeof(Guid?), DbType.Object);
        OracleAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Summary for OracleConnectionMock.
    /// PT: Resumo para OracleConnectionMock.
    /// </summary>
    public OracleConnectionMock(
       OracleDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"Oracle {Db.Version}";
    }

    /// <summary>
    /// EN: Summary for CreateTransaction.
    /// PT: Resumo para CreateTransaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new OracleTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Summary for CreateDbCommandCore.
    /// PT: Resumo para CreateDbCommandCore.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new OracleCommandMock(this, transaction as OracleTransactionMock);

    internal override Exception NewException(string message, int code)
        => new OracleMockException(message, code);
}
