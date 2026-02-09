using System.Data.Common;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Oracle mock connection. Hoje é um wrapper/alias do MySqlConnectionMock,
/// reaproveitando o mesmo engine em memória. Serve para isolar o ponto de troca
/// quando você implementar um executor/strategies específicos do Oracle.
/// </summary>
public class OracleConnectionMock
    : DbConnectionMockBase
{
    public OracleConnectionMock(
       OracleDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"Oracle {Db.Version}";
    }

    protected override DbTransaction CreateTransaction()
        => new OracleTransactionMock(this);

    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new OracleCommandMock(this, transaction as OracleTransactionMock);

    internal override Exception NewException(string message, int code)
        => new OracleMockException(message, code);
}
