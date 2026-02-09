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

    protected override DbTransaction CreateTransaction()
        => new SqlServerTransactionMock(this);

    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlServerCommandMock(this, transaction as SqlServerTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqlServerMockException(message, code);
}
