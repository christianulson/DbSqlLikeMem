using System.Data.Common;

namespace DbSqlLikeMem.MySql;

public sealed class MySqlConnectionMock
    : DbConnectionMockBase
{
    public MySqlConnectionMock(
       MySqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"MySQL {Db.Version}";
    }

    protected override DbTransaction CreateTransaction()
        => new MySqlTransactionMock(this);

    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new MySqlCommandMock(this, transaction as MySqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new MySqlMockException(message, code);
}
