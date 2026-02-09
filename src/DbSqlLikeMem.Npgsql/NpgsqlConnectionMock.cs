using System.Data.Common;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// Npgsql mock connection. Hoje é um wrapper/alias do MySqlConnectionMock,
/// reaproveitando o mesmo engine em memória. Serve para isolar o ponto de troca
/// quando você implementar um executor/strategies específicos do PostgreSQL.
/// </summary>
public sealed class NpgsqlConnectionMock
    : DbConnectionMockBase
{
    public NpgsqlConnectionMock(
       NpgsqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? new(), defaultDatabase)
    {
        _serverVersion = $"PostgreSQL {Db.Version}";
    }

    protected override DbTransaction CreateTransaction()
        => new NpgsqlTransactionMock(this);

    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new NpgsqlCommandMock(this, transaction as NpgsqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new NpgsqlMockException(message, code);
}
