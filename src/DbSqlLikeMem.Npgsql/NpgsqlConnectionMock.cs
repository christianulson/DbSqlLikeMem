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

    /// <summary>
    /// EN: Creates a PostgreSQL transaction mock.
    /// PT: Cria um mock de transação PostgreSQL.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction()
        => new NpgsqlTransactionMock(this);

    /// <summary>
    /// EN: Creates a PostgreSQL command mock for the transaction.
    /// PT: Cria um mock de comando PostgreSQL para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new NpgsqlCommandMock(this, transaction as NpgsqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new NpgsqlMockException(message, code);
}
