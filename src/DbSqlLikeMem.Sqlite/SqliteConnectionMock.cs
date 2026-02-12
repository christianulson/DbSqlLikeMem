using System.Data.Common;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteConnectionMock
    : DbConnectionMockBase
{
    static SqliteConnectionMock()
    {
        SqliteAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqliteConnectionMock(
       SqliteDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQLite {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a SQLite transaction mock.
    /// PT: Cria um mock de transação SQLite.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction()
        => new SqliteTransactionMock(this);

    /// <summary>
    /// EN: Creates a SQLite command mock for the transaction.
    /// PT: Cria um mock de comando SQLite para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqliteCommandMock(this, transaction as SqliteTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqliteMockException(message, code);
}
