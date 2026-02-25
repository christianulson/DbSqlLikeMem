namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Defines the class MySqlConnectionMock.
/// PT: Define a classe MySqlConnectionMock.
/// </summary>
public sealed class MySqlConnectionMock
    : DbConnectionMockBase
{
    static MySqlConnectionMock()
    {
        MySqlAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Implements MySqlConnectionMock.
    /// PT: Implementa MySqlConnectionMock.
    /// </summary>
    public MySqlConnectionMock(
       MySqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"MySQL {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a MySQL transaction mock.
    /// PT: Cria um simulado de transação MySQL.
    /// </summary>
    /// <returns>EN: Transaction instance. PT: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new MySqlTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a MySQL command mock for the transaction.
    /// PT: Cria um simulado de comando MySQL para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT: Transação atual.</param>
    /// <returns>EN: Command instance. PT: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new MySqlCommandMock(this, transaction as MySqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new MySqlMockException(message, code);
}
