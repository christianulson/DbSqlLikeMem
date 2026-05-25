namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Provides a MySQL-specific mock connection implementation.
/// PT-br: Fornece uma implementacao de conexao mock especifica de MySQL.
/// </summary>
public class MySqlConnectionMock
    : DbConnectionMockBase
{
    static MySqlConnectionMock()
    {
        MySqlAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Creates a MySQL mock connection with optional database overrides.
    /// PT-br: Cria uma conexao mock de MySQL com substituicoes opcionais de banco.
    /// </summary>
    public MySqlConnectionMock(
       MySqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"MySQL {FormatServerVersion(Db.Version)}";
    }

    private static string FormatServerVersion(int version)
        => version switch
        {
            30 => "3.0",
            40 => "4.0",
            55 => "5.5",
            56 => "5.6",
            57 => "5.7",
            80 => "8.0",
            84 => "8.4",
            _ => version.ToString(),
        };

    /// <summary>
    /// EN: Creates a MySQL transaction mock.
    /// PT-br: Cria um simulado de transação MySQL.
    /// </summary>
    /// <returns>EN: Transaction instance. PT-br: Instância da transação.</returns>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new MySqlTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a MySQL command mock for the transaction.
    /// PT-br: Cria um simulado de comando MySQL para a transação.
    /// </summary>
    /// <param name="transaction">EN: Current transaction. PT-br: Transação atual.</param>
    /// <returns>EN: Command instance. PT-br: Instância do comando.</returns>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new MySqlCommandMock(this, transaction as MySqlTransactionMock);

    /// <summary>
    /// EN: Creates the MySQL-specific mock exception used by this connection.
    /// PT-br: Cria a excecao simulada especifica do MySQL usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new MySqlMockException(message, code);
}
