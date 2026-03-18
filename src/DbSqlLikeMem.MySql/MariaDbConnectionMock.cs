namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Mock connection specialized for MariaDB while reusing the shared MySQL command/runtime path.
/// PT: Conexao simulada especializada para MariaDB reutilizando o caminho compartilhado de comando/runtime do MySQL.
/// </summary>
public sealed class MariaDbConnectionMock : MySqlConnectionMock
{
    static MariaDbConnectionMock()
    {
        MySqlAstQueryExecutorRegister.Register(MariaDbDialect.DialectName);
    }

    /// <summary>
    /// EN: Creates a MariaDB connection mock with an optional in-memory database and default database name.
    /// PT: Cria uma conexao simulada MariaDB com banco em memoria opcional e nome padrao de banco.
    /// </summary>
    /// <param name="db">EN: Optional MariaDB mock database. PT: Banco MariaDB simulado opcional.</param>
    /// <param name="defaultDatabase">EN: Optional default database name. PT: Nome padrao opcional do banco.</param>
    public MariaDbConnectionMock(
        MariaDbDbMock? db = null,
        string? defaultDatabase = null
        ) : base(db ?? new MariaDbDbMock(), defaultDatabase)
    {
        _serverVersion = $"MariaDB {Db.Version}";
    }
}
