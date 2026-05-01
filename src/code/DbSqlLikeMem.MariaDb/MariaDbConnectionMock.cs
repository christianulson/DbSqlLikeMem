namespace DbSqlLikeMem.MariaDb;

/// <summary>
/// EN: Mock connection specialized for MariaDB while reusing the shared MySQL command/runtime path.
/// PT-br: Conexao simulada especializada para MariaDB reutilizando o caminho compartilhado de comando/runtime do MySQL.
/// </summary>
public sealed class MariaDbConnectionMock : MySqlConnectionMock
{
    static MariaDbConnectionMock()
    {
        MySqlAstQueryExecutorRegister.Register(MariaDbDialect.DialectName);
    }

    /// <summary>
    /// EN: Creates a MariaDB connection mock with an optional in-memory database and default database name.
    /// PT-br: Cria uma conexao simulada MariaDB com banco em memoria opcional e nome padrao de banco.
    /// </summary>
    /// <param name="db">EN: Optional MariaDB mock database. PT-br: Banco MariaDB simulado opcional.</param>
    /// <param name="defaultDatabase">EN: Optional default database name. PT-br: Nome padrao opcional do banco.</param>
    public MariaDbConnectionMock(
        MariaDbDbMock? db = null,
        string? defaultDatabase = null
        ) : base(db ?? new MariaDbDbMock(), defaultDatabase)
    {
        _serverVersion = $"MariaDB {FormatServerVersion(Db.Version)}";
    }

    private static string FormatServerVersion(int version)
        => version switch
        {
            MariaDbDbVersions.Version10_3 => "10.3",
            MariaDbDbVersions.Version10_5 => "10.5",
            MariaDbDbVersions.Version10_6 => "10.6",
            MariaDbDbVersions.Version11_0 => "11.0",
            _ => version.ToString(),
        };
}
