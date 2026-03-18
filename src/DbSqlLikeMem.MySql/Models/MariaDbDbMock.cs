namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: In-memory database mock configured for MariaDB syntax and version gates.
/// PT: Banco de dados simulado em memoria configurado para sintaxe e gates de versao do MariaDB.
/// </summary>
public class MariaDbDbMock : MySqlDbMock
{
    /// <summary>
    /// EN: Initializes an in-memory MariaDB mock database with the requested version.
    /// PT: Inicializa um banco MariaDB simulado em memoria com a versao informada.
    /// </summary>
    /// <param name="version">EN: Optional simulated MariaDB version. PT: Versao simulada opcional do MariaDB.</param>
    public MariaDbDbMock(int? version = null)
        : base(version ?? MariaDbDbVersions.Default, static currentVersion => new MariaDbDialect(currentVersion))
    {
    }
}
