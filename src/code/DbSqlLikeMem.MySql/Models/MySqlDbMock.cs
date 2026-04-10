namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: In-memory database mock configured for MySQL.
/// PT: Banco de dados simulado em memória configurado para MySQL.
/// </summary>
public class MySqlDbMock
    : DbMock
{
    /// <inheritdoc/>
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Initializes an in-memory MySQL mock database with the requested version and optional ANSI_QUOTES and PIPES_AS_CONCAT parsing modes.
    /// PT: Inicializa um banco MySQL simulado em memória com a versão informada e os modos opcionais de parsing ANSI_QUOTES e PIPES_AS_CONCAT.
    /// </summary>
    /// <param name="version">EN: Optional simulated version. PT: Versão simulada opcional.</param>
    /// <param name="ansiQuotes">EN: Enables ANSI_QUOTES mode so double quotes are parsed as identifiers. PT: Habilita o modo ANSI_QUOTES para que aspas duplas sejam interpretadas como identificadores.</param>
    /// <param name="pipesAsConcat">EN: Enables PIPES_AS_CONCAT mode so || is parsed as string concatenation. PT: Habilita o modo PIPES_AS_CONCAT para que || seja interpretado como concatenacao de strings.</param>
    public MySqlDbMock(
        int? version = null,
        bool ansiQuotes = false,
        bool pipesAsConcat = false
        ) : this(version, currentVersion => new MySqlDialect(currentVersion, ansiQuotes, pipesAsConcat))
    {
    }

    /// <summary>
    /// EN: Initializes an in-memory MySQL-family mock database with a custom dialect factory.
    /// PT: Inicializa um banco simulado em memória da família MySQL com uma factory de dialeto customizada.
    /// </summary>
    /// <param name="version">EN: Optional simulated version. PT: Versão simulada opcional.</param>
    /// <param name="dialectFactory">EN: Factory used to create the dialect bound to this database. PT: Factory usada para criar o dialeto associado a este banco.</param>
    private protected MySqlDbMock(
        int? version,
        Func<int, SqlDialectBase> dialectFactory
        ) : base(version ?? MySqlDbVersions.Default)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialectFactory, nameof(dialectFactory));
        Dialect = dialectFactory(Version);
    }

    /// <summary>
    /// EN: Creates a MySQL schema mock instance.
    /// PT: Cria uma instância de simulado de schema MySQL.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new MySqlSchemaMock(schemaName, this, tables);
}
