namespace DbSqlLikeMem.Auto;

/// <summary>
/// EN: In-memory database mock configured for Auto.
/// PT-br: Banco de dados simulado em memória configurado para Auto.
/// </summary>
public class AutoDbMock
    : DbMock
{
    /// <inheritdoc/>
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Initializes an in-memory Auto mock database with the requested version.
    /// PT-br: Inicializa um banco Auto simulado em memória com a versão informada.
    /// </summary>
    public AutoDbMock(
        int? version = null
        ) : this(version, static currentVersion => new AutoSqlDialect(currentVersion))
    {
    }

    /// <summary>
    /// EN: Initializes an in-memory Auto-family mock database with a custom dialect factory.
    /// PT-br: Inicializa um banco simulado em memória da família Auto com uma factory de dialeto customizada.
    /// </summary>
    /// <param name="version">EN: Optional simulated version. PT-br: Versão simulada opcional.</param>
    /// <param name="dialectFactory">EN: Factory used to create the dialect bound to this database. PT-br: Factory usada para criar o dialeto associado a este banco.</param>
    private protected AutoDbMock(
        int? version,
        Func<int, SqlDialectBase> dialectFactory
        ) : base(version ?? 1)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialectFactory, nameof(dialectFactory));
        Dialect = dialectFactory(Version);
    }

    /// <summary>
    /// EN: Creates a Auto schema mock instance.
    /// PT-br: Cria uma instância de simulado de schema Auto.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT-br: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT-br: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT-br: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new AutoSchemaMock(schemaName, this, tables);
}
