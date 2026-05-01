namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: In-memory database mock configured for Npgsql.
/// PT-br: Banco de dados simulado em memória configurado para Npgsql.
/// </summary>
public class NpgsqlDbMock : DbMock
{
    private const string DefaultNpgsqlSchemaName = "public";

    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Gets the schema lookup order used by Npgsql when callers omit an explicit schema name.
    /// PT-br: Obtém a ordem de consulta de schema usada pelo Npgsql quando os chamadores omitem um nome de schema explicito.
    /// </summary>
    protected override IReadOnlyList<string> ImplicitSchemaLookupOrder => [DefaultNpgsqlSchemaName, "DefaultSchema"];

    /// <summary>
    /// EN: Initializes an in-memory PostgreSQL mock database with the requested version.
    /// PT-br: Inicializa um banco PostgreSQL simulado em memória com a versão informada.
    /// </summary>
    public NpgsqlDbMock(
    int? version = null
    ) : base(version ?? 17)
    {
        Dialect = new NpgsqlDialect(Version);
    }

    /// <summary>
    /// EN: Creates a PostgreSQL schema mock instance.
    /// PT-br: Cria uma instância de simulado de schema PostgreSQL.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT-br: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT-br: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT-br: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new NpgsqlSchemaMock(schemaName, this, tables);
}
