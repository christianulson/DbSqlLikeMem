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
    /// EN: Initializes an in-memory MySQL mock database with the requested version.
    /// PT: Inicializa um banco MySQL simulado em memória com a versão informada.
    /// </summary>
    public MySqlDbMock(
        int? version = null
        ): base(version ?? 8)
    {
        Dialect = new MySqlDialect(Version);
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
