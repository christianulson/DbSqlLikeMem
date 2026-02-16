namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: In-memory database mock configured for SQLite.
/// PT: Mock de banco em memória configurado para SQLite.
/// </summary>
public class SqliteDbMock
    : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqliteDbMock(
        int? version = null
        ): base(version ?? 3)
    {
        Dialect = new SqliteDialect(Version);
    }

    /// <summary>
    /// EN: Creates a SQLite schema mock instance.
    /// PT: Cria uma instância de mock de schema SQLite.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new SqliteSchemaMock(schemaName, this, tables);
}
