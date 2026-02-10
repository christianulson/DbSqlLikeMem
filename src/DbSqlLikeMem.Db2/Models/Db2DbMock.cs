namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: In-memory database mock configured for DB2.
/// PT: Mock de banco em memória configurado para DB2.
/// </summary>
public class Db2DbMock
    : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Db2DbMock(
        int? version = null
        ): base(version ?? 11)
    {
        Dialect = new Db2Dialect(Version);
    }

    /// <summary>
    /// EN: Creates a DB2 schema mock instance.
    /// PT: Cria uma instância de mock de schema DB2.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new Db2SchemaMock(schemaName, this, tables);
}
