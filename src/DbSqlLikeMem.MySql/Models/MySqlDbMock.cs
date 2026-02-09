namespace DbSqlLikeMem.MySql;

public class MySqlDbMock
    : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public MySqlDbMock(
        int? version = null
        ): base(version ?? 8)
    {
        Dialect = new MySqlDialect(Version);
    }

    /// <summary>
    /// EN: Creates a MySQL schema mock instance.
    /// PT: Cria uma instância de mock de schema MySQL.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new MySqlSchemaMock(schemaName, this, tables);
}
