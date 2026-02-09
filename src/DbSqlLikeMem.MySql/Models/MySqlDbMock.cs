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

    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new MySqlSchemaMock(schemaName, this, tables);
}
