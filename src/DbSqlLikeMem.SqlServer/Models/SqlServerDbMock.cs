namespace DbSqlLikeMem.SqlServer;

public class SqlServerDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public SqlServerDbMock(
        int? version = null
        ) : base(version ?? 2022)
    {
        Dialect = new SqlServerDialect(Version);
    }
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new SqlServerSchemaMock(schemaName, this, tables);
}
