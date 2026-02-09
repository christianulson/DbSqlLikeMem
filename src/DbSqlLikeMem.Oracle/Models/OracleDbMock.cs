namespace DbSqlLikeMem.Oracle;

public class OracleDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public OracleDbMock(
        int? version = null
        ) : base(version ?? 23)
    {
        Dialect = new OracleDialect(Version);
    }

    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new OracleSchemaMock(schemaName, this, tables);
}
