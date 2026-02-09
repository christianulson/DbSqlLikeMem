namespace DbSqlLikeMem.Npgsql;

public class NpgsqlDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public NpgsqlDbMock(
    int? version = null
    ) : base(version ?? 17)
    {
        Dialect = new NpgsqlDialect(Version);
    }

    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new NpgsqlSchemaMock(schemaName, this, tables);
}
