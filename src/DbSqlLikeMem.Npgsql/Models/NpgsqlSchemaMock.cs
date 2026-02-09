namespace DbSqlLikeMem.Npgsql;

public class NpgsqlSchemaMock(
    string schemaName,
    NpgsqlDbMock db,
    IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    protected override TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new NpgsqlTableMock(tableName, this, columns, rows);
}
