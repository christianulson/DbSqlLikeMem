namespace DbSqlLikeMem.SqlServer;

public class SqlServerSchemaMock(
    string schemaName,
    SqlServerDbMock db,
    IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    protected override TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new SqlServerTableMock(tableName, this, columns, rows);
}
