namespace DbSqlLikeMem.MySql;

public class MySqlSchemaMock(
    string schemaName,
    MySqlDbMock db,
    IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    protected override TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new MySqlTableMock(tableName, this, columns, rows);
}
