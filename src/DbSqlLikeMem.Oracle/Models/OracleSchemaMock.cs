namespace DbSqlLikeMem.Oracle;

public class OracleSchemaMock(
    string schemaName,
    OracleDbMock db,
    IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    protected override TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new OracleTableMock(tableName, this, columns, rows);
}
