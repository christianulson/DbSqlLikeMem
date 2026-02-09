namespace DbSqlLikeMem.MySql;

internal class MySqlTableMock(
        string tableName,
        MySqlSchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    public override string? CurrentColumn
    {
        get { return MySqlValueHelper.CurrentColumn; }
        set { MySqlValueHelper.CurrentColumn = value; }
    }

    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null)
    {
        var exp = MySqlValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    public override Exception UnknownColumn(string columnName)
        => MySqlExceptionFactory.UnknownColumn(columnName);

    public override Exception DuplicateKey(string tbl, string key, object? val)
        => MySqlExceptionFactory.DuplicateKey(tbl, key, val);

    public override Exception ColumnCannotBeNull(string col)
        => MySqlExceptionFactory.ColumnCannotBeNull(col);

    public override Exception ForeignKeyFails(string col, string refTbl)
        => MySqlExceptionFactory.ForeignKeyFails(col, refTbl);

    public override Exception ReferencedRow(string tbl)
        => MySqlExceptionFactory.ReferencedRow(tbl);
}
