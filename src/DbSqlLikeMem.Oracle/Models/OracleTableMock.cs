namespace DbSqlLikeMem.Oracle;

internal class OracleTableMock(
        string tableName,
        OracleSchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{    public override string? CurrentColumn
    {
        get { return OracleValueHelper.CurrentColumn; }
        set { OracleValueHelper.CurrentColumn = value; }
    }

    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null)
    {
        var exp = OracleValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    public override Exception UnknownColumn(string columnName)
        => OracleExceptionFactory.UnknownColumn(columnName);

    public override Exception DuplicateKey(string tbl, string key, object? val)
        => OracleExceptionFactory.DuplicateKey(tbl, key, val);

    public override Exception ColumnCannotBeNull(string col)
        => OracleExceptionFactory.ColumnCannotBeNull(col);

    public override Exception ForeignKeyFails(string col, string refTbl)
        => OracleExceptionFactory.ForeignKeyFails(col, refTbl);

    public override Exception ReferencedRow(string tbl)
        => OracleExceptionFactory.ReferencedRow(tbl);
}
