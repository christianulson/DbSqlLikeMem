namespace DbSqlLikeMem.Npgsql;

internal class NpgsqlTableMock(
        string tableName,
        SchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{
    public override string? CurrentColumn
    {
        get { return NpgsqlValueHelper.CurrentColumn; }
        set { NpgsqlValueHelper.CurrentColumn = value; }
    }

    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null)
    {
        var exp = NpgsqlValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    public override Exception UnknownColumn(string columnName)
        => NpgsqlExceptionFactory.UnknownColumn(columnName);

    public override Exception DuplicateKey(string tbl, string key, object? val)
        => NpgsqlExceptionFactory.DuplicateKey(tbl, key, val);

    public override Exception ColumnCannotBeNull(string col)
        => NpgsqlExceptionFactory.ColumnCannotBeNull(col);

    public override Exception ForeignKeyFails(string col, string refTbl)
        => NpgsqlExceptionFactory.ForeignKeyFails(col, refTbl);

    public override Exception ReferencedRow(string tbl)
        => NpgsqlExceptionFactory.ReferencedRow(tbl);
}
