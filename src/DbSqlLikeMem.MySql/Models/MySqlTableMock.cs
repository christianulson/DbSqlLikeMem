namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Table mock specialized for MySQL schema operations.
/// PT: Mock de tabela especializado para operações de esquema MySQL.
/// </summary>
internal class MySqlTableMock(
        string tableName,
        MySqlSchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override string? CurrentColumn
    {
        get { return MySqlValueHelper.CurrentColumn; }
        set { MySqlValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
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

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => MySqlExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => MySqlExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => MySqlExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => MySqlExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => MySqlExceptionFactory.ReferencedRow(tbl);
}
