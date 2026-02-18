namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Table mock specialized for DB2 schema operations.
/// PT: Mock de tabela especializado para operações de esquema DB2.
/// </summary>
internal class Db2TableMock(
        string tableName,
        Db2SchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override string? CurrentColumn
    {
        get { return Db2ValueHelper.CurrentColumn; }
        set { Db2ValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = Db2ValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => Db2ExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => Db2ExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => Db2ExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => Db2ExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => Db2ExceptionFactory.ReferencedRow(tbl);
}
