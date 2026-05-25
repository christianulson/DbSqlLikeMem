namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Table mock specialized for Oracle schema operations.
/// PT-br: simulado de tabela especializado para operações de esquema Oracle.
/// </summary>
internal class OracleTableMock(
        string tableName,
        OracleSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{
    public override string? CurrentColumn
    {
        get { return OracleValueHelper.CurrentColumn; }
        set { OracleValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// EN: Implements ResolveRowsFrameRange.
    /// PT-br: Implementa ResolveRowsFrameRange.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = OracleValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// EN: Implements UnknownColumn.
    /// PT-br: Implementa UnknownColumn.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => OracleExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Implements DuplicateKey.
    /// PT-br: Implementa DuplicateKey.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => OracleExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Implements ColumnCannotBeNull.
    /// PT-br: Implementa ColumnCannotBeNull.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => OracleExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Implements ForeignKeyFails.
    /// PT-br: Implementa ForeignKeyFails.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => OracleExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Implements ReferencedRow.
    /// PT-br: Implementa ReferencedRow.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => OracleExceptionFactory.ReferencedRow(tbl);
}
