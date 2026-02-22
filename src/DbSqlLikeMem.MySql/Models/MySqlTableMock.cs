namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Table mock specialized for MySQL schema operations.
/// PT: simulado de tabela especializado para operações de esquema MySQL.
/// </summary>
internal class MySqlTableMock(
        string tableName,
        MySqlSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <inheritdoc/>
    public override string? CurrentColumn
    {
        get { return MySqlValueHelper.CurrentColumn; }
        set { MySqlValueHelper.CurrentColumn = value; }
    }

    /// <inheritdoc/>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = MySqlValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <inheritdoc/>
    public override Exception UnknownColumn(string columnName)
        => MySqlExceptionFactory.UnknownColumn(columnName);

    /// <inheritdoc/>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => MySqlExceptionFactory.DuplicateKey(tbl, key, val);

    /// <inheritdoc/>
    public override Exception ColumnCannotBeNull(string col)
        => MySqlExceptionFactory.ColumnCannotBeNull(col);

    /// <inheritdoc/>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => MySqlExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <inheritdoc/>
    public override Exception ReferencedRow(string tbl)
        => MySqlExceptionFactory.ReferencedRow(tbl);
}
