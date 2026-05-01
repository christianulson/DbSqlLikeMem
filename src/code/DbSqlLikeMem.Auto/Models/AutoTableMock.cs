using System.Data;

namespace DbSqlLikeMem.Auto;

/// <summary>
/// EN: Table mock specialized for Auto schema operations.
/// PT-br: Tabela simulada especializada para operações de esquema no Auto.
/// </summary>
internal class AutoTableMock(
        string tableName,
        AutoSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <inheritdoc/>
    public override string? CurrentColumn
    {
        get { return AutoValueHelper.CurrentColumn; }
        set { AutoValueHelper.CurrentColumn = value; }
    }

    /// <inheritdoc/>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = AutoValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <inheritdoc/>
    public override Exception UnknownColumn(string columnName)
        => AutoExceptionFactory.UnknownColumn(columnName);

    /// <inheritdoc/>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => AutoExceptionFactory.DuplicateKey(tbl, key, val);

    /// <inheritdoc/>
    public override Exception ColumnCannotBeNull(string col)
        => AutoExceptionFactory.ColumnCannotBeNull(col);

    /// <inheritdoc/>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => AutoExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <inheritdoc/>
    public override Exception ReferencedRow(string tbl)
        => AutoExceptionFactory.ReferencedRow(tbl);
}
