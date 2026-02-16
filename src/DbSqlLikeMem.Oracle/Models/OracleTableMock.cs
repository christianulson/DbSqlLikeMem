using System.Collections.Immutable;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Table mock specialized for Oracle schema operations.
/// PT: Mock de tabela especializado para operações de esquema Oracle.
/// </summary>
internal class OracleTableMock(
        string tableName,
        OracleSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{    public override string? CurrentColumn
    {
        get { return OracleValueHelper.CurrentColumn; }
        set { OracleValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        ImmutableDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = OracleValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => OracleExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => OracleExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => OracleExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => OracleExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => OracleExceptionFactory.ReferencedRow(tbl);
}
