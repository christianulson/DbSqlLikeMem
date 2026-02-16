using System.Collections.Immutable;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Table mock specialized for SQLite schema operations.
/// PT: Mock de tabela especializado para operações de esquema SQLite.
/// </summary>
internal class SqliteTableMock(
        string tableName,
        SqliteSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override string? CurrentColumn
    {
        get { return SqliteValueHelper.CurrentColumn; }
        set { SqliteValueHelper.CurrentColumn = value; }
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
        var exp = SqliteValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => SqliteExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqliteExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => SqliteExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqliteExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => SqliteExceptionFactory.ReferencedRow(tbl);
}
