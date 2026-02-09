namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Table mock specialized for SQL Server schema operations.
/// PT: Mock de tabela especializado para operações de esquema SQL Server.
/// </summary>
public class SqlServerTableMock(
        string tableName,
        SqlServerSchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{
    public override string? CurrentColumn {
        get { return SqlServerValueHelper.CurrentColumn; }
        set { SqlServerValueHelper.CurrentColumn = value; }
    }

    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null)
    {
        var exp = SqlServerValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    public override Exception UnknownColumn(string columnName)
        => SqlServerExceptionFactory.UnknownColumn(columnName);

    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqlServerExceptionFactory.DuplicateKey(tbl, key, val);

    public override Exception ColumnCannotBeNull(string col)
        => SqlServerExceptionFactory.ColumnCannotBeNull(col);

    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqlServerExceptionFactory.ForeignKeyFails(col, refTbl);

    public override Exception ReferencedRow(string tbl)
        => SqlServerExceptionFactory.ReferencedRow(tbl);
}
