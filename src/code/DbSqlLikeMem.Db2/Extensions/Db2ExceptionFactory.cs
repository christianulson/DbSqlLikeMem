namespace DbSqlLikeMem.Db2;

internal static class Db2ExceptionFactory
{
    /// <summary>
    /// EN: Implements DuplicateKey.
    /// PT-br: Implementa DuplicateKey.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new Db2MockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// EN: Implements UnknownColumn.
    /// PT-br: Implementa UnknownColumn.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new Db2MockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// EN: Implements ColumnCannotBeNull.
    /// PT-br: Implementa ColumnCannotBeNull.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new Db2MockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// EN: Implements ForeignKeyFails.
    /// PT-br: Implementa ForeignKeyFails.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new Db2MockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// EN: Implements ReferencedRow.
    /// PT-br: Implementa ReferencedRow.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new Db2MockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
