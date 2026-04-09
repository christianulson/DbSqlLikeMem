namespace DbSqlLikeMem.MySql;

internal static class MySqlExceptionFactory
{
    /// <summary>
    /// EN: Implements DuplicateKey.
    /// PT: Implementa DuplicateKey.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new MySqlMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// EN: Implements UnknownColumn.
    /// PT: Implementa UnknownColumn.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new MySqlMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// EN: Implements ColumnCannotBeNull.
    /// PT: Implementa ColumnCannotBeNull.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new MySqlMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// EN: Implements ForeignKeyFails.
    /// PT: Implementa ForeignKeyFails.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new MySqlMockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// EN: Implements ReferencedRow.
    /// PT: Implementa ReferencedRow.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new MySqlMockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
