namespace DbSqlLikeMem.Oracle;

internal static class OracleExceptionFactory
{
    /// <summary>
    /// EN: Implements DuplicateKey.
    /// PT: Implementa DuplicateKey.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new OracleMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// EN: Implements UnknownColumn.
    /// PT: Implementa UnknownColumn.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new OracleMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// EN: Implements ColumnCannotBeNull.
    /// PT: Implementa ColumnCannotBeNull.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new OracleMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// EN: Implements ForeignKeyFails.
    /// PT: Implementa ForeignKeyFails.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new OracleMockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// EN: Implements ReferencedRow.
    /// PT: Implementa ReferencedRow.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new OracleMockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
