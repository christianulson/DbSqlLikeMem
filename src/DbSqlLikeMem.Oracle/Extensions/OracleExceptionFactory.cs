namespace DbSqlLikeMem.Oracle;

internal static class OracleExceptionFactory
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new OracleMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new OracleMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new OracleMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new OracleMockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new OracleMockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
