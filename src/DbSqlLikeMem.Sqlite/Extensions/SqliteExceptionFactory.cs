namespace DbSqlLikeMem.Sqlite;

internal static class SqliteExceptionFactory
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new SqliteMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new SqliteMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new SqliteMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new SqliteMockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new SqliteMockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
