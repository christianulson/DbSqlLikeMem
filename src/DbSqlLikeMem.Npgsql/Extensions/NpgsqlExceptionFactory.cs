namespace DbSqlLikeMem.Npgsql;

internal static class NpgsqlExceptionFactory
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new NpgsqlMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new NpgsqlMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new NpgsqlMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new NpgsqlMockException(
            SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new NpgsqlMockException(
            SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
