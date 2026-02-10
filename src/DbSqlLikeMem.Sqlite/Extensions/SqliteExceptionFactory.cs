namespace DbSqlLikeMem.Sqlite;

internal static class SqliteExceptionFactory
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new SqliteMockException($"Duplicate entry '{val}' for key '{key}'", 1062);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception UnknownColumn(string col)
        => new SqliteMockException($"Unknown column '{col}'", 1054);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new SqliteMockException($"Column '{col}' cannot be null", 1048);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new SqliteMockException(
            $"Cannot add or update a child row: a foreign key constraint fails ({col} → {refTbl})", 1452);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new SqliteMockException(
            $"Cannot delete or update a parent row: a foreign key constraint fails (child referencing '{tbl}')", 1451);
}
