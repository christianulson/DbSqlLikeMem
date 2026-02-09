namespace DbSqlLikeMem.Oracle;

internal static class OracleExceptionFactory
{
    public static Exception DuplicateKey(string tbl, string key, object? val)
    => new OracleMockException($"Duplicate entry '{val}' for key '{key}'", 1062);

    public static Exception UnknownColumn(string col)
        => new OracleMockException($"Unknown column '{col}'", 1054);

    public static Exception ColumnCannotBeNull(string col)
        => new OracleMockException($"Column '{col}' cannot be null", 1048);

    public static Exception ForeignKeyFails(string col, string refTbl)
        => new OracleMockException(
            $"Cannot add or update a child row: a foreign key constraint fails ({col} → {refTbl})", 1452);

    public static Exception ReferencedRow(string tbl)
        => new OracleMockException(
            $"Cannot delete or update a parent row: a foreign key constraint fails (child referencing '{tbl}')", 1451);
}