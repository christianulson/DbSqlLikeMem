namespace DbSqlLikeMem.SqlAzure;

internal static class SqlAzureExceptionFactory
{
    public static Exception DuplicateKey(string tbl, string key, object? val)
        => new SqlAzureMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062);

    public static Exception UnknownColumn(string col)
        => new SqlAzureMockException(SqlExceptionMessages.UnknownColumn(col), 1054);

    public static Exception ColumnCannotBeNull(string col)
        => new SqlAzureMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048);

    public static Exception ForeignKeyFails(string col, string refTbl)
        => new SqlAzureMockException(SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452);

    public static Exception ReferencedRow(string tbl)
        => new SqlAzureMockException(SqlExceptionMessages.ReferencedRow(tbl), 1451);
}
