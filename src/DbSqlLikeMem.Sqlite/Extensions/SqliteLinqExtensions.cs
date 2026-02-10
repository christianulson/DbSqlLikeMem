namespace DbSqlLikeMem.Sqlite;
/// <summary>
/// Auto-generated summary.
/// </summary>
public static class SqliteLinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this SqliteConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this SqliteConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new SqliteQueryProvider(cnn);
        return new SqliteQueryable<T>(provider, tableName);
    }
}
