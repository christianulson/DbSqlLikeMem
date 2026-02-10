namespace DbSqlLikeMem.SqlServer;
/// <summary>
/// Auto-generated summary.
/// </summary>
public static class SqlServerLinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this SqlServerConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this SqlServerConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var provider = new SqlServerQueryProvider(cnn);
        return new SqlServerQueryable<T>(provider, tableName);
    }
}
