namespace DbSqlLikeMem.MySql;
/// <summary>
/// Auto-generated summary.
/// </summary>
public static class MySqlLinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this MySqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this MySqlConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var provider = new MySqlQueryProvider(cnn);
        return new MySqlQueryable<T>(provider, tableName);
    }
}
