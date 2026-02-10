namespace DbSqlLikeMem.Oracle;
/// <summary>
/// Auto-generated summary.
/// </summary>
public static class OracleLinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this OracleConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this OracleConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new OracleQueryProvider(cnn);
        return new OracleQueryable<T>(provider, tableName);
    }
}
