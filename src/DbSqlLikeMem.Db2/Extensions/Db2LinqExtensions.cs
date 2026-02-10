namespace DbSqlLikeMem.Db2;
/// <summary>
/// Auto-generated summary.
/// </summary>
public static class Db2LinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this Db2ConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this Db2ConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new Db2QueryProvider(cnn);
        return new Db2Queryable<T>(provider, tableName);
    }
}
