namespace DbSqlLikeMem.SqlServer;
public static class SqlServerLinqExtensions
{
    public static IQueryable<T> AsQueryable<T>(this SqlServerConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    public static IQueryable<T> AsQueryable<T>(
        this SqlServerConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new SqlServerQueryProvider(cnn);
        return new SqlServerQueryable<T>(provider, tableName);
    }
}