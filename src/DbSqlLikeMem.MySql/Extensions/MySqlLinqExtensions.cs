namespace DbSqlLikeMem.MySql;
public static class MySqlLinqExtensions
{
    public static IQueryable<T> AsQueryable<T>(this MySqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    public static IQueryable<T> AsQueryable<T>(
        this MySqlConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new MySqlQueryProvider(cnn);
        return new MySqlQueryable<T>(provider, tableName);
    }
}