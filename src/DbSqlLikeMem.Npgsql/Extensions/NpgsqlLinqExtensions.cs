namespace DbSqlLikeMem.Npgsql;
public static class NpgsqlLinqExtensions
{
    public static IQueryable<T> AsQueryable<T>(this NpgsqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    public static IQueryable<T> AsQueryable<T>(
        this NpgsqlConnectionMock cnn,
        string tableName)
    {
        ArgumentNullException.ThrowIfNull(cnn);
        ArgumentNullException.ThrowIfNull(tableName);

        var provider = new NpgsqlQueryProvider(cnn);
        return new NpgsqlQueryable<T>(provider, tableName);
    }
}