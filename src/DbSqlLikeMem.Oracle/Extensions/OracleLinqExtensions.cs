namespace DbSqlLikeMem.Oracle;
public static class OracleLinqExtensions
{
    public static IQueryable<T> AsQueryable<T>(this OracleConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

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