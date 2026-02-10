namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// Auto-generated summary.
/// </summary>
public static class NpgsqlLinqExtensions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this NpgsqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this NpgsqlConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName,nameof(tableName));

        var provider = new NpgsqlQueryProvider(cnn);
        return new NpgsqlQueryable<T>(provider, tableName);
    }
}
