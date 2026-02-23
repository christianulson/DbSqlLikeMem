namespace DbSqlLikeMem.SqlServer;
/// <summary>
/// EN: Defines the class SqlServerLinqExtensions.
/// PT: Define a classe SqlServerLinqExtensions.
/// </summary>
public static class SqlServerLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this SqlServerConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
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
