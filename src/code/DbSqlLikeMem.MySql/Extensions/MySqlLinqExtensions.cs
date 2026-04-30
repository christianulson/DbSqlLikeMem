namespace DbSqlLikeMem.MySql;
/// <summary>
/// EN: Adds MySQL-specific LINQ queryable helpers for mock connections.
/// PT: Adiciona helpers LINQ especificos de MySQL para conexoes mock.
/// </summary>
public static class MySqlLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this MySqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
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
