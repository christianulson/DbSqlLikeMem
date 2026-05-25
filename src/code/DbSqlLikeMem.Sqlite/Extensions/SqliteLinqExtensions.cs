namespace DbSqlLikeMem.Sqlite;
/// <summary>
/// EN: Adds SQLite-specific LINQ queryable helpers for mock connections.
/// PT-br: Adiciona helpers LINQ especificos de SQLite para conexoes mock.
/// </summary>
public static class SqliteLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT-br: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this SqliteConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT-br: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this SqliteConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName, nameof(tableName));

        var provider = new SqliteQueryProvider(cnn);
        return new SqliteQueryable<T>(provider, tableName);
    }
}
