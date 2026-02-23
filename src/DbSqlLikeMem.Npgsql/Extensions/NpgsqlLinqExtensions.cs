namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Defines the class NpgsqlLinqExtensions.
/// PT: Define a classe NpgsqlLinqExtensions.
/// </summary>
public static class NpgsqlLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this NpgsqlConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
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
