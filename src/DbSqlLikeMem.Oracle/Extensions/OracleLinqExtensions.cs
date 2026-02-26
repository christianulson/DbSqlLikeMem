namespace DbSqlLikeMem.Oracle;
/// <summary>
/// EN: Defines the class OracleLinqExtensions.
/// PT: Define a classe OracleLinqExtensions.
/// </summary>
public static class OracleLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this OracleConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this OracleConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var provider = new OracleQueryProvider(cnn);
        return new OracleQueryable<T>(provider, tableName);
    }
}
