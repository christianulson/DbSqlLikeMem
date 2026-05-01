namespace DbSqlLikeMem.Db2;
/// <summary>
/// EN: Adds Db2-specific LINQ queryable helpers for mock connections.
/// PT-br: Adiciona helpers LINQ especificos de DB2 para conexoes mock.
/// </summary>
public static class Db2LinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT-br: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this Db2ConnectionMock cnn)
        => cnn.AsQueryable<T>(typeof(T).Name);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT-br: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this Db2ConnectionMock cnn,
        string tableName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName, nameof(tableName));

        var provider = new Db2QueryProvider(cnn);
        return new Db2Queryable<T>(provider, tableName);
    }
}
