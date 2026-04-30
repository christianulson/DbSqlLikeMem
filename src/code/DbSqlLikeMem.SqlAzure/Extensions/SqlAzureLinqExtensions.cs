using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Defines LINQ helper extensions for SQL Azure mock connections.
/// PT: Define extensões helper de LINQ para conexões simuladas de SQL Azure.
/// </summary>
public static class SqlAzureLinqExtensions
{
    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the default table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela padrão.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this SqlAzureConnectionMock cnn)
        => SqlServerLinqExtensions.AsQueryable<T>(cnn);

    /// <summary>
    /// EN: Creates a queryable source for <typeparamref name="T"/> using the informed table name.
    /// PT: Cria uma fonte consultável para <typeparamref name="T"/> usando o nome de tabela informado.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(
        this SqlAzureConnectionMock cnn,
        string tableName)
        => SqlServerLinqExtensions.AsQueryable<T>(cnn, tableName);
}
