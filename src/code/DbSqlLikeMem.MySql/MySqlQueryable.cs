namespace DbSqlLikeMem.MySql;
/// <summary>
/// EN: IQueryable wrapper for MySQL LINQ translation.
/// PT: Wrapper IQueryable para tradução LINQ do MySQL.
/// </summary>
public class MySqlQueryable<T> : IOrderedQueryable<T>
{
    /// <summary>
    /// EN: Gets or sets TableName.
    /// PT: Obtém ou define TableName.
    /// </summary>
    public string TableName { get; }
    /// <summary>
    /// EN: Gets or sets Expression.
    /// PT: Obtém ou define Expression.
    /// </summary>
    public Expression Expression { get; }
    /// <summary>
    /// EN: Gets or sets Provider.
    /// PT: Obtém ou define Provider.
    /// </summary>
    public IQueryProvider Provider { get; }

    // Construtor para a raiz da consulta
    internal MySqlQueryable(
        MySqlQueryProvider provider,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        // Aqui a Expression.Type será IQueryable<TEntity>, compatível com Queryable.Where, etc.
        Expression = Expression.Constant(this, typeof(IQueryable<T>));
    }

    // Construtor usado pelo provider ao compor Where/OrderBy/Take/etc.
    internal MySqlQueryable(
        MySqlQueryProvider provider,
        Expression expression,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
    }

    /// <summary>
    /// EN: Implements typeof.
    /// PT: Implementa typeof.
    /// </summary>
    public Type ElementType => typeof(T);
    IEnumerator IEnumerable.GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    /// <summary>
    /// EN: Implements GetEnumerator.
    /// PT: Implementa GetEnumerator.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
}
