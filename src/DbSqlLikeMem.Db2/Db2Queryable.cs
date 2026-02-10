using System.Collections;
using System.Linq.Expressions;

namespace DbSqlLikeMem.Db2;
/// <summary>
/// EN: IQueryable wrapper for DB2 LINQ translation.
/// PT: Wrapper IQueryable para tradução LINQ do DB2.
/// </summary>
public class Db2Queryable<T> : IOrderedQueryable<T>
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public string TableName { get; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Expression Expression { get; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IQueryProvider Provider { get; }

    // Construtor para a raiz da consulta
    internal Db2Queryable(
        Db2QueryProvider provider,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        // Aqui a Expression.Type será IQueryable<TEntity>, compatível com Queryable.Where, etc.
        Expression = Expression.Constant(this, typeof(IQueryable<T>));
    }

    // Construtor usado pelo provider ao compor Where/OrderBy/Take/etc.
    internal Db2Queryable(
        Db2QueryProvider provider,
        Expression expression,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Type ElementType => typeof(T);
    IEnumerator IEnumerable.GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
}
