using System.Collections;
using System.Linq.Expressions;

namespace DbSqlLikeMem.SqlServer;
/// <summary>
/// EN: IQueryable wrapper for SQL Server LINQ translation.
/// PT: Wrapper IQueryable para tradução LINQ do SQL Server.
/// </summary>
public class SqlServerQueryable<T> : IOrderedQueryable<T>
{
    public string TableName { get; }
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    // Construtor para a raiz da consulta
    internal SqlServerQueryable(
        SqlServerQueryProvider provider,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        // Aqui a Expression.Type será IQueryable<TEntity>, compatível com Queryable.Where, etc.
        Expression = Expression.Constant(this, typeof(IQueryable<T>));
    }

    // Construtor usado pelo provider ao compor Where/OrderBy/Take/etc.
    internal SqlServerQueryable(
        SqlServerQueryProvider provider,
        Expression expression,
        string tableName)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
    }

    public Type ElementType => typeof(T);
    IEnumerator IEnumerable.GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
}
