using System.Collections;
using System.Linq.Expressions;

namespace DbSqlLikeMem.SqlServer;
/// <summary>
/// EN: Represents Sql Server Queryable.
/// PT: Representa Sql Server Queryable.
/// </summary>
public class SqlServerQueryable<T> : IOrderedQueryable<T>
{
    /// <summary>
    /// EN: Gets or sets table name.
    /// PT: Obtém ou define table name.
    /// </summary>
    public string TableName { get; }
    /// <summary>
    /// EN: Gets or sets expression.
    /// PT: Obtém ou define expression.
    /// </summary>
    public Expression Expression { get; }
    /// <summary>
    /// EN: Executes sql server queryable.
    /// PT: Executa sql server queryable.
    /// </summary>
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

    /// <summary>
    /// EN: Executes typeof.
    /// PT: Executa typeof.
    /// </summary>
    public Type ElementType => typeof(T);
    IEnumerator IEnumerable.GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    /// <summary>
    /// EN: Gets enumerator.
    /// PT: Obtém enumerador.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
}
