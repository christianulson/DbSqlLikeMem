using System.Linq.Expressions;
using System.Reflection;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteQueryProvider(
    SqliteConnectionMock cnn
    ) : IQueryProvider
{
    private readonly SqliteConnectionMock _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly SqliteTranslator _translator = new();

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var elementType = expression.Type.GetGenericArguments()[0];
        var tableName = ExtractTableName(expression);
        var queryType = typeof(SqliteQueryable<>).MakeGenericType(elementType);

        return (IQueryable)Activator.CreateInstance(
            queryType,
            /* provider   */ this,
            /* expression */ expression,
            /* tableName  */ tableName
        )!;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var tableName = ExtractTableName(expression);
        return new SqliteQueryable<TElement>(this, expression, tableName);
    }

    // ... Execute<TEntity>, Execute(Expression) permanecem iguais ...

    private static string ExtractTableName(Expression expression)
    {
        switch (expression)
        {
            // 1) Se for um ConstantExpression cuja Value é SqliteQueryable<AlgumTipo>
            case ConstantExpression c:
                {
                    var val = c.Value;
                    if (val != null)
                    {
                        var valType = val.GetType();
                        if (valType.IsGenericType &&
                            valType.GetGenericTypeDefinition() == typeof(SqliteQueryable<>))
                        {
                            // Pega a propriedade pública TableName por reflection
                            var prop = valType.GetProperty(
                                nameof(SqliteQueryable<object>.TableName),
                                BindingFlags.Instance | BindingFlags.Public
                            );
                            if (prop != null)
                                return (string)prop.GetValue(val)!;
                        }
                    }
                    break;
                }

            // 2) Se for uma chamada de método (Where, OrderBy, etc), recorre ao primeiro argumento
            case MethodCallExpression m:
                return ExtractTableName(m.Arguments[0]);
        }

        throw new InvalidOperationException(
            $"Não foi possível extrair o nome da tabela da expressão: {expression}"
        );
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var translation = _translator.Translate(expression);
        var sql = translation.Sql ?? string.Empty;

        return LinqQueryExecutor.Execute<TResult>(_cnn, sql, translation.Params);
    }

    // Implementação não-genérica, exigida pela interface
    object IQueryProvider.Execute(Expression expression)
        => Execute<object>(expression);
}
