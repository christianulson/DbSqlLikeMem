using System.Linq.Expressions;
using System.Reflection;

namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Summary for Db2QueryProvider.
/// PT: Resumo para Db2QueryProvider.
/// </summary>
public sealed class Db2QueryProvider(
    Db2ConnectionMock cnn
    ) : IQueryProvider
{
    private readonly Db2ConnectionMock _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly Db2Translator _translator = new();

    /// <summary>
    /// EN: Summary for CreateQuery.
    /// PT: Resumo para CreateQuery.
    /// </summary>
    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var elementType = expression.Type.GetGenericArguments()[0];
        var tableName = ExtractTableName(expression);
        var queryType = typeof(Db2Queryable<>).MakeGenericType(elementType);

        return (IQueryable)Activator.CreateInstance(
            queryType,
            /* provider   */ this,
            /* expression */ expression,
            /* tableName  */ tableName
        )!;
    }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var tableName = ExtractTableName(expression);
        return new Db2Queryable<TElement>(this, expression, tableName);
    }

    // ... Execute<TEntity>, Execute(Expression) permanecem iguais ...

    private static string ExtractTableName(Expression expression)
    {
        switch (expression)
        {
            // 1) Se for um ConstantExpression cuja Value é Db2Queryable<AlgumTipo>
            case ConstantExpression c:
                {
                    var val = c.Value;
                    if (val != null)
                    {
                        var valType = val.GetType();
                        if (valType.IsGenericType &&
                            valType.GetGenericTypeDefinition() == typeof(Db2Queryable<>))
                        {
                            // Pega a propriedade pública TableName por reflection
                            var prop = valType.GetProperty(
                                nameof(Db2Queryable<object>.TableName),
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
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var translation = _translator.Translate(expression);
        var sql = translation.Sql ?? string.Empty;

        return LinqQueryExecutor.Execute<TResult>(_cnn, sql, translation.Params);
    }

    // Implementação não-genérica, exigida pela interface
    object IQueryProvider.Execute(Expression expression)
        => Execute<object>(expression);
}
