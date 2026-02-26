using System.Linq.Expressions;
using System.Reflection;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Provides LINQ query translation and execution for the PostgreSQL mock connection.
/// PT: Fornece tradução e execução de consultas LINQ para a conexão simulada do PostgreSQL.
/// </summary>
public sealed class NpgsqlQueryProvider(
    NpgsqlConnectionMock cnn
    ) : IQueryProvider
{
    private readonly NpgsqlConnectionMock _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly NpgsqlTranslator _translator = new();

    /// <summary>
    /// EN: Creates a new query instance.
    /// PT: Cria uma nova instância de consulta.
    /// </summary>
    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var elementType = expression.Type.GetGenericArguments()[0];
        var tableName = ExtractTableName(expression);
        var queryType = typeof(NpgsqlQueryable<>).MakeGenericType(elementType);

        return (IQueryable)Activator.CreateInstance(
            queryType,
            /* provider   */ this,
            /* expression */ expression,
            /* tableName  */ tableName
        )!;
    }

    /// <summary>
    /// EN: Creates a typed query for the provided expression after null validation.
    /// PT: Cria uma consulta tipada para a expressão informada após validação de nulo.
    /// </summary>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var tableName = ExtractTableName(expression);
        return new NpgsqlQueryable<TElement>(this, expression, tableName);
    }

    // ... Execute<TEntity>, Execute(Expression) permanecem iguais ...

    private static string ExtractTableName(Expression expression)
    {
        switch (expression)
        {
            // 1) Se for um ConstantExpression cuja Value é NpgsqlQueryable<AlgumTipo>
            case ConstantExpression c:
                {
                    var val = c.Value;
                    if (val != null)
                    {
                        var valType = val.GetType();
                        if (valType.IsGenericType &&
                            valType.GetGenericTypeDefinition() == typeof(NpgsqlQueryable<>))
                        {
                            // Pega a propriedade pública TableName por reflection
                            var prop = valType.GetProperty(
                                nameof(NpgsqlQueryable<object>.TableName),
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
    /// EN: Executes the provided expression and returns the translated result.
    /// PT: Executa a expressão informada e retorna o resultado traduzido.
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
