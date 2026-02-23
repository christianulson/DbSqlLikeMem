namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Defines the class MySqlQueryProvider.
/// PT: Define a classe MySqlQueryProvider.
/// </summary>
public sealed class MySqlQueryProvider(
    MySqlConnectionMock cnn
    ) : IQueryProvider
{
    private readonly MySqlConnectionMock _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly MySqlTranslator _translator = new();

    /// <summary>
    /// EN: Implements CreateQuery.
    /// PT: Implementa CreateQuery.
    /// </summary>
    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var elementType = expression.Type.GetGenericArguments()[0];
        var tableName = ExtractTableName(expression);
        var queryType = typeof(MySqlQueryable<>).MakeGenericType(elementType);

        return (IQueryable)Activator.CreateInstance(
            queryType,
            /* provider   */ this,
            /* expression */ expression,
            /* tableName  */ tableName
        )!;
    }

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
    /// </summary>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));
        var tableName = ExtractTableName(expression);
        return new MySqlQueryable<TElement>(this, expression, tableName);
    }

    // ... Execute<TEntity>, Execute(Expression) permanecem iguais ...

    private static string ExtractTableName(Expression expression)
    {
        switch (expression)
        {
            // 1) Se for um ConstantExpression cuja Value é MySqlQueryable<AlgumTipo>
            case ConstantExpression c:
                {
                    var val = c.Value;
                    if (val != null)
                    {
                        var valType = val.GetType();
                        if (valType.IsGenericType &&
                            valType.GetGenericTypeDefinition() == typeof(MySqlQueryable<>))
                        {
                            // Pega a propriedade pública TableName por reflection
                            var prop = valType.GetProperty(
                                nameof(MySqlQueryable<object>.TableName),
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
    /// EN: Implements this member.
    /// PT: Implementa este membro.
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
