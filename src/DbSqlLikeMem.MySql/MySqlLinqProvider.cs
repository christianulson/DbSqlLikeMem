using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace DbSqlLikeMem.MySql;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class MySqlQueryProvider(
    MySqlConnectionMock cnn
    ) : IQueryProvider
{
    private readonly MySqlConnectionMock _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly MySqlTranslator _translator = new();

    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
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
    /// Auto-generated summary.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(expression, nameof(expression));

        // Traduz a árvore de expressão em SQL + parâmetros
        var translation = _translator.Translate(expression);

        var sql = translation.Sql ?? string.Empty;
        var paramObj = translation.Params; // anonymous object / DynamicParameters / null

        static MethodInfo FindDapperMethodWithOptionalTail(
            string name,
            int genericArgCount)
        {
            var sqlMapper = typeof(Dapper.SqlMapper);

            // Queremos o método cujo prefixo de parâmetros seja:
            // (IDbConnection, string, object)
            // e o resto (se existir) seja OPTIONAL.
            var candidates = sqlMapper
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Where(m => m.Name == name
                    && m.IsGenericMethodDefinition
                    && m.GetGenericArguments().Length == genericArgCount)
                .Select(m => new { Method = m, Params = m.GetParameters() })
                .Where(x => x.Params.Length >= 3
                    && typeof(IDbConnection).IsAssignableFrom(x.Params[0].ParameterType)
                    && x.Params[1].ParameterType == typeof(string)
                    && (x.Params[2].ParameterType == typeof(object)
                        || x.Params[2].ParameterType.IsAssignableFrom(typeof(object)))
                    && x.Params.Skip(3).All(p => p.IsOptional))
                .ToList();

            if (candidates.Count == 0)
                throw new InvalidOperationException(
                    $"Não encontrei overload de Dapper.SqlMapper.{name} com prefixo (IDbConnection, string, object) e cauda opcional.");

            // Se houver mais de um, escolhe o mais “completo” (mais params), porque é o padrão do Dapper.
            return candidates
                .OrderByDescending(x => x.Params.Length)
                .First()
                .Method;
        }

        static object?[] BuildInvokeArgs(
            ParameterInfo[] ps,
            IDbConnection cnn,
            string sql,
            object? paramObj)
        {
            // Preenche os 3 primeiros e o resto com Missing (pra usar defaults dos opcionais)
            var args = new object?[ps.Length];
            args[0] = cnn;
            args[1] = sql;
            args[2] = paramObj ?? new { };

            for (int i = 3; i < ps.Length; i++)
                args[i] = Type.Missing;

            return args;
        }

        // IEnumerable (mas não string)
        if (typeof(IEnumerable).IsAssignableFrom(typeof(TResult))
            && typeof(TResult) != typeof(string))
        {
            var elementType = typeof(TResult).IsGenericType
                ? typeof(TResult).GetGenericArguments().First()
                : typeof(object);

            var def = FindDapperMethodWithOptionalTail("Query", genericArgCount: 1);
            var mi = def.MakeGenericMethod(elementType);

            var invokeArgs = BuildInvokeArgs(mi.GetParameters(), _cnn, sql, paramObj);
            var data = mi.Invoke(null, invokeArgs)!;

            return (TResult)data;
        }
        else
        {
            var def = FindDapperMethodWithOptionalTail("QuerySingleOrDefault", genericArgCount: 1);
            var mi = def.MakeGenericMethod(typeof(TResult));

            var invokeArgs = BuildInvokeArgs(mi.GetParameters(), _cnn, sql, paramObj);
            var data = mi.Invoke(null, invokeArgs);

            return (TResult)data!;
        }
    }

    // Implementação não-genérica, exigida pela interface
    object IQueryProvider.Execute(Expression expression)
        => Execute<object>(expression);
}
