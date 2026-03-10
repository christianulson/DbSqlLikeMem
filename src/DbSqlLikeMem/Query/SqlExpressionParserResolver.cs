using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class SqlExpressionParserResolver
{
    private static readonly ConcurrentDictionary<Type, Func<string, object, SqlExpr>> ParseWhereDelegateCache = new();

    public static SqlExpr ParseWhere(string raw, object dialect)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(raw, nameof(raw));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var dialectType = dialect.GetType();
        var parserDelegate = ParseWhereDelegateCache.GetOrAdd(dialectType, CreateParseWhereDelegate);
        return parserDelegate(raw, dialect);
    }

    private static Func<string, object, SqlExpr> CreateParseWhereDelegate(Type dialectType)
    {
        var methodInfo = typeof(SqlExpressionParser)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m =>
            {
                if (m.Name != nameof(SqlExpressionParser.ParseWhere))
                    return false;

                var parameters = m.GetParameters();
                return parameters.Length == 2
                    && parameters[0].ParameterType == typeof(string)
                    && parameters[1].ParameterType.IsAssignableFrom(dialectType);
            });

        if (methodInfo is null)
        {
            throw new MissingMethodException(
                $"{nameof(SqlExpressionParser)}.{nameof(SqlExpressionParser.ParseWhere)}(string, {dialectType.Name}) nao encontrado.");
        }

        return (raw, dialectInstance) =>
        {
            try
            {
                return (SqlExpr)methodInfo.Invoke(null, [raw, dialectInstance])!;
            }
            catch (TargetInvocationException tie) when (tie.InnerException is not null)
            {
                throw tie.InnerException;
            }
        };
    }
}
