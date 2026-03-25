namespace DbSqlLikeMem;

internal static class SqlServerFunctionSupportHelper
{
    public static void EnsureSupport(FunctionCallExpr fn, QueryExecutionContext context)
    {
        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && definition.AllowsCall)
        {
            return;
        }

        if (definition is null
            && context.Dialect.TryGetScalarFunctionDefinition(fn, out definition)
            && definition is not null
            && definition.AllowsCall)
        {
            return;
        }

        throw SqlUnsupported.ForDialect(context.Dialect, fn.Name.ToUpperInvariant());
    }
}
