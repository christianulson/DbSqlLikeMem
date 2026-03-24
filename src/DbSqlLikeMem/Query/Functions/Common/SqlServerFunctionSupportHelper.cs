namespace DbSqlLikeMem;

internal static class SqlServerFunctionSupportHelper
{
    public static void EnsureSupport(FunctionCallExpr fn, ISqlDialect dialect)
    {
        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return;

        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && definition.AllowsCall)
        {
            return;
        }

        if (definition is null
            && dialect.TryGetScalarFunctionDefinition(fn, out definition)
            && definition is not null
            && definition.AllowsCall)
        {
            return;
        }

        throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
    }
}
