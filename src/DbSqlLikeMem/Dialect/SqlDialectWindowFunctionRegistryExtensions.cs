namespace DbSqlLikeMem;

internal static class SqlDialectWindowFunctionRegistryExtensions
{
    internal static WindowFunctionExpr BindWindowFunctionDefinition(
        this WindowFunctionExpr windowFunction,
        DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunction, nameof(windowFunction));

        return definition is null
            ? windowFunction
            : windowFunction with { ResolvedWindowFunction = definition };
    }

    internal static WindowFunctionExpr BindWindowFunctionDefinition(
        this WindowFunctionExpr windowFunction,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunction, nameof(windowFunction));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        return dialect.TryGetWindowFunctionDefinition(windowFunction, out var definition)
            ? windowFunction with { ResolvedWindowFunction = definition }
            : windowFunction;
    }

    internal static void AddWindowFunction(
        this ISqlDialect dialect,
        DbFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.Functions.Add(definition);
    }

    internal static void AddWindowFunction(
        this ISqlDialect dialect,
        string name,
        int minArguments,
        int maxArguments,
        bool requiresOver = true,
        bool requiresOrderBy = false)
        => dialect.AddWindowFunction(
            new DbFunctionDef(name, minArguments, maxArguments, requiresOver, requiresOrderBy));

    internal static void AddWindowFunctions(
        this ISqlDialect dialect,
        params DbFunctionDef[] definitions)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definitions, nameof(definitions));

        foreach (var definition in definitions)
            dialect.AddWindowFunction(definition);
    }

    internal static void AddWindowFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        params DbFunctionDef[] definitions)
    {
        if (supported)
            dialect.AddWindowFunctions(definitions);
    }
}
