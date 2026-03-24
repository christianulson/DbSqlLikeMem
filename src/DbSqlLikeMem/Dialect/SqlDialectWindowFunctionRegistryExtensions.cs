namespace DbSqlLikeMem;

internal static class SqlDialectWindowFunctionRegistryExtensions
{
    internal static bool TryGetWindowFunctionDefinition(
        this ISqlDialect dialect,
        WindowFunctionExpr windowFunction,
        out DbWindowFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunction, nameof(windowFunction));

        if (windowFunction.ResolvedWindowFunction is not null)
        {
            definition = windowFunction.ResolvedWindowFunction;
            return true;
        }

        return dialect.TryGetWindowFunctionDefinition(windowFunction.Name, out definition);
    }

    internal static bool TryGetWindowFunctionDefinition(
        this ISqlDialect dialect,
        string name,
        out DbWindowFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));

        return dialect.WindowFunctions.TryGetValue(name, out definition)
            && definition is not null;
    }

    internal static void AddWindowFunction(
        this ISqlDialect dialect,
        string name,
        int minArguments,
        int maxArguments,
        bool requiresOrderBy)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("name vazio", nameof(name));

        if (minArguments < 0)
            throw new ArgumentOutOfRangeException(nameof(minArguments));

        if (maxArguments < minArguments)
            throw new ArgumentOutOfRangeException(nameof(maxArguments));

        dialect.WindowFunctions[name] = new DbWindowFunctionDef(name, minArguments, maxArguments, requiresOrderBy);
    }

    internal static void AddWindowFunction(
        this ISqlDialect dialect,
        DbWindowFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.WindowFunctions[definition.Name] = definition;
    }

    internal static void AddWindowFunctions(
        this ISqlDialect dialect,
        params DbWindowFunctionDef[] definitions)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definitions, nameof(definitions));

        foreach (var definition in definitions)
            dialect.AddWindowFunction(definition);
    }

    internal static void AddWindowFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        params DbWindowFunctionDef[] definitions)
    {
        if (supported)
            dialect.AddWindowFunctions(definitions);
    }

    internal static WindowFunctionExpr BindWindowFunctionDefinition(
        this WindowFunctionExpr windowFunction,
        DbWindowFunctionDef? definition)
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

        return dialect.TryGetWindowFunctionDefinition(windowFunction.Name, out var definition)
            ? windowFunction with { ResolvedWindowFunction = definition }
            : windowFunction;
    }
}
