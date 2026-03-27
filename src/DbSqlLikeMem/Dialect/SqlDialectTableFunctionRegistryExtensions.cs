namespace DbSqlLikeMem;

internal static class SqlDialectTableFunctionRegistryExtensions
{
    internal static FunctionCallExpr BindTableFunctionDefinition(
        this FunctionCallExpr call,
        DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        return definition is null
            ? call
            : call with { ResolvedTableFunction = definition };
    }

    internal static FunctionCallExpr BindTableFunctionDefinition(
        this FunctionCallExpr call,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        return dialect.TryGetTableFunctionDefinition(call, out var definition)
            ? call with { ResolvedTableFunction = definition }
            : call;
    }

    internal static void AddTableFunction(
        this ISqlDialect dialect,
        DbFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.Functions.Add(definition);
    }

    internal static void AddTableFunction(
        this ISqlDialect dialect,
        string name,
        int minArguments,
        int maxArguments,
        AstQueryTableFunctionHandler? astExecutor = null)
    {
        var definition = DbFunctionDef.CreateTable(
            name,
            signatures: new DbFunctionSignature([], minArguments, maxArguments)) with
        {
            TableExecutor = astExecutor
        };

        dialect.AddTableFunction(definition);
    }

    internal static void AddTableFunctions(
        this ISqlDialect dialect,
        params DbFunctionDef[] definitions)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definitions, nameof(definitions));

        foreach (var definition in definitions)
            dialect.AddTableFunction(definition);
    }

    internal static void AddTableFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        params DbFunctionDef[] definitions)
    {
        if (supported)
            dialect.AddTableFunctions(definitions);
    }
}
