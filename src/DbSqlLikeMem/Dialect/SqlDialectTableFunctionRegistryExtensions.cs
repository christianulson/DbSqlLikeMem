namespace DbSqlLikeMem;

internal static class SqlDialectTableFunctionRegistryExtensions
{
    internal static bool TryGetTableFunctionDefinition(
        this ISqlDialect dialect,
        string name,
        out DbTableFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));

        return dialect.TableFunctions.TryGetValue(name, out definition)
            && definition is not null;
    }

    internal static bool TryGetTableFunctionDefinition(
        this ISqlDialect dialect,
        FunctionCallExpr call,
        out DbTableFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        if (call.ResolvedTableFunction is not null)
        {
            definition = call.ResolvedTableFunction;
            return true;
        }

        return dialect.TryGetTableFunctionDefinition(call.Name, out definition);
    }

    internal static void AddTableFunction(
        this ISqlDialect dialect,
        string name,
        int minArguments,
        int maxArguments)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("name vazio", nameof(name));

        if (minArguments < 0)
            throw new ArgumentOutOfRangeException(nameof(minArguments));

        if (maxArguments < minArguments)
            throw new ArgumentOutOfRangeException(nameof(maxArguments));

        dialect.TableFunctions[name] = new DbTableFunctionDef(name, minArguments, maxArguments);
    }

    internal static void AddTableFunction(
        this ISqlDialect dialect,
        DbTableFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.TableFunctions[definition.Name] = definition;
    }

    internal static void AddTableFunctions(
        this ISqlDialect dialect,
        params DbTableFunctionDef[] definitions)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definitions, nameof(definitions));

        foreach (var definition in definitions)
            dialect.AddTableFunction(definition);
    }
}
