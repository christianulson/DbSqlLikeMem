namespace DbSqlLikeMem;

internal static class SqlDialectScalarFunctionRegistryExtensions
{
    internal static bool TryEvalZeroArgTemporalFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(context, nameof(context));
        ArgumentNullExceptionCompatible.ThrowIfNull(fn, nameof(fn));
        _ = evalArg;

        result = null;
        if (fn.Args.Count != 0)
            return false;

        return context.TryEvaluateZeroArgCall(fn.Name, out result)
            || context.TryEvaluateZeroArgIdentifier(fn.Name, out result);
    }

    internal static FunctionCallExpr BindScalarFunctionDefinition(
        this FunctionCallExpr call,
        DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        return definition is null
            ? call
            : call with { ResolvedScalarFunction = definition };
    }

    internal static CallExpr BindScalarFunctionDefinition(
        this CallExpr call,
        DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        return definition is null
            ? call
            : call with { ResolvedScalarFunction = definition };
    }

    internal static FunctionCallExpr BindScalarFunctionDefinition(
        this FunctionCallExpr call,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        return dialect.TryGetScalarFunctionDefinition(call, out var definition)
            ? call with { ResolvedScalarFunction = definition }
            : call;
    }

    internal static CallExpr BindScalarFunctionDefinition(
        this CallExpr call,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        return dialect.TryGetScalarFunctionDefinition(call, out var definition)
            ? call with { ResolvedScalarFunction = definition }
            : call;
    }

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        DbFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.Functions.Add(definition);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        DbFunctionDef definition,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        //dialect.AddScalarFunction(definition);
        foreach (var name in names)
            dialect.AddScalarFunction(definition with { Name = name });
    }

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        DbInvocationStyle invocationStyle,
        SqlTemporalFunctionKind? temporalKind,
        params DbFunctionParameterDef[] parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        dialect.AddScalarFunctionWithHandler(
            name,
            returnTypeSql,
            executionHandler,
            invocationStyle,
            temporalKind,
            parameters);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string? returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        DbInvocationStyle invocationStyle,
        SqlTemporalFunctionKind? temporalKind,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        foreach (var name in names)
            dialect.AddScalarFunctionWithHandler(name, returnTypeSql ?? string.Empty, executionHandler, invocationStyle, temporalKind);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string? returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        DbInvocationStyle invocationStyle,
        params string[] names)
        => dialect.AddScalarFunctions(
            returnTypeSql,
            executionHandler,
            invocationStyle,
            null,
            names);

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params DbFunctionParameterDef[] parameters)
        => dialect.AddScalarFunctionWithHandler(
            name,
            returnTypeSql,
            executionHandler,
            DbInvocationStyle.Call,
            null,
            parameters);

    private static void AddScalarFunctionWithHandler(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        DbInvocationStyle invocationStyle,
        SqlTemporalFunctionKind? temporalKind,
        params DbFunctionParameterDef[] parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        var definition = temporalKind is SqlTemporalFunctionKind temporal
            ? DbFunctionDef.CreateTemporal(name, returnTypeSql.Trim(), temporal, invocationStyle)
            : invocationStyle switch
            {
                DbInvocationStyle.Identifier => DbFunctionDef.CreateIdentifier(name, returnTypeSql.Trim()),
                _ when invocationStyle == (DbInvocationStyle.Call | DbInvocationStyle.Identifier) => DbFunctionDef.CreateCallOrIdentifier(name, returnTypeSql.Trim()),
                _ => DbFunctionDef.CreateScalar(name, returnTypeSql.Trim(), invocationStyle: invocationStyle)
            };

        dialect.AddScalarFunction(definition with
        {
            Parameters = parameters,
            AstExecutor = executionHandler
        });
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string? returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        foreach (var name in names)
            dialect.AddScalarFunction(name, returnTypeSql ?? string.Empty, executionHandler);
    }

}
