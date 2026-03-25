namespace DbSqlLikeMem;

internal static class SqlDialectScalarFunctionRegistryExtensions
{
    internal static bool TryGetScalarFunctionDefinition(
        this ISqlDialect dialect,
        string name,
        out DbScalarFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));

        return dialect.ScalarFunctions.TryGetValue(name, out definition)
            && definition is not null;
    }

    internal static bool TryGetScalarFunctionDefinition(
        this ISqlDialect dialect,
        FunctionCallExpr call,
        out DbScalarFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        if (call.ResolvedScalarFunction is not null)
        {
            definition = call.ResolvedScalarFunction;
            return true;
        }

        return dialect.TryGetScalarFunctionDefinition(call.Name, out definition);
    }

    internal static bool TryGetScalarFunctionDefinition(
        this ISqlDialect dialect,
        CallExpr call,
        out DbScalarFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        if (call.ResolvedScalarFunction is not null)
        {
            definition = call.ResolvedScalarFunction;
            return true;
        }

        return dialect.TryGetScalarFunctionDefinition(call.Name, out definition);
    }

    internal static FunctionCallExpr BindScalarFunctionDefinition(
        this FunctionCallExpr call,
        DbScalarFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        return definition is null
            ? call
            : call with { ResolvedScalarFunction = definition };
    }

    internal static CallExpr BindScalarFunctionDefinition(
        this CallExpr call,
        DbScalarFunctionDef? definition)
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

        return dialect.TryGetScalarFunctionDefinition(call.Name, out var definition)
            ? call with { ResolvedScalarFunction = definition }
            : call;
    }

    internal static CallExpr BindScalarFunctionDefinition(
        this CallExpr call,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        return dialect.TryGetScalarFunctionDefinition(call.Name, out var definition)
            ? call with { ResolvedScalarFunction = definition }
            : call;
    }

    internal static FunctionCallExpr BindTableFunctionDefinition(
        this FunctionCallExpr call,
        DbTableFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(call, nameof(call));

        return definition is null
            ? call
            : call with { ResolvedTableFunction = definition };
    }

    internal static bool TryEvalZeroArgTemporalFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(fn, nameof(fn));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = evalArg;

        result = null;
        if (fn.Args.Count != 0)
            return false;

        return SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, fn.Name, out result)
            || SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, fn.Name, out result);
    }

    internal static bool AllowsTemporalIdentifier(
        this ISqlDialect dialect,
        string name)
    {
        if (dialect.TryGetScalarFunctionDefinition(name, out var definition)
            && definition!.TemporalKind is not null)
        {
            return definition.AllowsIdentifier;
        }

        return dialect.TemporalFunctionIdentifierNames.Any(token => token.Equals(name, StringComparison.OrdinalIgnoreCase));
    }

    internal static bool AllowsTemporalCall(
        this ISqlDialect dialect,
        string name)
    {
        if (dialect.TryGetScalarFunctionDefinition(name, out var definition)
            && definition!.TemporalKind is not null)
        {
            return definition.AllowsCall;
        }

        return dialect.TemporalFunctionCallNames.Any(token => token.Equals(name, StringComparison.OrdinalIgnoreCase));
    }

    internal static bool TryGetTemporalFunctionKind(
        this ISqlDialect dialect,
        string name,
        out SqlTemporalFunctionKind kind)
    {
        if (dialect.TryGetScalarFunctionDefinition(name, out var definition)
            && definition!.TemporalKind is SqlTemporalFunctionKind registeredKind)
        {
            kind = registeredKind;
            return true;
        }

        return dialect.TemporalFunctionNames.TryGetValue(name, out kind);
    }

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        params DbScalarFunctionParameterDef[] parameters)
        => dialect.AddScalarFunction(name, returnTypeSql, SqlFunctionBodyFactory.Identity(), parameters);

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        DbScalarFunctionDef definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        dialect.ScalarFunctions[definition.Name] = definition;
    }

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        Func<SqlExpr, object> fnBody,
        params DbScalarFunctionParameterDef[] parameters)
        => dialect.AddScalarFunction(name, returnTypeSql, fnBody, SqlScalarFunctionUsageKind.Call, null, parameters);

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params DbScalarFunctionParameterDef[] parameters)
        => dialect.AddScalarFunction(name, returnTypeSql, executionHandler, SqlScalarFunctionUsageKind.Call, null, parameters);

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        Func<SqlExpr, object> fnBody,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params DbScalarFunctionParameterDef[] parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(fnBody, nameof(fnBody));

        dialect.ScalarFunctions[name] = new DbScalarFunctionDef(
            name,
            returnTypeSql.Trim(),
            parameters,
            fnBody,
            usageKind,
            temporalKind);
    }

    internal static void AddScalarFunction(
        this ISqlDialect dialect,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params DbScalarFunctionParameterDef[] parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        dialect.ScalarFunctions[name] = new DbScalarFunctionDef(
            name,
            returnTypeSql.Trim(),
            parameters,
            SqlFunctionBodyFactory.Identity(),
            usageKind,
            temporalKind)
        {
            AstExecutor = executionHandler
        };
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        string name,
        string returnTypeSql,
        params DbScalarFunctionParameterDef[] parameters)
    {
        if (supported)
            dialect.AddScalarFunction(name, returnTypeSql, parameters);
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        DbScalarFunctionDef definition)
    {
        if (supported)
            dialect.AddScalarFunction(definition);
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        string name,
        string returnTypeSql,
        Func<SqlExpr, object> fnBody,
        params DbScalarFunctionParameterDef[] parameters)
    {
        if (supported)
            dialect.AddScalarFunction(name, returnTypeSql, fnBody, parameters);
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params DbScalarFunctionParameterDef[] parameters)
    {
        if (supported)
            dialect.AddScalarFunction(name, returnTypeSql, executionHandler, parameters);
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        string name,
        string returnTypeSql,
        Func<SqlExpr, object> fnBody,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params DbScalarFunctionParameterDef[] parameters)
    {
        if (supported)
            dialect.AddScalarFunction(name, returnTypeSql, fnBody, usageKind, temporalKind, parameters);
    }

    internal static void AddScalarFunctionIf(
        this ISqlDialect dialect,
        bool supported,
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params DbScalarFunctionParameterDef[] parameters)
    {
        if (supported)
            dialect.AddScalarFunction(name, returnTypeSql, executionHandler, usageKind, temporalKind, parameters);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string returnTypeSql,
        Func<SqlExpr, object>? fnBody,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(names, nameof(names));

        var body = fnBody ?? SqlFunctionBodyFactory.Identity();
        foreach (var name in names)
            dialect.AddScalarFunction(name, returnTypeSql, body);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params string[] names)
        => dialect.AddScalarFunctions(returnTypeSql, executionHandler, SqlScalarFunctionUsageKind.Call, null, names);

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string returnTypeSql,
        Func<SqlExpr, object>? fnBody,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(names, nameof(names));

        var body = fnBody ?? SqlFunctionBodyFactory.Identity();
        foreach (var name in names)
            dialect.AddScalarFunction(name, returnTypeSql, body, usageKind, temporalKind);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(names, nameof(names));
        ArgumentNullExceptionCompatible.ThrowIfNull(executionHandler, nameof(executionHandler));

        foreach (var name in names)
            dialect.AddScalarFunction(name, returnTypeSql, executionHandler, usageKind, temporalKind);
    }

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        string returnTypeSql,
        params string[] names)
        => dialect.AddScalarFunctions(returnTypeSql, (Func<SqlExpr, object>?)null, names);

    internal static void AddScalarFunctions(
        this ISqlDialect dialect,
        DbScalarFunctionDef definition,
        params string[] names)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));
        ArgumentNullExceptionCompatible.ThrowIfNull(names, nameof(names));

        foreach (var name in names)
        {
            dialect.AddScalarFunction(
                name.Equals(definition.Name, StringComparison.OrdinalIgnoreCase)
                    ? definition
                    : definition with { Name = name });
        }
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        string returnTypeSql,
        Func<SqlExpr, object>? fnBody,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(returnTypeSql, fnBody, names);
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(returnTypeSql, executionHandler, names);
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        string returnTypeSql,
        Func<SqlExpr, object>? fnBody,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(returnTypeSql, fnBody, usageKind, temporalKind, names);
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executionHandler,
        SqlScalarFunctionUsageKind usageKind,
        SqlTemporalFunctionKind? temporalKind,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(returnTypeSql, executionHandler, usageKind, temporalKind, names);
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        string returnTypeSql,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(returnTypeSql, names);
    }

    internal static void AddScalarFunctionsIf(
        this ISqlDialect dialect,
        bool supported,
        DbScalarFunctionDef definition,
        params string[] names)
    {
        if (supported)
            dialect.AddScalarFunctions(definition, names);
    }
}
