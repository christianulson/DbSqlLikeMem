using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryJsonExtractionFunctionEvaluator
{
    internal static bool TryEvalJsonAccessShimFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "__JSON_ACCESS_JSON", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!AstQueryExecutionRuntimeHelper.TryGetJsonAndPathArguments(evalArg, out var json, out var path))
        {
            result = null;
            return true;
        }

        var normalizedPath = NormalizeJsonPath(path!);
        var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, normalizedPath);
        if (value is JsonElement element && element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            result = null;
            return true;
        }

        result = string.Equals(fn.Name, "__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)
            ? value?.ToString()
            : value;
        return true;
    }

    private static string NormalizeJsonPath(string path)
    {
        var trimmed = path.Trim();
        if (trimmed.Length == 0)
            return trimmed;

        if (trimmed[0] == '$'
            || trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return trimmed;

        if (trimmed.StartsWith("{", StringComparison.Ordinal) && trimmed.EndsWith("}", StringComparison.Ordinal))
        {
            var inner = trimmed[1..^1];
            if (!string.IsNullOrWhiteSpace(inner))
            {
                var parts = inner
                    .Split(',')
                    .Select(part => part.Trim())
                    .Where(part => part.Length > 0)
                    .Select(part => part.Trim('"'))
                    .Where(part => part.Length > 0)
                    .ToArray();

                if (parts.Length > 0)
                    return "$." + string.Join(".", parts);
            }
        }

        if (int.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index) && index >= 0)
            return $"$[{index}]";

        if (IsSimpleJsonPropertyName(trimmed))
            return "$." + trimmed;

        return trimmed;
    }

    private static bool IsSimpleJsonPropertyName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        if (!(char.IsLetter(value[0]) || value[0] == '_'))
            return false;

        for (var i = 1; i < value.Length; i++)
        {
            var ch = value[i];
            if (!(char.IsLetterOrDigit(ch) || ch == '_'))
                return false;
        }

        return true;
    }

    internal static bool TryEvalJsonExtractionFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        context.EnsureJsonExtractionSupported(fn.Name);
        var json = evalArg(0);
        if (IsNullish(json))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && fn.Args.Count == 1)
        {
            result = TryEvalJsonQueryWithoutPath(json!);
            return true;
        }

        var path = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(path))
        {
            result = null;
            return true;
        }

        result = TryEvalJsonExtractionValue(context, fn, json!, path!);
        return true;
    }

    internal static void EnsureJsonExtractionSupported(
        this QueryExecutionContext context,
        string functionName)
    {
        if (context.Dialect.TryGetScalarFunctionDefinition(functionName, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw context.NotSupported(functionName.ToUpperInvariant());
        }

        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && (!context.Dialect.TryGetScalarFunctionDefinition("JSON_EXTRACT", out var jsonExtractDefinition)
                || jsonExtractDefinition is null
                || !jsonExtractDefinition.AllowsCall))
            throw context.NotSupported("JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && (!context.Dialect.TryGetScalarFunctionDefinition("JSON_QUERY", out var jsonQueryDefinition)
                || jsonQueryDefinition is null
                || !jsonQueryDefinition.AllowsCall))
            throw context.NotSupported("JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && (!context.Dialect.TryGetScalarFunctionDefinition("JSON_VALUE", out var jsonValueDefinition)
                || jsonValueDefinition is null
                || !jsonValueDefinition.AllowsCall))
            throw context.NotSupported("JSON_VALUE");
    }

    internal static object? TryEvalJsonExtractionValue(QueryExecutionContext context, FunctionCallExpr fn, object json, string path)
    {
        try
        {
            if (string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json, path, out var element))
                    return null;

                return element.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? element.GetRawText()
                    : null;
            }

            if (string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase))
            {
                if (context.Dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
                {
                    if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json, path, out var element))
                        return null;

                    return QueryJsonFunctionHelper.ConvertJsonElementToSqlServerJsonValue(element);
                }

                var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json, path);
                return QueryJsonFunctionHelper.ApplyJsonValueReturningClause(fn, value);
            }

            return QueryJsonFunctionHelper.TryReadJsonPathValue(json, path);
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            AstQueryExecutionRuntimeHelper.LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    internal static object? TryEvalJsonQueryWithoutPath(object json)
    {
        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
            return null;

        return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }
}
