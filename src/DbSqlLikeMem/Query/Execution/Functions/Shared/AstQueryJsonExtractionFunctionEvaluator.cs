using System.Text.Json;
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

        var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
        result = string.Equals(fn.Name, "__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)
            ? value?.ToString()
            : value;
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

        result = TryEvalJsonExtractionValue(fn, json!, path!);
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

    internal static object? TryEvalJsonExtractionValue(FunctionCallExpr fn, object json, string path)
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

            var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json, path);
            return string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                ? QueryJsonFunctionHelper.ApplyJsonValueReturningClause(fn, value)
                : value;
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
