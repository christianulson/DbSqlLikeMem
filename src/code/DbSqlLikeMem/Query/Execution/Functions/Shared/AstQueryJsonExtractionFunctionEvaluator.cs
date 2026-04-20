using System.Collections.Concurrent;

using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryJsonExtractionFunctionEvaluator
{
    private const int NormalizedJsonPathCacheSoftLimit = 256;
    private static readonly ConcurrentDictionary<string, string> _normalizedJsonPathCache = new(StringComparer.Ordinal);

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

        var normalizedPath = GetNormalizedJsonPath(path!);
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

    private static string GetNormalizedJsonPath(string path)
    {
        if (_normalizedJsonPathCache.TryGetValue(path, out var cached))
            return cached;

        var normalized = NormalizeJsonPath(path);
        if (_normalizedJsonPathCache.Count < NormalizedJsonPathCacheSoftLimit)
            _normalizedJsonPathCache.TryAdd(path, normalized);

        return normalized;
    }

    private static string NormalizeJsonPath(string path)
    {
        var trimmed = path.AsSpan().Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        if (trimmed[0] == '$'
            || trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return trimmed.ToString();

        if (trimmed.StartsWith("{", StringComparison.Ordinal) && trimmed.EndsWith("}", StringComparison.Ordinal))
        {
            var inner = trimmed[1..^1];
            var hasContent = false;
            for (var i = 0; i < inner.Length; i++)
            {
                if (!char.IsWhiteSpace(inner[i]))
                {
                    hasContent = true;
                    break;
                }
            }

            if (hasContent)
            {
                var builder = new System.Text.StringBuilder(inner.Length + 2);
                var added = false;
                var start = 0;
                while (start < inner.Length)
                {
                    var remaining = inner[start..];
                    var comma = remaining.IndexOf(',');
                    var segment = comma >= 0
                        ? remaining[..comma]
                        : remaining;

                    segment = segment.Trim();
                    while (segment.Length > 0 && segment[0] == '"')
                        segment = segment[1..];
                    while (segment.Length > 0 && segment[^1] == '"')
                        segment = segment[..^1];

                    if (segment.Length > 0)
                    {
                        if (added)
                            builder.Append('.');

                        builder.AppendSpan(segment);
                        added = true;
                    }

                    if (comma < 0)
                        break;

                    start += comma + 1;
                }

                if (added)
                    return "$." + builder;
            }
        }

        if (ReadOnlySpanCompatibility.TryParseInt32(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index) && index >= 0)
            return $"$[{index}]";

        if (IsSimpleJsonPropertyName(trimmed))
            return "$." + trimmed.ToString();

        return trimmed.ToString();
    }

    private static bool IsSimpleJsonPropertyName(ReadOnlySpan<char> value)
    {
        if (value.Length == 0)
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
                var lookup = QueryJsonFunctionHelper.LookupJsonPath(json, path);
                if (!lookup.Success)
                {
                    if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                        throw new InvalidOperationException($"JSON_QUERY path '{path}' is invalid in the mock.");

                    if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                        throw new InvalidOperationException($"JSON_QUERY strict path '{path}' was not found in the JSON payload.");

                    return null;
                }

                return lookup.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? lookup.Value.GetRawText()
                    : lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict
                        ? throw new InvalidOperationException($"JSON_QUERY strict path '{path}' was not found in the JSON payload.")
                        : null;
            }

            if (string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase))
            {
                if (context.Dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
                {
                    var lookup = QueryJsonFunctionHelper.LookupJsonPath(json, path);
                    if (!lookup.Success)
                    {
                        if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                            throw new InvalidOperationException($"JSON_VALUE path '{path}' is invalid in the mock.");

                        if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                            throw new InvalidOperationException($"JSON_VALUE strict path '{path}' was not found in the JSON payload.");

                        return null;
                    }

                    var sqlServerValue = QueryJsonFunctionHelper.ConvertJsonElementToSqlServerJsonValue(lookup.Value);
                    if (sqlServerValue is null
                        && lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict
                        && lookup.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        throw new InvalidOperationException($"JSON_VALUE strict path '{path}' was not found in the JSON payload.");

                    if (sqlServerValue is string text && text.Length > 4000)
                    {
                        if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                            throw new InvalidOperationException($"JSON_VALUE strict path '{path}' exceeds the 4000 character limit.");

                        return null;
                    }

                    return sqlServerValue;
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
