using System.Text.Json.Nodes;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryMySqlJsonFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalMySqlJsonUtilityFunction(context, fn, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalMySqlJsonUtilityFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (string.Equals(fn.Name, "JSON_OBJECT", StringComparison.OrdinalIgnoreCase))
        {
            return AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonQuoteFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_PRETTY", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonPrettyFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_VALID", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonValidFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_TYPE", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonTypeFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonLengthFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_KEYS", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonKeysFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_SET", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonSetFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_REMOVE", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonRemoveFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_INSERT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonInsertReplaceFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_CONTAINS", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonContainsFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_CONTAINS_PATH", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonContainsPathFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_SEARCH", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonSearchFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_MERGE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonMergeFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_APPEND", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_ARRAY_APPEND", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonAppendFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_ARRAY_INSERT", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonArrayInsertFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_STORAGE_SIZE", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonStorageSizeFunction(context, fn, evalArg, out result);
        }

        if (string.Equals(fn.Name, "JSON_OVERLAPS", StringComparison.OrdinalIgnoreCase))
        {
            return TryEvalJsonOverlapsFunction(context, fn, evalArg, out result);
        }

        result = null;
        return false;
    }

    private static bool TryEvalJsonQuoteFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = JsonSerializer.Serialize(text);
        return true;
    }

    private static bool TryEvalJsonPrettyFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
        {
            result = null;
            return true;
        }

        result = JsonNode
            .Parse(element.GetRawText())
            ?.ToJsonString(new JsonSerializerOptions { WriteIndented = true })
            .Replace("\r\n", "\n");
        return true;
    }

    private static bool TryEvalJsonValidFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            result = 0;
            return true;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(text!, out _);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    private static bool TryEvalJsonTypeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
        {
            result = null;
            return true;
        }

        result = element.ValueKind switch
        {
            JsonValueKind.Object => "OBJECT",
            JsonValueKind.Array => "ARRAY",
            JsonValueKind.String => "STRING",
            JsonValueKind.Number => element.TryGetInt64(out _)
                ? "INTEGER"
                : "DOUBLE",
            JsonValueKind.True => "BOOLEAN",
            JsonValueKind.False => "BOOLEAN",
            JsonValueKind.Null => SqlConst.NULL,
            _ => null
        };
        return true;
    }

    private static bool TryEvalJsonLengthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count > 1)
        {
            var path = evalArg(1)?.ToString();
            if (!string.IsNullOrWhiteSpace(path)
                && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
            {
                element = pathElement;
            }
            else if (!string.IsNullOrWhiteSpace(path))
            {
                result = null;
                return true;
            }
        }

        result = element.ValueKind switch
        {
            JsonValueKind.Array => element.GetArrayLength(),
            JsonValueKind.Object => element.EnumerateObject().Count(),
            _ => 1
        };
        return true;
    }

    private static bool TryEvalJsonKeysFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count > 1)
        {
            var path = evalArg(1)?.ToString();
            if (!string.IsNullOrWhiteSpace(path)
                && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
            {
                element = pathElement;
            }
            else if (!string.IsNullOrWhiteSpace(path))
            {
                result = null;
                return true;
            }
        }

        if (element.ValueKind != JsonValueKind.Object)
        {
            result = null;
            return true;
        }

        var keys = element.EnumerateObject().Select(static prop => (object?)prop.Name).ToArray();
        result = AstQueryJsonSharedFunctionEvaluator.BuildJsonArray(keys);
        return true;
    }

    private static bool TryEvalJsonSetFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
            throw new InvalidOperationException("JSON_SET() espera um JSON seguido de pares path/valor.");

        var json = evalArg(0);
        if (IsNullish(json) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i += 2)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonPathTokens(path!, out var tokens))
            {
                result = null;
                return true;
            }

            var value = evalArg(i + 1);
            if (!AstQueryJsonPathFunctionEvaluator.TrySetJsonPathValue(ref root, tokens, value))
            {
                result = null;
                return true;
            }
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonRemoveFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_REMOVE() espera um JSON e ao menos um path.");

        var json = evalArg(0);
        if (IsNullish(json) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonPathTokens(path!, out var tokens))
            {
                result = null;
                return true;
            }

            AstQueryJsonPathFunctionEvaluator.TryRemoveJsonPathValue(root, tokens);
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonInsertReplaceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        var isInsert = string.Equals(fn.Name, "JSON_INSERT", StringComparison.OrdinalIgnoreCase);
        if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera um JSON seguido de pares path/valor.");

        var json = evalArg(0);
        if (IsNullish(json) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i += 2)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonPathTokens(path!, out var tokens))
            {
                result = null;
                return true;
            }

            var value = evalArg(i + 1);
            var exists = AstQueryJsonPathFunctionEvaluator.TryGetJsonNodeAtPath(root, tokens, out _);
            if (isInsert && exists)
                continue;

            if (!isInsert && !exists)
                continue;

            if (!AstQueryJsonPathFunctionEvaluator.TrySetJsonPathValue(ref root, tokens, value))
            {
                result = null;
                return true;
            }
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonContainsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_CONTAINS() espera um JSON e um candidato.");

        var targetValue = evalArg(0);
        var candidateValue = evalArg(1);
        if (IsNullish(targetValue) || IsNullish(candidateValue))
        {
            result = null;
            return true;
        }

        if (targetValue is null || candidateValue is null
            || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(targetValue, out var targetElement)
            || !TryParseJsonCandidate(candidateValue, out var candidateElement))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count > 2)
        {
            var path = evalArg(2)?.ToString();
            if (string.IsNullOrWhiteSpace(path)
                || !QueryJsonFunctionHelper.TryReadJsonPathElement(targetElement, path!, out var pathElement))
            {
                result = 0;
                return true;
            }

            result = JsonContains(pathElement, candidateElement) ? 1 : 0;
            return true;
        }

        result = JsonContains(targetElement, candidateElement) ? 1 : 0;
        return true;
    }

    private static bool TryEvalJsonContainsPathFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("JSON_CONTAINS_PATH() espera um JSON, modo e paths.");

        var json = evalArg(0);
        var mode = evalArg(1)?.ToString() ?? string.Empty;
        if (IsNullish(json))
        {
            result = null;
            return true;
        }

        if (json is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(json, out var element))
        {
            result = null;
            return true;
        }

        var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
        var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
        if (!requireAll && !requireOne)
        {
            result = null;
            return true;
        }

        var anyFound = false;
        for (var i = 2; i < fn.Args.Count; i++)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path))
            {
                if (requireAll)
                {
                    result = 0;
                    return true;
                }

                continue;
            }

            var found = QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out _);
            if (found)
                anyFound = true;

            if (requireAll && !found)
            {
                result = 0;
                return true;
            }

            if (requireOne && found)
            {
                result = 1;
                return true;
            }
        }

        result = requireAll ? 1 : (anyFound ? 1 : 0);
        return true;
    }

    private static bool TryEvalJsonSearchFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("JSON_SEARCH() espera JSON, modo e termo.");

        var json = evalArg(0);
        var mode = evalArg(1)?.ToString() ?? string.Empty;
        var search = evalArg(2)?.ToString() ?? string.Empty;
        if (IsNullish(json) || string.IsNullOrWhiteSpace(search))
        {
            result = null;
            return true;
        }

        if (json is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(json, out var element))
        {
            result = null;
            return true;
        }

        var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
        var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
        if (!requireAll && !requireOne)
        {
            result = null;
            return true;
        }

        var pathStart = 3;
        if (fn.Args.Count > 4)
        {
            var escapeCandidate = evalArg(3)?.ToString();
            if (!string.IsNullOrEmpty(escapeCandidate)
                && escapeCandidate!.Length == 1)
                pathStart = 4;
        }

        var results = new List<string>();
        if (fn.Args.Count > pathStart)
        {
            for (var i = pathStart; i < fn.Args.Count; i++)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path))
                    continue;

                if (QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var scoped))
                    CollectJsonSearchMatches(scoped, path!, search, results);
            }
        }
        else
        {
            CollectJsonSearchMatches(element, "$", search, results);
        }

        if (results.Count == 0)
        {
            result = null;
            return true;
        }

        result = requireOne ? results[0] : AstQueryJsonSharedFunctionEvaluator.BuildJsonArray(results.Cast<object?>());
        return true;
    }

    private static void CollectJsonSearchMatches(
        JsonElement element,
        string currentPath,
        string search,
        IList<string> results)
    {
        if (currentPath.IndexOf(search, StringComparison.OrdinalIgnoreCase) >= 0)
            results.Add(currentPath);

        if (element.ValueKind == JsonValueKind.Array)
        {
            var index = 0;
            foreach (var item in element.EnumerateArray())
            {
                CollectJsonSearchMatches(item, $"{currentPath}[{index}]", search, results);
                index++;
            }

            return;
        }

        if (element.ValueKind != JsonValueKind.Object)
            return;

        foreach (var prop in element.EnumerateObject())
            CollectJsonSearchMatches(prop.Value, $"{currentPath}.{prop.Name}", search, results);
    }

    private static bool TryEvalJsonAppendFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera um JSON seguido de pares path/valor.");

        var json = evalArg(0);
        if (IsNullish(json) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i += 2)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonPathTokens(path!, out var tokens))
            {
                result = null;
                return true;
            }

            var value = evalArg(i + 1);
            if (!AstQueryJsonPathFunctionEvaluator.TryAppendJsonPathValue(ref root, tokens, value))
            {
                result = null;
                return true;
            }
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonMergeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos dois JSONs.");

        if (!TryParseJsonNodeOrNull(evalArg(0), out var mergedRoot))
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            if (!TryParseJsonNodeOrNull(evalArg(i), out var nextNode))
            {
                result = null;
                return true;
            }

            mergedRoot = string.Equals(fn.Name, "JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase)
                ? MergeJsonPatch(mergedRoot, nextNode)
                : MergeJsonPreserve(mergedRoot, nextNode);
        }

        result = mergedRoot!.ToJsonString();
        return true;
    }

    private static bool TryParseJsonNodeOrNull(object? value, out JsonNode? node)
    {
        if (IsNullish(value))
        {
            node = null;
            return true;
        }

        if (value is JsonNode jsonNode)
        {
            node = jsonNode.DeepClone();
            return true;
        }

        if (AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(value!, out var parsed))
        {
            node = parsed?.DeepClone();
            return true;
        }

        node = null;
        return false;
    }

    private static JsonNode MergeJsonPreserve(JsonNode? left, JsonNode? right)
    {
        if (left is null)
            return right?.DeepClone() ?? JsonValue.Create((string?)null)!;

        if (right is null)
            return left.DeepClone();

        if (left is JsonObject leftObject && right is JsonObject rightObject)
            return MergeJsonObjectsPreserve(leftObject, rightObject);

        if (left is JsonArray leftArray && right is JsonArray rightArray)
            return MergeJsonArrays(leftArray, rightArray);

        if (left is JsonArray leftArrayOnly)
        {
            var merged = new JsonArray();
            foreach (var item in leftArrayOnly)
                merged.Add(item?.DeepClone());
            merged.Add(right.DeepClone());
            return merged;
        }

        if (right is JsonArray rightArrayOnly)
        {
            var merged = new JsonArray();
            merged.Add(left.DeepClone());
            foreach (var item in rightArrayOnly)
                merged.Add(item?.DeepClone());
            return merged;
        }

        var pair = new JsonArray
        {
            left.DeepClone(),
            right.DeepClone()
        };
        return pair;
    }

    private static JsonNode MergeJsonPatch(JsonNode? left, JsonNode? right)
    {
        if (right is null)
            return JsonValue.Create((string?)null)!;

        if (left is not JsonObject leftObject || right is not JsonObject rightObject)
            return right.DeepClone();

        var merged = new JsonObject();
        foreach (var prop in leftObject)
        {
            if (prop.Value is not null)
                merged[prop.Key] = prop.Value.DeepClone();
        }

        foreach (var prop in rightObject)
        {
            if (IsJsonNullNode(prop.Value))
            {
                merged.Remove(prop.Key);
                continue;
            }

            merged[prop.Key] = prop.Value!.DeepClone();
        }

        return merged;
    }

    private static JsonNode MergeJsonObjectsPreserve(JsonObject left, JsonObject right)
    {
        var merged = new JsonObject();
        foreach (var prop in left)
        {
            if (prop.Value is not null)
                merged[prop.Key] = prop.Value.DeepClone();
        }

        foreach (var prop in right)
        {
            if (!merged.TryGetPropertyValue(prop.Key, out var existing) || existing is null)
            {
                merged[prop.Key] = prop.Value?.DeepClone();
                continue;
            }

            merged[prop.Key] = MergeJsonPreserve(existing, prop.Value);
        }

        return merged;
    }

    private static JsonNode MergeJsonArrays(JsonArray left, JsonArray right)
    {
        var merged = new JsonArray();
        foreach (var item in left)
            merged.Add(item?.DeepClone());
        foreach (var item in right)
            merged.Add(item?.DeepClone());
        return merged;
    }

    private static bool IsJsonNullNode(JsonNode? node)
        => node is null
            || (node is JsonValue jsonValue
                && string.Equals(jsonValue.ToJsonString(), "null", StringComparison.OrdinalIgnoreCase));

    private static bool TryEvalJsonArrayInsertFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
            throw new InvalidOperationException("JSON_ARRAY_INSERT() espera um JSON seguido de pares path/valor.");

        var json = evalArg(0);
        if (IsNullish(json) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i += 2)
        {
            var path = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonPathTokens(path!, out var tokens))
            {
                result = null;
                return true;
            }

            var value = evalArg(i + 1);
            if (!AstQueryJsonPathFunctionEvaluator.TryInsertJsonPathValue(ref root, tokens, value))
            {
                result = null;
                return true;
            }
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonStorageSizeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("JSON_STORAGE_SIZE() espera um JSON.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value!, out var element))
        {
            result = null;
            return true;
        }

        var raw = element.GetRawText();
        result = (long)Encoding.UTF8.GetByteCount(raw);
        return true;
    }

    private static bool TryEvalJsonOverlapsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_OVERLAPS() espera dois JSONs.");

        var leftValue = evalArg(0);
        var rightValue = evalArg(1);
        if (IsNullish(leftValue) || IsNullish(rightValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(leftValue!, out var leftElement)
            || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(rightValue!, out var rightElement))
        {
            result = null;
            return true;
        }

        result = JsonOverlaps(leftElement, rightElement) ? 1 : 0;
        return true;
    }

    private static bool JsonContains(JsonElement target, JsonElement candidate)
    {
        if (candidate.ValueKind == JsonValueKind.Object)
        {
            if (target.ValueKind != JsonValueKind.Object)
                return false;

            foreach (var prop in candidate.EnumerateObject())
            {
                if (!target.TryGetProperty(prop.Name, out var targetProp))
                    return false;

                if (!JsonContains(targetProp, prop.Value))
                    return false;
            }

            return true;
        }

        if (candidate.ValueKind == JsonValueKind.Array)
        {
            if (target.ValueKind != JsonValueKind.Array)
                return false;

            var targetItems = target.EnumerateArray().ToArray();
            foreach (var candidateItem in candidate.EnumerateArray())
            {
                if (!targetItems.Any(item => JsonContains(item, candidateItem)))
                    return false;
            }

            return true;
        }

        return JsonElementEquals(target, candidate);
    }

    private static bool JsonOverlaps(JsonElement left, JsonElement right)
    {
        if (left.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in left.EnumerateArray())
            {
                if (JsonOverlaps(item, right))
                    return true;
            }

            return false;
        }

        if (right.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in right.EnumerateArray())
            {
                if (JsonOverlaps(left, item))
                    return true;
            }

            return false;
        }

        if (left.ValueKind == JsonValueKind.Object
            && right.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in left.EnumerateObject())
            {
                if (right.TryGetProperty(prop.Name, out var rightProp)
                    && JsonOverlaps(prop.Value, rightProp))
                {
                    return true;
                }
            }

            return false;
        }

        if (left.ValueKind == JsonValueKind.Object
            || right.ValueKind == JsonValueKind.Object)
            return false;

        return JsonElementEquals(left, right);
    }

    private static bool JsonElementEquals(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == JsonValueKind.Number
                && right.ValueKind == JsonValueKind.Number)
            {
                if (left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec))
                    return ldec == rdec;

                if (left.TryGetDouble(out var ldbl) && right.TryGetDouble(out var rdbl))
                    return Math.Abs(ldbl - rdbl) < double.Epsilon;
            }

            return false;
        }

        return left.ToString() == right.ToString();
    }

    private static bool TryParseJsonCandidate(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        if (text.TrimStart().StartsWith("{", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("[", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("\"", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("true", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("false", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("null", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("-", StringComparison.Ordinal)
            || char.IsDigit(text.TrimStart()[0]))
        {
            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
                return true;
            }
            catch
            {
                // fallthrough to treat as string
            }
        }

        element = JsonSerializer.SerializeToElement(text);
        return true;
    }
}

