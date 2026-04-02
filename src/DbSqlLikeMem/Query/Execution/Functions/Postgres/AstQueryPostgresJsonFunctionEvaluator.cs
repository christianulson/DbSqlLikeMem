namespace DbSqlLikeMem;

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

internal static class AstQueryPostgresJsonFunctionEvaluator
{
    private const int JsonPathParseCacheSoftLimit = 512;
    private static readonly HashSet<string> _supportedFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "TO_JSON",
        "TO_JSONB",
        "ROW_TO_JSON",
        "JSON_SCALAR",
        "JSON_SERIALIZE",
        "JSONB_PATH_EXISTS",
        "JSONB_PATH_QUERY_ARRAY",
        "JSON_TYPEOF",
        "JSONB_TYPEOF",
        "JSON_ARRAY_LENGTH",
        "JSONB_ARRAY_LENGTH",
        "JSON_BUILD_ARRAY",
        "JSONB_BUILD_ARRAY",
        "JSON_BUILD_OBJECT",
        "JSONB_BUILD_OBJECT",
        "JSON_EXTRACT_PATH",
        "JSONB_EXTRACT_PATH",
        "JSON_EXTRACT_PATH_TEXT",
        "JSONB_EXTRACT_PATH_TEXT",
        "JSON_STRIP_NULLS",
        "JSONB_STRIP_NULLS",
        "JSONB_OBJECT",
        "JSONB_SET",
        "JSONB_SET_LAX",
        "JSONB_INSERT",
        "JSONB_PRETTY"
    };
    private static readonly ConcurrentDictionary<string, JsonPathTokenCacheEntry> _jsonPathTokenCache = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, JsonPathTokenCacheEntry> _postgresJsonPathTokenCache = new(StringComparer.Ordinal);

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_supportedFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        if (name is "TO_JSON" or "TO_JSONB" or "ROW_TO_JSON")
        {
            var value = evalArg(0);
            result = AstQueryExecutorBase.IsNullish(value) ? null : JsonSerializer.Serialize(value);
            return true;
        }

        if (name is "JSON_SCALAR" or "JSON_SERIALIZE")
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonCandidate(value!, out var candidate))
            {
                result = null;
                return true;
            }

            result = candidate.GetRawText();
            return true;
        }

        if (name is "JSONB_PATH_EXISTS" or "JSONB_PATH_QUERY_ARRAY")
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 2)
                throw new InvalidOperationException($"{name}() espera JSONB e jsonpath.");

            var value = evalArg(0);
            var path = evalArg(1)?.ToString();
            if (AstQueryExecutorBase.IsNullish(value) || string.IsNullOrWhiteSpace(path))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            if (!TryReadPostgresJsonPath(element, path!, out var target))
            {
                result = name == "JSONB_PATH_EXISTS" ? false : "[]";
                return true;
            }

            if (name == "JSONB_PATH_EXISTS")
            {
                result = true;
                return true;
            }

            result = BuildJsonArray([target]);
            return true;
        }

        if (name is "JSON_TYPEOF" or "JSONB_TYPEOF")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Object => "object",
                JsonValueKind.Array => "array",
                JsonValueKind.String => "string",
                JsonValueKind.Number => "number",
                JsonValueKind.True => "boolean",
                JsonValueKind.False => "boolean",
                JsonValueKind.Null => "null",
                _ => null
            };
            return true;
        }

        if (name is "JSON_ARRAY_LENGTH" or "JSONB_ARRAY_LENGTH")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element)
                || element.ValueKind != JsonValueKind.Array)
            {
                result = null;
                return true;
            }

            result = element.GetArrayLength();
            return true;
        }

        if (name is "JSON_BUILD_ARRAY" or "JSONB_BUILD_ARRAY")
        {
            var values = new object?[fn.Args.Count];
            for (var i = 0; i < fn.Args.Count; i++)
                values[i] = evalArg(i);

            result = BuildJsonArray(values);
            return true;
        }

        if (name is "JSON_BUILD_OBJECT" or "JSONB_BUILD_OBJECT")
        {
            if (fn.Args.Count % 2 != 0)
                throw new InvalidOperationException($"{name}() espera um numero par de argumentos.");

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < fn.Args.Count; i += 2)
            {
                var key = evalArg(i)?.ToString() ?? string.Empty;
                var val = evalArg(i + 1);
                pairs.Add((key, val));
            }

            result = BuildJsonObject(pairs);
            return true;
        }

        if (name is "JSON_EXTRACT_PATH" or "JSONB_EXTRACT_PATH" or "JSON_EXTRACT_PATH_TEXT" or "JSONB_EXTRACT_PATH_TEXT")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var pathSegment = evalArg(i)?.ToString();
                JsonElement nextElement;
                if (string.IsNullOrEmpty(pathSegment)
                    || !TryReadPostgresJsonPathElement(element, pathSegment!, out nextElement))
                {
                    result = null;
                    return true;
                }

                element = nextElement;
            }

            if (name.EndsWith("_TEXT", StringComparison.Ordinal))
            {
                result = element.ValueKind switch
                {
                    JsonValueKind.String => element.GetString(),
                    JsonValueKind.Null => null,
                    _ => element.GetRawText()
                };
                return true;
            }

            result = element.GetRawText();
            return true;
        }

        if (name is "JSON_STRIP_NULLS" or "JSONB_STRIP_NULLS")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !TryParseJsonNode(value!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            var normalized = AstQueryJsonSharedFunctionEvaluator.CloneJsonNode(root);
            AstQueryJsonSharedFunctionEvaluator.StripJsonNullProperties(normalized);
            result = normalized.ToJsonString();
            return true;
        }

        if (name is "JSONB_OBJECT")
        {
            return TryBuildJsonbObjectFunction(fn, evalArg, out result);
        }

        if (name is "JSONB_SET" or "JSONB_SET_LAX")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException($"{name}() espera JSON, caminho e novo valor.");

            var json = evalArg(0);
            var pathValue = evalArg(1);
            var newValue = evalArg(2);
            if (AstQueryExecutorBase.IsNullish(json) || AstQueryExecutorBase.IsNullish(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            if (!TryParsePostgresJsonPathTokens(pathValue!, out var tokens))
            {
                result = null;
                return true;
            }

            if (name is "JSONB_SET_LAX" && AstQueryExecutorBase.IsNullish(newValue))
            {
                var createIfMissingLax = fn.Args.Count < 4 || Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
                var treatment = fn.Args.Count > 4
                    ? (evalArg(4)?.ToString() ?? "use_json_null").Trim().ToLowerInvariant()
                    : "use_json_null";

                if (treatment == "return_target")
                {
                    result = root.ToJsonString();
                    return true;
                }

                if (treatment == "delete_key")
                {
                    if (tokens.Count > 0)
                    {
                        if (createIfMissingLax || TryGetJsonNodeAtPath(root, tokens, out _))
                            TryRemoveJsonPathValue(root, tokens);
                    }

                    result = root.ToJsonString();
                    return true;
                }

                if (treatment == "raise_exception")
                    throw new InvalidOperationException("JSONB_SET_LAX() recebeu null com tratamento raise_exception.");

                newValue = null;
            }

            var createIfMissing = fn.Args.Count < 4 || Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
            if (!createIfMissing && !TryGetJsonNodeAtPath(root, tokens, out _))
            {
                result = root.ToJsonString();
                return true;
            }

            if (!TrySetJsonPathValue(ref root, tokens, newValue))
            {
                result = null;
                return true;
            }

            result = root.ToJsonString();
            return true;
        }

        if (name is "JSONB_INSERT")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSONB_INSERT() espera JSON, caminho e novo valor.");

            var json = evalArg(0);
            var pathValue = evalArg(1);
            var newValue = evalArg(2);
            var insertAfter = fn.Args.Count > 3 && Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
            if (AstQueryExecutorBase.IsNullish(json) || AstQueryExecutorBase.IsNullish(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            if (!TryParsePostgresJsonPathTokens(pathValue!, out var tokens)
                || tokens.Count == 0)
            {
                result = null;
                return true;
            }

            if (!TryInsertJsonPathValue(root, tokens, newValue, insertAfter))
            {
                result = null;
                return true;
            }

            result = root.ToJsonString();
            return true;
        }

        if (name is "JSONB_PRETTY")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            result = JsonSerializer.Serialize(element, options)
                .Replace("\r\n", "\n");
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalJsonbObjectFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "JSONB_OBJECT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        return TryBuildJsonbObjectFunction(fn, evalArg, out result);
    }

    private static bool TryParseJsonElement(object value, out JsonElement element)
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

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
            return true;
        }
        catch
        {
            element = default;
            return false;
        }
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

    private static bool TryReadPostgresJsonPathElement(
        JsonElement element,
        string pathSegment,
        out JsonElement target)
    {
        target = default;
        if (string.IsNullOrEmpty(pathSegment))
            return false;

        if (element.ValueKind == JsonValueKind.Object)
        {
            if (element.TryGetProperty(pathSegment, out target))
                return true;

            return false;
        }

        if (element.ValueKind == JsonValueKind.Array
            && int.TryParse(pathSegment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
            && index >= 0)
        {
            var currentIndex = 0;
            foreach (var item in element.EnumerateArray())
            {
                if (currentIndex == index)
                {
                    target = item;
                    return true;
                }

                currentIndex++;
            }
        }

        return false;
    }

    private static bool TryReadPostgresJsonPath(
        JsonElement element,
        string path,
        out JsonElement target)
    {
        target = default;
        if (!TryParseJsonPathTokens(path, out var tokens))
            return false;

        var current = element;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (!TryReadPostgresJsonPathElement(current, token.PropertyName ?? string.Empty, out current))
                    return false;

                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (!TryReadPostgresJsonPathElement(current, (token.ArrayIndex ?? 0).ToString(CultureInfo.InvariantCulture), out current))
                    return false;
            }
        }

        target = current;
        return true;
    }

    private static bool TryParseJsonNode(object json, out JsonNode? node)
    {
        if (json is JsonNode jsonNode)
        {
            node = jsonNode;
            return true;
        }

        var text = json.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            node = null;
            return false;
        }

        try
        {
            node = JsonNode.Parse(text);
            return node is not null;
        }
        catch
        {
            node = null;
            return false;
        }
    }

    private static bool TryReadPostgresTextArray(object? value, out List<string> items)
    {
        items = [];
        if (AstQueryExecutorBase.IsNullish(value))
            return false;

        if (value is IEnumerable enumerable && value is not string)
        {
            foreach (var item in enumerable)
                items.Add(item?.ToString() ?? string.Empty);

            return true;
        }

        return false;
    }

    private static bool TryBuildJsonbObjectFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 1)
        {
            if (!TryReadPostgresTextArray(evalArg(0), out var entries) || entries.Count % 2 != 0)
            {
                result = null;
                return true;
            }

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < entries.Count; i += 2)
                pairs.Add((entries[i], entries[i + 1]));

            result = BuildJsonObject(pairs);
            return true;
        }

        if (fn.Args.Count == 2)
        {
            if (!TryReadPostgresTextArray(evalArg(0), out var keys)
                || !TryReadPostgresTextArray(evalArg(1), out var values)
                || keys.Count != values.Count)
            {
                result = null;
                return true;
            }

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < keys.Count; i++)
                pairs.Add((keys[i], values[i]));

            result = BuildJsonObject(pairs);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryParsePostgresJsonPathTokens(object value, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (!TryReadPostgresTextArray(value, out var segments))
            return false;

        var cacheKey = BuildPostgresJsonPathCacheKey(segments);
        if (_postgresJsonPathTokenCache.TryGetValue(cacheKey, out var cached))
        {
            tokens = cached.Success ? [.. cached.Tokens] : [];
            return cached.Success;
        }

        foreach (var segment in segments)
        {
            if (int.TryParse(segment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
                && index >= 0)
            {
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, segment, null));
        }

        var success = tokens.Count > 0;
        CacheJsonPathParseEntry(_postgresJsonPathTokenCache, cacheKey, new JsonPathTokenCacheEntry(success, [.. tokens]));
        return success;
    }

    private static bool TryParseJsonPathTokens(string path, out List<JsonPathToken> tokens)
    {
        if (_jsonPathTokenCache.TryGetValue(path, out var cached))
        {
            tokens = cached.Success ? [.. cached.Tokens] : [];
            return cached.Success;
        }

        var success = TryParseJsonPathTokensCore(path, out tokens);
        CacheJsonPathParseEntry(_jsonPathTokenCache, path, new JsonPathTokenCacheEntry(success, [.. tokens]));
        return success;
    }

    private static bool TryParseJsonPathTokensCore(string path, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[4..].TrimStart();
        else if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[7..].TrimStart();

        if (trimmed.Length == 0 || trimmed[0] != '$')
            return false;

        var i = 1;
        while (i < trimmed.Length)
        {
            while (i < trimmed.Length && char.IsWhiteSpace(trimmed[i]))
                i++;

            if (i >= trimmed.Length)
                break;

            if (trimmed[i] == '.')
            {
                i++;
                var start = i;
                while (i < trimmed.Length && (char.IsLetterOrDigit(trimmed[i]) || trimmed[i] == '_'))
                    i++;

                if (i == start)
                    return false;

                var property = trimmed[start..i];
                tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                continue;
            }

            if (trimmed[i] == '[')
            {
                i++;
                if (i >= trimmed.Length)
                    return false;

                if (trimmed[i] is '"' or '\'')
                {
                    var quote = trimmed[i];
                    i++;
                    var start = i;
                    while (i < trimmed.Length && trimmed[i] != quote)
                        i++;

                    if (i >= trimmed.Length)
                        return false;

                    var property = trimmed[start..i];
                    i++;
                    if (i >= trimmed.Length || trimmed[i] != ']')
                        return false;
                    i++;
                    tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                    continue;
                }

                var indexStart = i;
                while (i < trimmed.Length && char.IsDigit(trimmed[i]))
                    i++;

                if (i == indexStart || i >= trimmed.Length || trimmed[i] != ']')
                    return false;

                if (!int.TryParse(trimmed[indexStart..i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
                    return false;

                i++;
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            return false;
        }

        return true;
    }

    private static string BuildPostgresJsonPathCacheKey(IReadOnlyList<string> segments)
        => string.Join("\u001F", segments);

    private static void CacheJsonPathParseEntry(
        ConcurrentDictionary<string, JsonPathTokenCacheEntry> cache,
        string key,
        JsonPathTokenCacheEntry entry)
    {
        if (cache.Count >= JsonPathParseCacheSoftLimit)
            cache.Clear();

        cache[key] = entry;
    }

    private static JsonNode CreateJsonNodeFromValue(object? value)
    {
        if (value is null or DBNull)
        {
            return JsonValue.Create((string?)null)
                ?? JsonNode.Parse("null")!;
        }

        if (value is JsonElement element)
            return JsonNode.Parse(element.GetRawText())!;

        if (value is JsonNode node)
            return node;

        return JsonValue.Create(value)
            ?? JsonNode.Parse(JsonSerializer.Serialize(value))!;
    }

    private static JsonNode CreateJsonContainer(JsonPathToken nextToken)
        => nextToken.Kind == JsonPathTokenKind.ArrayIndex
            ? new JsonArray()
            : new JsonObject();

    private static bool TrySetJsonPathValue(
        ref JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        JsonNode? current = root;
        JsonNode? parent = null;
        JsonPathToken? parentToken = null;

        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            var isLast = i == tokens.Count - 1;

            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not JsonObject obj)
                {
                    if (current is null or JsonValue)
                    {
                        obj = new JsonObject();
                        AssignJsonChild(ref root, parent, parentToken, obj);
                    }
                    else
                    {
                        return false;
                    }
                }

                if (isLast)
                {
                    obj[token.PropertyName!] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = obj[token.PropertyName!];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    obj[token.PropertyName!] = child;
                }

                parent = obj;
                parentToken = token;
                current = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not JsonArray array)
                {
                    if (current is null or JsonValue)
                    {
                        array = new JsonArray();
                        AssignJsonChild(ref root, parent, parentToken, array);
                    }
                    else
                    {
                        return false;
                    }
                }

                var index = token.ArrayIndex ?? 0;
                while (array.Count <= index)
                    array.Add(null);

                if (isLast)
                {
                    array[index] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = array[index];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    array[index] = child;
                }

                parent = array;
                parentToken = token;
                current = child;
            }
        }

        return false;
    }

    private static bool TryGetJsonNodeAtPath(
        JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        out JsonNode? node)
    {
        node = root;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (node is not JsonObject obj)
                {
                    node = null;
                    return false;
                }

                if (!obj.TryGetPropertyValue(token.PropertyName!, out var child))
                {
                    node = null;
                    return false;
                }

                node = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (node is not JsonArray array)
                {
                    node = null;
                    return false;
                }

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                {
                    node = null;
                    return false;
                }

                node = array[index];
            }
        }

        return node is not null;
    }

    private static void AssignJsonChild(
        ref JsonNode root,
        JsonNode? parent,
        JsonPathToken? parentToken,
        JsonNode child)
    {
        if (parent is null)
        {
            root = child;
            return;
        }

        if (parent is JsonObject obj && parentToken?.Kind == JsonPathTokenKind.Property)
        {
            obj[parentToken.Value.PropertyName!] = child;
            return;
        }

        if (parent is JsonArray array && parentToken?.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var index = parentToken.Value.ArrayIndex ?? 0;
            while (array.Count <= index)
                array.Add(null);
            array[index] = child;
        }
    }

    private static bool TryRemoveJsonPathValue(
        JsonNode root,
        IReadOnlyList<JsonPathToken> tokens)
    {
        if (tokens.Count == 0)
            return false;

        JsonNode? current = root;
        for (var i = 0; i < tokens.Count - 1; i++)
        {
            var token = tokens[i];
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not JsonObject obj)
                    return true;

                current = obj[token.PropertyName!];
                if (current is null)
                    return true;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not JsonArray array)
                    return true;

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                    return true;
                current = array[index];
                if (current is null)
                    return true;
            }
        }

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (current is not JsonObject obj)
                return true;

            obj.Remove(lastToken.PropertyName!);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (current is not JsonArray array)
                return true;

            var index = lastToken.ArrayIndex ?? 0;
            if (index < 0 || index >= array.Count)
                return true;

            array.RemoveAt(index);
            return true;
        }

        return true;
    }

    private static bool TryInsertJsonPathValue(
        JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value,
        bool insertAfter)
    {
        if (tokens.Count == 0)
            return false;

        if (tokens.Count == 1)
        {
            var targetToken = tokens[0];
            if (targetToken.Kind == JsonPathTokenKind.Property && root is JsonObject rootObject)
            {
                if (rootObject[targetToken.PropertyName!] is not null)
                    return true;

                rootObject[targetToken.PropertyName!] = CreateJsonNodeFromValue(value);
                return true;
            }

            if (targetToken.Kind == JsonPathTokenKind.ArrayIndex && root is JsonArray rootArray)
            {
                var insertIndex = targetToken.ArrayIndex ?? 0;
                if (insertAfter)
                    insertIndex++;

                insertIndex = Math.Max(0, Math.Min(insertIndex, rootArray.Count));
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            return false;
        }

        var parentTokens = tokens.Take(tokens.Count - 1).ToList();
        if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is null)
            return false;

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (parent is not JsonObject obj)
                return false;

            if (obj[lastToken.PropertyName!] is not null)
                return true;

            obj[lastToken.PropertyName!] = CreateJsonNodeFromValue(value);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (parent is not JsonArray array)
                return false;

            var insertIndex = lastToken.ArrayIndex ?? 0;
            if (insertAfter)
                insertIndex++;

            insertIndex = Math.Max(0, Math.Min(insertIndex, array.Count));
            array.Insert(insertIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        return false;
    }

    private static string BuildJsonArray(IEnumerable<object?> values)
    {
        var parts = values.Select(static value =>
        {
            if (value is null or DBNull)
                return "null";

            if (value is JsonElement element)
                return element.GetRawText();

            return JsonSerializer.Serialize(value);
        });

        return "[" + string.Join(",", parts) + "]";
    }

    private static string BuildJsonObject(IEnumerable<(string Key, object? Value)> pairs)
    {
        var parts = pairs.Select(static pair =>
        {
            var key = JsonSerializer.Serialize(pair.Key ?? string.Empty);
            var value = pair.Value;
            if (value is null or DBNull)
                return $"{key}:null";

            if (value is JsonElement element)
                return $"{key}:{element.GetRawText()}";

            return $"{key}:{JsonSerializer.Serialize(value)}";
        });

        return "{" + string.Join(",", parts) + "}";
    }

    private enum JsonPathTokenKind
    {
        Property,
        ArrayIndex
    }

    private readonly record struct JsonPathToken(JsonPathTokenKind Kind, string? PropertyName, int? ArrayIndex);
    private readonly record struct JsonPathTokenCacheEntry(bool Success, JsonPathToken[] Tokens);
}
