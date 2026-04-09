using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryJsonPathFunctionEvaluator
{
    private static readonly ConcurrentDictionary<string, JsonPathTokenCacheEntry> _jsonPathTokenCache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, JsonPathTokenCacheEntry> _postgresJsonPathTokenCache = new(StringComparer.OrdinalIgnoreCase);

    internal enum JsonPathTokenKind
    {
        Property,
        ArrayIndex
    }

    internal readonly record struct JsonPathToken(JsonPathTokenKind Kind, string? PropertyName, int? ArrayIndex);
    internal readonly record struct JsonPathTokenCacheEntry(bool Success, JsonPathToken[] Tokens);

    internal static bool TryParseJsonNode(object json, out JsonNode? node)
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

    internal static JsonNode CreateJsonNodeFromValue(object? value)
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

    internal static bool TrySetJsonPathValue(
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
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not JsonObject obj)
                {
                    if (parent is null || parentToken is null)
                    {
                        if (root is not JsonObject)
                        {
                            root = new JsonObject();
                            current = root;
                        }

                        obj = (JsonObject)root;
                        current = obj;
                    }
                    else
                    {
                        obj = new JsonObject();
                        AssignJsonChild(ref root, parent, parentToken.Value, obj);
                        current = obj;
                    }
                }

                if (i == tokens.Count - 1)
                {
                    obj[token.PropertyName!] = CreateJsonNodeFromValue(value);
                    return true;
                }

                parent = current;
                parentToken = token;
                if (!obj.TryGetPropertyValue(token.PropertyName!, out var next) || next is null)
                {
                    next = CreateJsonContainer(tokens[i + 1]);
                    obj[token.PropertyName!] = next;
                }

                current = next;
                continue;
            }

            if (current is not JsonArray array)
            {
                if (parent is null || parentToken is null)
                {
                    if (root is not JsonArray)
                    {
                        root = new JsonArray();
                        current = root;
                    }

                    array = (JsonArray)root;
                    current = array;
                }
                else
                {
                    array = new JsonArray();
                    AssignJsonChild(ref root, parent, parentToken.Value, array);
                    current = array;
                }
            }

            if (token.ArrayIndex is null)
                return false;

            var index = token.ArrayIndex.Value;
            if (index < 0)
                return false;

            if (i == tokens.Count - 1)
            {
                EnsureJsonArraySize(array, index);
                array[index] = CreateJsonNodeFromValue(value);
                return true;
            }

            while (array.Count <= index)
                array.Add(null);

            var child = array[index];
            if (child is null)
            {
                child = CreateJsonContainer(tokens[i + 1]);
                array[index] = child;
            }

            parent = current;
            parentToken = token;
            current = child;
        }

        return true;
    }

    internal static bool TryGetJsonNodeAtPath(
        JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        out JsonNode? node)
    {
        node = root;
        if (tokens.Count == 0)
            return true;

        JsonNode? current = root;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not JsonObject obj || !obj.TryGetPropertyValue(token.PropertyName!, out current))
                {
                    node = null;
                    return false;
                }

                continue;
            }

            if (current is not JsonArray array || token.ArrayIndex is null || token.ArrayIndex.Value < 0 || token.ArrayIndex.Value >= array.Count)
            {
                node = null;
                return false;
            }

            current = array[token.ArrayIndex.Value];
        }

        node = current;
        return true;
    }

    internal static bool TryRemoveJsonPathValue(
        JsonNode root,
        IReadOnlyList<JsonPathToken> tokens)
    {
        if (tokens.Count == 0)
            return false;

        if (tokens.Count == 1)
        {
            var token = tokens[0];
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (root is not JsonObject rootObject)
                    return false;

                return rootObject.Remove(token.PropertyName!);
            }

            if (root is not JsonArray rootArray || token.ArrayIndex is null || token.ArrayIndex.Value < 0 || token.ArrayIndex.Value >= rootArray.Count)
                return false;

            rootArray.RemoveAt(token.ArrayIndex.Value);
            return true;
        }

        var parentTokens = tokens.Take(tokens.Count - 1).ToList();
        if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is null)
            return false;

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (parent is not JsonObject obj)
                return false;

            return obj.Remove(lastToken.PropertyName!);
        }

        if (parent is not JsonArray array || lastToken.ArrayIndex is null || lastToken.ArrayIndex.Value < 0 || lastToken.ArrayIndex.Value >= array.Count)
            return false;

        array.RemoveAt(lastToken.ArrayIndex.Value);
        return true;
    }

    internal static bool TryAppendJsonPathValue(
        ref JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
        {
            if (root is JsonArray rootArray)
            {
                rootArray.Add(CreateJsonNodeFromValue(value));
                return true;
            }

            root = new JsonArray
            {
                root is null ? null : AstQueryJsonSharedFunctionEvaluator.CloneJsonNode(root),
                CreateJsonNodeFromValue(value)
            };
            return true;
        }

        if (!TryGetJsonNodeAtPath(root, tokens, out var node))
            return false;

        if (node is JsonArray array)
        {
            array.Add(CreateJsonNodeFromValue(value));
            return true;
        }

        var newArray = new JsonArray
        {
            node is null ? null : AstQueryJsonSharedFunctionEvaluator.CloneJsonNode(node),
            CreateJsonNodeFromValue(value)
        };

        return TrySetJsonPathValue(ref root, tokens, newArray);
    }

    internal static bool TryInsertJsonPathValue(
        ref JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        var last = tokens[^1];
        if (last.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var parentTokens = tokens.Take(tokens.Count - 1).ToList();
            if (parentTokens.Count == 0)
            {
                if (root is not JsonArray rootArray)
                    return false;

                var index = Math.Max(0, last.ArrayIndex ?? 0);
                var insertIndex = Math.Min(index, rootArray.Count);
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is not JsonArray parentArray)
                return false;

            var parentIndex = Math.Max(0, last.ArrayIndex ?? 0);
            var targetIndex = Math.Min(parentIndex, parentArray.Count);
            parentArray.Insert(targetIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        if (!TryGetJsonNodeAtPath(root, tokens, out var node) || node is not JsonArray array)
            return false;

        array.Add(CreateJsonNodeFromValue(value));
        return true;
    }

    private static JsonNode CreateJsonContainer(JsonPathToken nextToken)
        => nextToken.Kind == JsonPathTokenKind.ArrayIndex
            ? new JsonArray()
            : new JsonObject();

    private static void EnsureJsonArraySize(JsonArray array, int index)
    {
        while (array.Count < index)
            array.Add(null);
    }

    private static void AssignJsonChild(
        ref JsonNode root,
        JsonNode parent,
        JsonPathToken parentToken,
        JsonNode child)
    {
        if (parentToken.Kind == JsonPathTokenKind.Property)
        {
            if (parent is not JsonObject parentObject)
                throw new InvalidOperationException("Parent node is not an object.");

            parentObject[parentToken.PropertyName!] = child;
            return;
        }

        if (parent is not JsonArray parentArray || parentToken.ArrayIndex is null)
            throw new InvalidOperationException("Parent node is not an array.");

        var index = parentToken.ArrayIndex.Value;
        while (parentArray.Count <= index)
            parentArray.Add(null);

        parentArray[index] = child;
    }

    internal static bool TryReadPostgresJsonPathElement(
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

    internal static bool TryReadPostgresJsonPath(
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

    internal static bool TryParsePostgresJsonPathTokens(object value, out List<JsonPathToken> tokens)
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

    internal static bool TryParseJsonPathTokens(string path, out List<JsonPathToken> tokens)
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

    internal static bool TryParseSqlServerJsonModifyPath(
        string path,
        out List<JsonPathToken> tokens,
        out bool append,
        out bool strict)
    {
        tokens = [];
        append = false;
        strict = false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("append ", StringComparison.OrdinalIgnoreCase))
        {
            append = true;
            trimmed = trimmed[7..].TrimStart();
        }

        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            strict = true;

        return TryParseJsonPathTokens(trimmed, out tokens);
    }

    internal static bool TryReadPostgresTextArray(object? value, out List<string> items)
    {
        items = [];
        if (IsNullish(value))
            return false;

        if (value is IEnumerable enumerable && value is not string)
        {
            foreach (var item in enumerable)
                items.Add(item?.ToString() ?? string.Empty);

            return true;
        }

        return false;
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
        if (cache.Count >= AstQueryExecutorBase.JsonPathParseCacheSoftLimit)
            cache.Clear();

        cache[key] = entry;
    }
}
