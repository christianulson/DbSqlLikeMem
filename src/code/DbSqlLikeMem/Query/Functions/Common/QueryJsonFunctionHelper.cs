namespace DbSqlLikeMem;

internal static class QueryJsonFunctionHelper
{
    private const int JsonPathCacheSoftLimit = 512;
    private const int JsonRootCacheSoftLimit = 256;
    private const int JsonPathResolverCacheSoftLimit = 512;
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, JsonPathSpecCacheEntry> _jsonPathSpecCache = new(StringComparer.Ordinal);
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, JsonRootCacheEntry> _jsonRootCache = new(StringComparer.Ordinal);
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, JsonPathResolverCacheEntry> _jsonPathResolverCache = new(StringComparer.Ordinal);

    internal enum JsonPathMode
    {
        Lax,
        Strict
    }

    internal enum JsonPathLookupFailure
    {
        None,
        InvalidPath,
        NotFound
    }

    internal readonly record struct JsonPathLookupResult(
        bool Success,
        JsonPathMode Mode,
        JsonPathLookupFailure Failure,
        string Path,
        JsonElement Value
    );

    private enum JsonPathStepKind
    {
        Property,
        ArrayIndex
    }

    private readonly record struct JsonPathStep(JsonPathStepKind Kind, string? PropertyName, int? ArrayIndex);
    private readonly record struct JsonPathSpec(JsonPathMode Mode, string Path, IReadOnlyList<JsonPathStep> Steps);
    private readonly record struct JsonPathSpecCacheEntry(bool Success, JsonPathSpec Spec);
    private readonly record struct JsonRootCacheEntry(JsonElement Root);
    private readonly record struct JsonPathResolverCacheEntry(JsonPathResolver Resolver);
    private delegate bool JsonPathTraversal(JsonElement element, out JsonElement value);
    private delegate JsonPathLookupResult JsonPathResolver(JsonElement element);

    internal static object? ApplyJsonValueReturningClause(FunctionCallExpr fn, object? value)
    {
        if (IsNullish(value) || !TryGetJsonReturningTypeSql(fn, out var normalizedType))
            return value;

        return ConvertJsonValueForReturningType(value, normalizedType);
    }

    internal static object? TryReadJsonPathValue(object json, string path)
    {
        var lookup = LookupJsonPathNoClone(json, path);
        if (!lookup.Success)
            return null;

        return ConvertJsonElementToValue(lookup.Value);
    }

    internal static string? ConvertJsonElementToSqlServerJsonValue(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => null
        };

    internal static bool TryReadJsonPathElement(object json, string path, out JsonElement element)
    {
        element = default;
        var lookup = LookupJsonPathNoClone(json, path);
        if (!lookup.Success)
            return false;

        element = lookup.Value;
        return true;
    }

    internal static bool TryReadJsonPathElement(JsonElement element, string path, out JsonElement value)
    {
        value = default;
        var lookup = LookupJsonPathNoClone(element, path);
        if (!lookup.Success)
            return false;

        value = lookup.Value;
        return true;
    }

    internal static JsonPathLookupResult LookupJsonPath(object json, string path)
    {
        var lookup = LookupJsonPathNoClone(json, path);
        return lookup.Success
            ? lookup with { Value = lookup.Value.Clone() }
            : lookup;
    }

    private static JsonPathLookupResult LookupJsonPathNoClone(object json, string path)
    {
        if (TryLookupSimpleJsonPath(json, path, out var simpleLookup))
            return simpleLookup;

        if (!TryParseJsonPathSpec(path, out var spec, out _))
            return new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.InvalidPath, path, default);

        if (json is string jsonText)
        {
            try
            {
                return LookupJsonPathFromText(jsonText, spec);
            }
            catch (JsonException)
            {
                if (!TryGetJsonRootElement(jsonText, out var fallbackRoot))
                    throw;

                var fallbackLookup = GetJsonPathResolver(path, spec)(fallbackRoot);
                return fallbackLookup.Success
                    ? fallbackLookup with { Value = fallbackLookup.Value.Clone() }
                    : fallbackLookup;
            }
        }

        if (!TryGetJsonRootElement(json, out var root))
            return new JsonPathLookupResult(false, spec.Mode, JsonPathLookupFailure.NotFound, path, default);

        var lookup = GetJsonPathResolver(path, spec)(root);
        return lookup;
    }

    internal static bool TryGetJsonRootElement(object json, out JsonElement root)
    {
        if (json is JsonElement element)
        {
            root = element;
            return true;
        }

        if (json is JsonDocument document)
        {
            root = document.RootElement;
            return true;
        }

        var text = json.ToString() ?? string.Empty;
        if (_jsonRootCache.TryGetValue(text, out var cached))
        {
            root = cached.Root;
            return true;
        }

        using var parsed = JsonDocument.Parse(text);
        root = parsed.RootElement.Clone();
        CacheJsonRoot(text, new JsonRootCacheEntry(root));
        return true;
    }

    internal static JsonPathLookupResult LookupJsonPath(JsonElement element, string path)
    {
        if (TryLookupSimpleJsonPath(element, path, out var simpleLookup))
            return simpleLookup;

        if (!TryParseJsonPathSpec(path, out var spec, out _))
            return new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.InvalidPath, path, default);

        return GetJsonPathResolver(path, spec)(element);
    }

    private static bool TryLookupSimpleJsonPath(object json, string path, out JsonPathLookupResult result)
    {
        result = default;

        if (string.IsNullOrWhiteSpace(path))
            return false;

        var trimmed = path.AsSpan().Trim();
        if (trimmed.Length == 0)
            return false;

        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (trimmed[0] != '$')
            return false;

        if (trimmed.IndexOf('"') >= 0)
            return false;

        if (!TryGetJsonRootElement(json, out var root))
            return false;

        return TryReadSimpleJsonPath(root, trimmed, path, out result);
    }

    private static bool TryReadSimpleJsonPath(
        JsonElement root,
        ReadOnlySpan<char> path,
        string originalPath,
        out JsonPathLookupResult result)
    {
        result = default;

        var current = root;
        var index = 1;
        while (index < path.Length)
        {
            while (index < path.Length && char.IsWhiteSpace(path[index]))
                index++;

            if (index >= path.Length)
                break;

            if (path[index] == '.')
            {
                index++;
                if (!TryParseSimpleJsonProperty(path, ref index, out var propertyName))
                    return false;

                if (!current.TryGetProperty(propertyName, out current))
                {
                    result = new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.NotFound, originalPath, default);
                    return true;
                }

                continue;
            }

            if (path[index] == '[')
            {
                index++;
                if (!TryParseSimpleJsonArrayIndex(path, ref index, out var arrayIndex))
                    return false;

                if (current.ValueKind != JsonValueKind.Array
                    || arrayIndex < 0
                    || arrayIndex >= current.GetArrayLength())
                {
                    result = new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.NotFound, originalPath, default);
                    return true;
                }

                current = current[arrayIndex];
                continue;
            }

            return false;
        }

        result = new JsonPathLookupResult(true, JsonPathMode.Lax, JsonPathLookupFailure.None, originalPath, current);
        return true;
    }

    private static bool TryParseSimpleJsonProperty(
        ReadOnlySpan<char> path,
        ref int index,
        out ReadOnlySpan<char> propertyName)
    {
        propertyName = default;

        var start = index;
        while (index < path.Length && path[index] is not '.' and not '[' && !char.IsWhiteSpace(path[index]))
            index++;

        if (index == start)
            return false;

        propertyName = path[start..index];
        return true;
    }

    private static bool TryParseSimpleJsonArrayIndex(
        ReadOnlySpan<char> path,
        ref int index,
        out int arrayIndex)
    {
        arrayIndex = default;

        var start = index;
        var value = 0;
        while (index < path.Length && char.IsDigit(path[index]))
        {
            var digit = path[index] - '0';
            if (value > (int.MaxValue - digit) / 10)
                return false;

            value = value * 10 + digit;
            index++;
        }

        if (start == index || index >= path.Length || path[index] != ']')
            return false;

        arrayIndex = value;
        index++;
        return true;
    }

    private static bool TryGetJsonReturningTypeSql(FunctionCallExpr fn, out string normalizedType)
    {
        normalizedType = string.Empty;
        if (fn.Args.Count < 3 || fn.Args[2] is not RawSqlExpr raw)
            return false;

        const string prefix = "RETURNING ";
        if (!raw.Sql.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var typeSql = raw.Sql[prefix.Length..].AsSpan().Trim();
        if (typeSql.Length == 0)
            return false;

        normalizedType = typeSql.ToString().ToUpperInvariant();
        return true;
    }

    private static object? ConvertJsonValueForReturningType(object? value, string normalizedType)
    {
        if (IsJsonReturningDecimalType(normalizedType))
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningFloatingType(normalizedType))
            return Convert.ToDouble(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningIntegerType(normalizedType))
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningTextType(normalizedType))
            return value?.ToString();

        return value;
    }

    private static bool IsJsonReturningDecimalType(string normalizedType)
        => normalizedType.StartsWith("NUMBER", StringComparison.Ordinal)
            || normalizedType.StartsWith("DECIMAL", StringComparison.Ordinal)
            || normalizedType.StartsWith("NUMERIC", StringComparison.Ordinal);

    private static bool IsJsonReturningFloatingType(string normalizedType)
        => normalizedType.StartsWith("DOUBLE", StringComparison.Ordinal)
            || normalizedType.StartsWith("FLOAT", StringComparison.Ordinal)
            || normalizedType.StartsWith("REAL", StringComparison.Ordinal)
            || normalizedType.StartsWith("BINARY_DOUBLE", StringComparison.Ordinal)
            || normalizedType.StartsWith("BINARY_FLOAT", StringComparison.Ordinal);

    private static bool IsJsonReturningIntegerType(string normalizedType)
        => normalizedType.StartsWith("INT", StringComparison.Ordinal)
            || normalizedType.StartsWith("INTEGER", StringComparison.Ordinal)
            || normalizedType.StartsWith("SMALLINT", StringComparison.Ordinal)
            || normalizedType.StartsWith("BIGINT", StringComparison.Ordinal);

    private static bool IsJsonReturningTextType(string normalizedType)
        => normalizedType.StartsWith("VARCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("VARCHAR2", StringComparison.Ordinal)
            || normalizedType.StartsWith("NVARCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("NVARCHAR2", StringComparison.Ordinal)
            || normalizedType.StartsWith("CHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("NCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("CLOB", StringComparison.Ordinal);

    private static JsonPathLookupResult LookupJsonPath(JsonElement element, JsonPathSpec spec)
    {
        if (!TryReadJsonPathElement(element, spec.Steps, out var value))
            return new JsonPathLookupResult(false, spec.Mode, JsonPathLookupFailure.NotFound, spec.Path, default);

        return new JsonPathLookupResult(true, spec.Mode, JsonPathLookupFailure.None, spec.Path, value);
    }

    private static JsonPathResolver GetJsonPathResolver(string path, JsonPathSpec spec)
    {
        if (_jsonPathResolverCache.TryGetValue(path, out var cached))
            return cached.Resolver;

        var resolver = BuildJsonPathResolver(spec);
        CacheJsonPathResolver(path, new JsonPathResolverCacheEntry(resolver));
        return resolver;
    }

    private static JsonPathResolver BuildJsonPathResolver(JsonPathSpec spec)
    {
        var traversal = BuildJsonPathTraversal(spec.Steps);
        return element => traversal(element, out var value)
            ? new JsonPathLookupResult(true, spec.Mode, JsonPathLookupFailure.None, spec.Path, value)
            : new JsonPathLookupResult(false, spec.Mode, JsonPathLookupFailure.NotFound, spec.Path, default);
    }

    private static JsonPathTraversal BuildJsonPathTraversal(IReadOnlyList<JsonPathStep> steps)
    {
        JsonPathTraversal traversal = static (JsonElement element, out JsonElement value) =>
        {
            value = element;
            return true;
        };

        for (var i = steps.Count - 1; i >= 0; i--)
        {
            var step = steps[i];
            var next = traversal;
            if (step.Kind == JsonPathStepKind.Property)
            {
                var propertyName = step.PropertyName!;
                traversal = (JsonElement element, out JsonElement value) =>
                {
                    if (element.ValueKind == JsonValueKind.Object
                        && element.TryGetProperty(propertyName, out var nextElement))
                    {
                        return next(nextElement, out value);
                    }

                    value = default;
                    return false;
                };
                continue;
            }

            var arrayIndex = step.ArrayIndex.GetValueOrDefault();
            traversal = (JsonElement element, out JsonElement value) =>
            {
                if (element.ValueKind == JsonValueKind.Array
                    && arrayIndex >= 0
                    && arrayIndex < element.GetArrayLength())
                {
                    return next(element[arrayIndex], out value);
                }

                value = default;
                return false;
            };
        }

        return traversal;
    }

    private static JsonPathLookupResult LookupJsonPathFromText(string jsonText, JsonPathSpec spec)
    {
        var bytes = Encoding.UTF8.GetBytes(jsonText);
        var reader = new Utf8JsonReader(bytes);
        if (!reader.Read())
            throw new JsonException("Invalid JSON payload.");

        if (!TryReadJsonPathElement(ref reader, spec.Steps, 0, out var value))
            return new JsonPathLookupResult(false, spec.Mode, JsonPathLookupFailure.NotFound, spec.Path, default);

        return new JsonPathLookupResult(true, spec.Mode, JsonPathLookupFailure.None, spec.Path, value);
    }

    private static bool TryParseJsonPathSpec(string path, out JsonPathSpec spec, out string? error)
    {
        if (_jsonPathSpecCache.TryGetValue(path, out var cached))
        {
            spec = cached.Spec;
            error = cached.Success ? null : "Cached invalid JSON path.";
            return cached.Success;
        }

        var success = TryParseJsonPathSpecCore(path, out spec, out error);
        CacheJsonPathSpec(path, new JsonPathSpecCacheEntry(success, spec));
        return success;
    }

    private static bool TryParseJsonPathSpecCore(string path, out JsonPathSpec spec, out string? error)
        => TryParseJsonPathSpecCore(path.AsSpan(), out spec, out error);

    private static bool TryParseJsonPathSpecCore(ReadOnlySpan<char> path, out JsonPathSpec spec, out string? error)
    {
        spec = default;
        error = null;

        var trimmed = path.Trim();
        if (trimmed.IsEmpty)
        {
            error = "JSON path is empty.";
            return false;
        }

        var mode = JsonPathMode.Lax;
        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
        {
            mode = JsonPathMode.Lax;
            trimmed = trimmed[4..].TrimStart();
        }
        else if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
        {
            mode = JsonPathMode.Strict;
            trimmed = trimmed[7..].TrimStart();
        }

        if (trimmed.Length == 0 || trimmed[0] != '$')
        {
            error = "JSON path must start with '$'.";
            return false;
        }

        var steps = new List<JsonPathStep>();
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
                if (!TryParseJsonPropertyStep(trimmed, ref i, out var propertyName, out error))
                    return false;

                steps.Add(new JsonPathStep(JsonPathStepKind.Property, propertyName, null));
                continue;
            }

            if (trimmed[i] == '[')
            {
                i++;
                if (!TryParseJsonBracketStep(trimmed, ref i, out var bracketStep, out error))
                    return false;

                steps.Add(bracketStep);
                continue;
            }

            error = $"Unexpected token '{trimmed[i]}' in JSON path.";
            return false;
        }

        spec = new JsonPathSpec(mode, trimmed.ToString(), [.. steps]);
        return true;
    }

    private static void CacheJsonPathSpec(string path, JsonPathSpecCacheEntry entry)
    {
        if (_jsonPathSpecCache.Count >= JsonPathCacheSoftLimit)
            _jsonPathSpecCache.Clear();

        _jsonPathSpecCache[path] = entry;
    }

    private static void CacheJsonRoot(string text, JsonRootCacheEntry entry)
    {
        if (_jsonRootCache.Count >= JsonRootCacheSoftLimit)
            _jsonRootCache.Clear();

        _jsonRootCache[text] = entry;
    }

    private static void CacheJsonPathResolver(string path, JsonPathResolverCacheEntry entry)
    {
        if (_jsonPathResolverCache.Count >= JsonPathResolverCacheSoftLimit)
            _jsonPathResolverCache.Clear();

        _jsonPathResolverCache[path] = entry;
    }

    private static bool TryReadJsonPathElement(
        JsonElement element,
        IReadOnlyList<JsonPathStep> steps,
        out JsonElement value)
    {
        value = element;
        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            if (step.Kind == JsonPathStepKind.Property)
            {
                if (value.ValueKind != JsonValueKind.Object
                    || !value.TryGetProperty(step.PropertyName!, out var nextProperty))
                {
                    value = default;
                    return false;
                }

                value = nextProperty;
                continue;
            }

            if (value.ValueKind != JsonValueKind.Array)
            {
                value = default;
                return false;
            }

            var index = step.ArrayIndex.GetValueOrDefault();
            if (index < 0 || index >= value.GetArrayLength())
            {
                value = default;
                return false;
            }

            value = value[index];
        }

        return true;
    }

    private static bool TryReadJsonPathElement(
        ref Utf8JsonReader reader,
        IReadOnlyList<JsonPathStep> steps,
        int stepIndex,
        out JsonElement value)
    {
        if (stepIndex >= steps.Count)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            value = document.RootElement.Clone();
            return true;
        }

        var step = steps[stepIndex];
        if (step.Kind == JsonPathStepKind.Property)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                value = default;
                reader.Skip();
                return false;
            }

            JsonElement matchedValue = default;
            var sawMatch = false;
            var matched = false;
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    value = matchedValue;
                    return sawMatch && matched;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new JsonException("Invalid JSON payload.");

                var propertyName = reader.GetString();
                if (!reader.Read())
                    throw new JsonException("Invalid JSON payload.");

                if (string.Equals(propertyName, step.PropertyName, StringComparison.Ordinal))
                {
                    sawMatch = true;
                    matched = TryReadJsonPathElement(ref reader, steps, stepIndex + 1, out var candidate);
                    matchedValue = matched ? candidate : default;

                    continue;
                }

                reader.Skip();
            }

            throw new JsonException("Invalid JSON payload.");
        }

        if (reader.TokenType != JsonTokenType.StartArray)
        {
            value = default;
            reader.Skip();
            return false;
        }

        var targetIndex = step.ArrayIndex.GetValueOrDefault();
        var currentIndex = 0;
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                value = default;
                return false;
            }

            if (currentIndex == targetIndex)
                return TryReadJsonPathElement(ref reader, steps, stepIndex + 1, out value);

            reader.Skip();
            currentIndex++;
        }

        throw new JsonException("Invalid JSON payload.");
    }

    private static bool TryParseJsonPropertyStep(
        ReadOnlySpan<char> path,
        ref int index,
        out string propertyName,
        out string? error)
    {
        propertyName = string.Empty;
        error = null;

        if (index >= path.Length)
        {
            error = "JSON path property name is missing.";
            return false;
        }

        if (path[index] == '"')
            return TryParseQuotedJsonProperty(path, ref index, out propertyName, out error);

        var start = index;
        while (index < path.Length && path[index] is not '.' and not '[' && !char.IsWhiteSpace(path[index]))
            index++;

        propertyName = path.Slice(start, index - start).ToString();
        if (propertyName.Length == 0)
        {
            error = "JSON path property name is missing.";
            return false;
        }

        return true;
    }

    private static bool TryParseQuotedJsonProperty(
        ReadOnlySpan<char> path,
        ref int index,
        out string propertyName,
        out string? error)
    {
        propertyName = string.Empty;
        error = null;

        index++; // opening quote
        var sb = new StringBuilder();
        while (index < path.Length)
        {
            var ch = path[index];
            if (ch == '\\' && index + 1 < path.Length)
            {
                sb.Append(path[index + 1]);
                index += 2;
                continue;
            }

            if (ch == '"')
            {
                index++;
                propertyName = sb.ToString();
                if (propertyName.Length == 0)
                {
                    error = "Quoted JSON path property name is empty.";
                    return false;
                }

                return true;
            }

            sb.Append(ch);
            index++;
        }

        error = "Quoted JSON path property is not closed.";
        return false;
    }

    private static bool TryParseJsonBracketStep(
        ReadOnlySpan<char> path,
        ref int index,
        out JsonPathStep step,
        out string? error)
    {
        step = default;
        error = null;

        if (index >= path.Length)
        {
            error = "JSON path bracket step is not closed.";
            return false;
        }

        if (path[index] == '"')
        {
            if (!TryParseQuotedJsonProperty(path, ref index, out var propertyName, out error))
                return false;

            if (index >= path.Length || path[index] != ']')
            {
                error = "JSON path bracket property is not closed.";
                return false;
            }

            index++;
            step = new JsonPathStep(JsonPathStepKind.Property, propertyName, null);
            return true;
        }

        var start = index;
        while (index < path.Length && char.IsDigit(path[index]))
            index++;

        if (start == index || index >= path.Length || path[index] != ']')
        {
            error = "JSON path array index is invalid.";
            return false;
        }

        if (!ReadOnlySpanCompatibility.TryParseInt32(path.Slice(start, index - start), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedIndex))
        {
            error = "JSON path array index is invalid.";
            return false;
        }

        index++; // closing bracket
        step = new JsonPathStep(JsonPathStepKind.ArrayIndex, null, parsedIndex);
        return true;
    }

    private static object? ConvertJsonElementToValue(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var integerValue) ? integerValue : element.GetDecimal(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.ToString()
        };

    private static bool IsNullish(object? value)
        => value is null or DBNull;
}
