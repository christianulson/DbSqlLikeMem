namespace DbSqlLikeMem;

internal static class QueryMariaDbFunctionHelper
{
    private static readonly Lazy<uint[]> _crc32cTable = new(CreateCrc32cTable);
    private static readonly IReadOnlyDictionary<string, MariaDbFunctionHandler> _handlers = CreateHandlers();
    private static readonly IReadOnlyDictionary<string, MariaDbJsonFunctionHandler> _jsonHandlers = CreateJsonHandlers();

    private delegate bool MariaDbFunctionHandler(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    private delegate bool MariaDbJsonFunctionHandler(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    public static bool TryEvalFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, MariaDbFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, MariaDbFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalBenchmarkFunction, "BENCHMARK");
        Register(handlers, TryEvalFieldFunction, "FIELD");
        Register(handlers, TryEvalLengthBFunction, "LENGTHB");
        Register(handlers, TryEvalDecodeOracleFunction, "DECODE_ORACLE");
        Register(handlers, TryEvalCrc32cFunction, "CRC32C");
        Register(handlers, TryEvalNaturalSortKeyFunction, "NATURAL_SORT_KEY");
        Register(handlers, TryEvalSFormatFunction, "SFORMAT");
        Register(handlers, TryEvalKdfFunction, "KDF");
        Register(handlers, TryEvalTrimOracleFunction, "TRIM_ORACLE");
        Register(handlers, TryEvalWeightStringFunction, "WEIGHT_STRING");
        Register(
            handlers,
            TryEvalJsonFunctions,
            "JSON_COMPACT",
            "JSON_PRETTY",
            "JSON_DETAILED",
            "JSON_LOOSE",
            "JSON_NORMALIZE",
            "JSON_EQUALS",
            "JSON_EXISTS",
            "JSON_SCHEMA_VALID",
            "JSON_ARRAY_INTERSECT",
            "JSON_OBJECT_FILTER_KEYS",
            "JSON_OBJECT_TO_ARRAY",
            "JSON_KEY_VALUE");
        return handlers;
    }

    private static Dictionary<string, MariaDbJsonFunctionHandler> CreateJsonHandlers()
    {
        var handlers = new Dictionary<string, MariaDbJsonFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalJsonCompactFunction, "JSON_COMPACT");
        Register(handlers, TryEvalJsonPrettyFunction, "JSON_PRETTY", "JSON_DETAILED");
        Register(handlers, TryEvalJsonLooseFunction, "JSON_LOOSE");
        Register(handlers, TryEvalJsonNormalizeFunction, "JSON_NORMALIZE");
        Register(handlers, TryEvalJsonEqualsFunction, "JSON_EQUALS");
        Register(handlers, TryEvalJsonExistsFunction, "JSON_EXISTS");
        Register(handlers, TryEvalJsonSchemaValidFunction, "JSON_SCHEMA_VALID");
        Register(handlers, TryEvalJsonArrayIntersectFunction, "JSON_ARRAY_INTERSECT");
        Register(handlers, TryEvalJsonObjectFilterKeysFunction, "JSON_OBJECT_FILTER_KEYS");
        Register(handlers, TryEvalJsonObjectToArrayFunction, "JSON_OBJECT_TO_ARRAY");
        Register(handlers, TryEvalJsonKeyValueFunction, "JSON_KEY_VALUE");
        return handlers;
    }

    private static void Register(
        Dictionary<string, MariaDbFunctionHandler> handlers,
        MariaDbFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static void Register(
        Dictionary<string, MariaDbJsonFunctionHandler> handlers,
        MariaDbJsonFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal static bool TryEvalBenchmarkFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BENCHMARK() espera contagem e expressao.");

        var countValue = evalArg(0);
        if (IsNullish(countValue))
        {
            result = null;
            return true;
        }

        if (!TryConvertToInt64(countValue!, out var count) || count <= 0)
        {
            result = 0;
            return true;
        }

        for (var i = 0L; i < count; i++)
            _ = evalArg(1);

        result = 0;
        return true;
    }

    private static bool TryEvalFieldFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FIELD() espera ao menos dois argumentos.");

        var candidate = evalArg(0);
        if (IsNullish(candidate))
        {
            result = 0;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            var current = evalArg(i);
            if (IsNullish(current))
                continue;

            if (ValuesAreEqual(candidate, current))
            {
                result = i;
                return true;
            }
        }

        result = 0;
        return true;
    }

    private static bool TryEvalLengthBFunction(
        this QueryExecutionContext context,
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

        if (value is byte[] bytes)
        {
            result = bytes.Length;
            return true;
        }

        result = Encoding.UTF8.GetByteCount(value!.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvalDecodeOracleFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DECODE_ORACLE() espera ao menos 3 argumentos.");

        var expr = evalArg(0);
        var pairCount = (fn.Args.Count - 1) / 2;
        var hasDefault = (fn.Args.Count - 1) % 2 == 1;

        for (var i = 0; i < pairCount; i++)
        {
            var search = evalArg(1 + i * 2);
            var current = evalArg(2 + i * 2);
            if (ValuesAreEqual(expr, search, context))
            {
                result = current;
                return true;
            }
        }

        result = hasDefault ? evalArg(fn.Args.Count - 1) : null;
        return true;
    }

    private static bool TryEvalCrc32cFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count is 0 or > 2)
            throw new InvalidOperationException("CRC32C() espera um ou dois argumentos.");

        uint seed = uint.MaxValue;
        if (fn.Args.Count == 2)
        {
            var seedValue = evalArg(0);
            if (!IsNullish(seedValue) && TryConvertToUInt32(seedValue!, out var parsedSeed))
                seed = parsedSeed ^ uint.MaxValue;
        }

        var payloadValue = evalArg(fn.Args.Count == 2 ? 1 : 0);
        if (IsNullish(payloadValue))
        {
            result = null;
            return true;
        }

        var bytes = payloadValue is byte[] payloadBytes
            ? payloadBytes
            : Encoding.UTF8.GetBytes(payloadValue!.ToString() ?? string.Empty);

        var crc = seed;
        foreach (var b in bytes)
        {
            var index = (crc ^ b) & 0xFF;
            crc = (crc >> 8) ^ _crc32cTable.Value[index];
        }

        result = crc ^ uint.MaxValue;
        return true;
    }

    private static bool TryEvalNaturalSortKeyFunction(
        this QueryExecutionContext context,
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

        var text = value!.ToString() ?? string.Empty;
        var sb = new StringBuilder(text.Length * 2);
        var i = 0;
        while (i < text.Length)
        {
            if (char.IsDigit(text[i]))
            {
                var start = i;
                while (i < text.Length && char.IsDigit(text[i]))
                    i++;

                var digits = text[start..i].TrimStart('0');
                if (digits.Length == 0)
                    digits = "0";

                sb.Append('#');
                sb.Append(digits.Length.ToString("D5", CultureInfo.InvariantCulture));
                sb.Append(':');
                sb.Append(digits);
                continue;
            }

            var segmentStart = i;
            while (i < text.Length && !char.IsDigit(text[i]))
                i++;

            sb.Append(text[segmentStart..i].ToUpperInvariant());
            sb.Append('|');
        }

        result = sb.ToString();
        return true;
    }

    private static bool TryEvalSFormatFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = string.Empty;
            return true;
        }

        var format = evalArg(0)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(format))
        {
            result = string.Empty;
            return true;
        }

        var args = new object?[Math.Max(0, fn.Args.Count - 1)];
        for (var i = 1; i < fn.Args.Count; i++)
            args[i - 1] = evalArg(i);

        result = ApplyBraceFormat(format, args);
        return true;
    }

    private static bool TryEvalKdfFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("KDF() espera pelo menos key_str e salt.");

        var keyText = evalArg(0)?.ToString() ?? string.Empty;
        var saltText = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(keyText) || string.IsNullOrWhiteSpace(saltText))
        {
            result = null;
            return true;
        }

        var kdfName = fn.Args.Count > 3 ? evalArg(3)?.ToString() : null;
        var widthBits = 128;
        if (fn.Args.Count > 4 && TryConvertToInt32(evalArg(4), out var widthValue))
            widthBits = widthValue;
        if (widthBits <= 0 || widthBits % 8 != 0)
            throw new InvalidOperationException("KDF() espera largura em bits divisivel por 8.");

        var outputLength = widthBits / 8;
        var algorithm = string.IsNullOrWhiteSpace(kdfName) ? "pbkdf2_hmac" : kdfName!.Trim();

        if (algorithm.Equals("hkdf", StringComparison.OrdinalIgnoreCase))
        {
            var infoText = fn.Args.Count > 2 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
            result = DeriveHkdfSha256(keyText, saltText, infoText, outputLength);
            return true;
        }

        if (!algorithm.Equals("pbkdf2_hmac", StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.NotSupported(context.Dialect, "KDF");

        var iterations = 1000;
        if (fn.Args.Count > 2 && TryConvertToInt32(evalArg(2), out var parsedIterations))
            iterations = parsedIterations;
        if (iterations <= 0)
            throw new InvalidOperationException("KDF() espera iterations positivas.");

#if NET6_0_OR_GREATER

        result = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(keyText),
            Encoding.UTF8.GetBytes(saltText),
            iterations,
            HashAlgorithmName.SHA256,
            outputLength);
#else
        using var pbkdf2 = new Rfc2898DeriveBytes(
            keyText,
            Encoding.UTF8.GetBytes(saltText),
            iterations);
        result = pbkdf2.GetBytes(outputLength);
#endif
        return true;
    }

    private static byte[] DeriveHkdfSha256(string keyText, string saltText, string infoText, int outputLength)
    {
        var ikm = Encoding.UTF8.GetBytes(keyText);
        var salt = Encoding.UTF8.GetBytes(saltText);
        var info = Encoding.UTF8.GetBytes(infoText ?? string.Empty);

        using var hmacExtract = new HMACSHA256(salt);
        var prk = hmacExtract.ComputeHash(ikm);

        var result = new byte[outputLength];
        var generated = 0;
        byte counter = 1;
        var previous = Array.Empty<byte>();
        while (generated < outputLength)
        {
            using var hmacExpand = new HMACSHA256(prk);
            var buffer = new byte[previous.Length + info.Length + 1];
            if (previous.Length > 0)
                Buffer.BlockCopy(previous, 0, buffer, 0, previous.Length);
            if (info.Length > 0)
                Buffer.BlockCopy(info, 0, buffer, previous.Length, info.Length);
            buffer[^1] = counter++;
            previous = hmacExpand.ComputeHash(buffer);

            var take = Math.Min(previous.Length, outputLength - generated);
            Buffer.BlockCopy(previous, 0, result, generated, take);
            generated += take;
        }

        return result;
    }

    private static bool TryEvalTrimOracleFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var source = evalArg(fn.Args.Count - 1);
        if (IsNullish(source))
        {
            result = null;
            return true;
        }

        var text = source!.ToString() ?? string.Empty;
        if (fn.Args.Count == 1)
        {
            result = text.Trim();
            return true;
        }

        var trimChars = evalArg(0)?.ToString();
        result = string.IsNullOrWhiteSpace(trimChars)
            ? text.Trim()
            : text.Trim(trimChars!.ToCharArray());
        return true;
    }

    private static bool TryEvalWeightStringFunction(
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

        if (value is byte[] bytes)
        {
            result = bytes.ToArray();
            return true;
        }

        result = Encoding.UTF8.GetBytes((value!.ToString() ?? string.Empty).ToUpperInvariant());
        return true;
    }

    private static bool TryEvalJsonFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_jsonHandlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalJsonCompactFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        if (!TryParseJsonElement(evalArg(0), out var element))
        {
            result = null;
            return true;
        }

        result = element.GetRawText();
        return true;
    }

    private static bool TryEvalJsonPrettyFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        if (!TryParseJsonElement(evalArg(0), out var element))
        {
            result = null;
            return true;
        }

        result = JsonSerializer.Serialize(element, new JsonSerializerOptions { WriteIndented = true });
        return true;
    }

    private static bool TryEvalJsonLooseFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryParseJsonElement(evalArg(0), out var element))
        {
            result = null;
            return true;
        }

        result = JsonSerializer.Serialize(element, new JsonSerializerOptions { WriteIndented = true });
        return true;
    }

    private static bool TryEvalJsonNormalizeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryParseJsonElement(evalArg(0), out var element))
        {
            result = null;
            return true;
        }

        result = NormalizeJsonElement(element);
        return true;
    }

    private static bool TryEvalJsonEqualsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_EQUALS() espera dois JSONs.");

        if (!TryParseJsonElement(evalArg(0), out var left)
            || !TryParseJsonElement(evalArg(1), out var right))
        {
            result = null;
            return true;
        }

        result = JsonElementEquals(left, right) ? 1 : 0;
        return true;
    }

    private static bool TryEvalJsonExistsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_EXISTS() espera um JSON e um path.");

        var json = evalArg(0);
        var path = evalArg(1)?.ToString();
        if (json is null or DBNull || string.IsNullOrWhiteSpace(path))
        {
            result = null;
            return true;
        }

        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
        {
            result = 0;
            return true;
        }

        result = QueryJsonFunctionHelper.TryReadJsonPathElement(root, path!, out _) ? 1 : 0;
        return true;
    }

    private static bool TryEvalJsonSchemaValidFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_SCHEMA_VALID() espera documento e schema.");

        if (!TryParseJsonElement(evalArg(0), out var document)
            || !TryParseJsonElement(evalArg(1), out var schema))
        {
            result = null;
            return true;
        }

        result = ValidateJsonSchema(document, schema) ? 1 : 0;
        return true;
    }

    private static bool TryEvalJsonArrayIntersectFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("JSON_ARRAY_INTERSECT() espera dois arrays JSON.");

        if (!TryParseJsonElement(evalArg(0), out var left)
            || !TryParseJsonElement(evalArg(1), out var right)
            || left.ValueKind != JsonValueKind.Array
            || right.ValueKind != JsonValueKind.Array)
        {
            result = null;
            return true;
        }

        var rightValues = right.EnumerateArray().Select(NormalizeJsonElement).ToHashSet(StringComparer.Ordinal);
        var intersected = new List<object?>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in left.EnumerateArray())
        {
            var normalized = NormalizeJsonElement(item);
            if (rightValues.Contains(normalized) && seen.Add(normalized))
                intersected.Add(ConvertJsonElementToValue(item));
        }

        result = BuildJsonArray(intersected);
        return true;
    }

    private static bool TryEvalJsonObjectFilterKeysFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryParseJsonElement(evalArg(0), out var element) || element.ValueKind != JsonValueKind.Object)
        {
            result = null;
            return true;
        }

        var keys = ReadJsonStringSet(evalArg(1));
        var pairs = new List<(string Key, object? Value)>();
        foreach (var prop in element.EnumerateObject())
        {
            if (keys.Contains(prop.Name))
                pairs.Add((prop.Name, ConvertJsonElementToValue(prop.Value)));
        }

        result = BuildJsonObject(pairs);
        return true;
    }

    private static bool TryEvalJsonObjectToArrayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryParseJsonElement(evalArg(0), out var element) || element.ValueKind != JsonValueKind.Object)
        {
            result = null;
            return true;
        }

        var rows = new List<object?>();
        foreach (var prop in element.EnumerateObject())
            rows.Add(BuildJsonArray([prop.Name, ConvertJsonElementToValue(prop.Value)]));

        result = BuildJsonArray(rows);
        return true;
    }

    private static bool TryEvalJsonKeyValueFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryParseJsonElement(evalArg(0), out var element))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count > 1)
        {
            var path = evalArg(1)?.ToString();
            if (string.IsNullOrWhiteSpace(path)
                || !QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
            {
                result = null;
                return true;
            }

            element = pathElement;
        }

        if (element.ValueKind != JsonValueKind.Object)
        {
            result = null;
            return true;
        }

        var rows = new List<object?>();
        foreach (var prop in element.EnumerateObject())
        {
            rows.Add(BuildJsonObject(
            [
                ("key", (object?)prop.Name),
                ("value", ConvertJsonElementToValue(prop.Value))
            ]));
        }

        result = BuildJsonArray(rows);
        return true;
    }

    private static bool TryParseJsonElement(object? value, out JsonElement element)
    {
        element = default;
        if (value is null or DBNull)
            return false;

        return QueryJsonFunctionHelper.TryGetJsonRootElement(value, out element);
    }

    private static string NormalizeJsonElement(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.Object => "{" + string.Join(",",
                element.EnumerateObject()
                    .OrderBy(static prop => prop.Name, StringComparer.Ordinal)
                    .Select(static prop => JsonSerializer.Serialize(prop.Name) + ":" + NormalizeJsonElement(prop.Value))) + "}",
            JsonValueKind.Array => "[" + string.Join(",", element.EnumerateArray().Select(NormalizeJsonElement)) + "]",
            JsonValueKind.String => JsonSerializer.Serialize(element.GetString() ?? string.Empty),
            JsonValueKind.Number => NormalizeJsonNumber(element.GetRawText()),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => element.GetRawText()
        };

    private static string NormalizeJsonNumber(string raw)
    {
        if (decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
            return decimalValue.ToString(CultureInfo.InvariantCulture);

        if (double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var doubleValue))
            return doubleValue.ToString("R", CultureInfo.InvariantCulture);

        return raw;
    }

    private static bool JsonElementEquals(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == JsonValueKind.Number && right.ValueKind == JsonValueKind.Number)
                return NormalizeJsonNumber(left.GetRawText()) == NormalizeJsonNumber(right.GetRawText());

            return false;
        }

        return left.ValueKind switch
        {
            JsonValueKind.Object => JsonObjectEquals(left, right),
            JsonValueKind.Array => JsonArrayEquals(left, right),
            JsonValueKind.String => string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal),
            JsonValueKind.Number => NormalizeJsonNumber(left.GetRawText()) == NormalizeJsonNumber(right.GetRawText()),
            JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null => true,
            _ => string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal)
        };
    }

    private static bool JsonObjectEquals(JsonElement left, JsonElement right)
    {
        var leftProps = left.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
        var rightProps = right.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
        if (leftProps.Count != rightProps.Count)
            return false;

        foreach (var kvp in leftProps)
        {
            if (!rightProps.TryGetValue(kvp.Key, out var rightValue))
                return false;

            if (!JsonElementEquals(kvp.Value, rightValue))
                return false;
        }

        return true;
    }

    private static bool JsonArrayEquals(JsonElement left, JsonElement right)
    {
        var leftItems = left.EnumerateArray().ToArray();
        var rightItems = right.EnumerateArray().ToArray();
        if (leftItems.Length != rightItems.Length)
            return false;

        for (var i = 0; i < leftItems.Length; i++)
        {
            if (!JsonElementEquals(leftItems[i], rightItems[i]))
                return false;
        }

        return true;
    }

    private static bool ValidateJsonSchema(JsonElement document, JsonElement schema)
    {
        if (schema.ValueKind != JsonValueKind.Object)
            return true;

        if (schema.TryGetProperty("type", out var typeNode))
        {
            if (typeNode.ValueKind == JsonValueKind.String)
            {
                var typeName = typeNode.GetString() ?? string.Empty;
                if (!SchemaTypeMatches(document, typeName))
                    return false;
            }
        }

        if (schema.TryGetProperty("enum", out var enumNode) && enumNode.ValueKind == JsonValueKind.Array)
        {
            var matched = enumNode.EnumerateArray().Any(candidate => JsonElementEquals(candidate, document));
            if (!matched)
                return false;
        }

        if (schema.TryGetProperty("required", out var requiredNode)
            && requiredNode.ValueKind == JsonValueKind.Array
            && document.ValueKind == JsonValueKind.Object)
        {
            var documentProps = document.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
            foreach (var required in requiredNode.EnumerateArray())
            {
                if (required.ValueKind != JsonValueKind.String)
                    continue;

                var requiredName = required.GetString() ?? string.Empty;
                if (!documentProps.ContainsKey(requiredName))
                    return false;
            }
        }

        if (schema.TryGetProperty("properties", out var propertiesNode)
            && propertiesNode.ValueKind == JsonValueKind.Object
            && document.ValueKind == JsonValueKind.Object)
        {
            var documentProps = document.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
            foreach (var prop in propertiesNode.EnumerateObject())
            {
                if (!documentProps.TryGetValue(prop.Name, out var documentProperty))
                    continue;

                if (!ValidateJsonSchema(documentProperty, prop.Value))
                    return false;
            }
        }

        if (schema.TryGetProperty("items", out var itemsNode)
            && document.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in document.EnumerateArray())
            {
                if (!ValidateJsonSchema(item, itemsNode))
                    return false;
            }
        }

        return true;
    }

    private static bool SchemaTypeMatches(JsonElement document, string typeName)
    {
        typeName = typeName.Trim().ToLowerInvariant();
        return typeName switch
        {
            "object" => document.ValueKind == JsonValueKind.Object,
            "array" => document.ValueKind == JsonValueKind.Array,
            "string" => document.ValueKind == JsonValueKind.String,
            "integer" => document.ValueKind == JsonValueKind.Number && document.TryGetInt64(out _),
            "number" => document.ValueKind == JsonValueKind.Number,
            "boolean" => document.ValueKind is JsonValueKind.True or JsonValueKind.False,
            "null" => document.ValueKind == JsonValueKind.Null,
            _ => true
        };
    }

    private static object? ConvertJsonElementToValue(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var i) ? i : element.GetDecimal(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };

    private static IReadOnlyCollection<string> ReadJsonStringSet(object? value)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        if (value is null or DBNull)
            return set;

        if (value is JsonElement element && element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String)
                    set.Add(item.GetString() ?? string.Empty);
            }

            return set;
        }

        if (value is JsonElement scalar && scalar.ValueKind == JsonValueKind.String)
        {
            set.Add(scalar.GetString() ?? string.Empty);
            return set;
        }

        foreach (var part in value.ToString()!
            .Split(',')
            .Select(_ => _.Trim())
            .Where(_ => !string.IsNullOrWhiteSpace(_)))
            set.Add(part);

        return set;
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
            if (pair.Value is null or DBNull)
                return $"{key}:null";

            if (pair.Value is JsonElement element)
                return $"{key}:{element.GetRawText()}";

            return $"{key}:{JsonSerializer.Serialize(pair.Value)}";
        });

        return "{" + string.Join(",", parts) + "}";
    }

    private static string ApplyBraceFormat(string format, object?[] args)
    {
        var sb = new StringBuilder(format.Length + args.Length * 8);
        var argIndex = 0;

        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch == '{')
            {
                if (i + 1 < format.Length && format[i + 1] == '{')
                {
                    sb.Append('{');
                    i++;
                    continue;
                }

                var end = format.IndexOf('}', i + 1);
                if (end < 0)
                {
                    sb.Append(ch);
                    continue;
                }

                if (argIndex < args.Length)
                    sb.Append(args[argIndex++]?.ToString() ?? string.Empty);

                i = end;
                continue;
            }

            if (ch == '}' && i + 1 < format.Length && format[i + 1] == '}')
            {
                sb.Append('}');
                i++;
                continue;
            }

            sb.Append(ch);
        }

        return sb.ToString();
    }

    private static bool ValuesAreEqual(object? left, object? right)
    {
        if (IsNullish(left) && IsNullish(right))
            return true;

        if (IsNullish(left) || IsNullish(right))
            return false;

        if (TryConvertToDecimal(left!, out var leftDecimal) && TryConvertToDecimal(right!, out var rightDecimal))
            return leftDecimal == rightDecimal;

        return string.Equals(Convert.ToString(left, CultureInfo.InvariantCulture), Convert.ToString(right, CultureInfo.InvariantCulture), StringComparison.Ordinal);
    }

    private static bool ValuesAreEqual(object? left, object? right,
        QueryExecutionContext context)
    {
        if (IsNullish(left) && IsNullish(right))
            return true;

        if (IsNullish(left) || IsNullish(right))
            return false;

        if (TryConvertToDecimal(left!, out var leftDecimal) && TryConvertToDecimal(right!, out var rightDecimal))
            return leftDecimal == rightDecimal;

        return left!.EqualsSql(right!, context);
    }

    private static bool TryConvertToDecimal(object value, out decimal numeric)
    {
        switch (value)
        {
            case decimal d:
                numeric = d;
                return true;
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                numeric = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case float f:
                numeric = (decimal)f;
                return true;
            case double dbl:
                numeric = (decimal)dbl;
                return true;
        }

        return decimal.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Any, CultureInfo.InvariantCulture, out numeric);
    }

    private static bool TryConvertToInt64(object value, out long numeric)
    {
        numeric = 0;
        switch (value)
        {
            case byte b:
                numeric = b;
                return true;
            case sbyte sb:
                numeric = sb;
                return true;
            case short s:
                numeric = s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i:
                numeric = i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l:
                numeric = l;
                return true;
            case ulong ul when ul <= long.MaxValue:
                numeric = (long)ul;
                return true;
        }

        return long.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    private static bool TryConvertToInt32(object? value, out int numeric)
    {
        if (value is null or DBNull)
        {
            numeric = 0;
            return false;
        }

        if (value is int i)
        {
            numeric = i;
            return true;
        }

        if (int.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric))
            return true;

        numeric = 0;
        return false;
    }

    private static bool TryConvertToUInt32(object value, out uint numeric)
    {
        numeric = 0;
        switch (value)
        {
            case byte b:
                numeric = b;
                return true;
            case sbyte sb when sb >= 0:
                numeric = (uint)sb;
                return true;
            case short s when s >= 0:
                numeric = (uint)s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i when i >= 0:
                numeric = (uint)i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l when l >= 0 && l <= uint.MaxValue:
                numeric = (uint)l;
                return true;
            case ulong ul when ul <= uint.MaxValue:
                numeric = (uint)ul;
                return true;
        }

        return uint.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    private static uint[] CreateCrc32cTable()
    {
        var table = new uint[256];
        for (var i = 0; i < table.Length; i++)
        {
            var crc = (uint)i;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0
                    ? (crc >> 1) ^ 0x82F63B78u
                    : crc >> 1;
            }

            table[i] = crc;
        }

        return table;
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
