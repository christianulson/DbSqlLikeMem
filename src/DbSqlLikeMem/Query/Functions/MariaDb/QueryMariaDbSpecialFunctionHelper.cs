using System.Text.Json.Nodes;

namespace DbSqlLikeMem;

internal static class QueryMariaDbSpecialFunctionHelper
{
    private static readonly HashSet<string> _dynamicColumnFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "COLUMN_CREATE",
        "COLUMN_ADD",
        "COLUMN_DELETE",
        "COLUMN_EXISTS",
        "COLUMN_CHECK",
        "COLUMN_JSON",
        "COLUMN_LIST",
        "COLUMN_GET"
    };

    private static readonly HashSet<string> _vectorFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "VECTOR",
        "VEC_FROMTEXT",
        "VEC_TOTEXT",
        "VEC_DISTANCE",
        "VEC_DISTANCE_EUCLIDEAN",
        "VEC_DISTANCE_COSINE"
    };

    private static readonly HashSet<string> _wsrepFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "WSREP_LAST_SEEN_GTID",
        "WSREP_LAST_WRITTEN_GTID",
        "WSREP_SYNC_WAIT_UPTO_GTID"
    };

    private delegate bool MariaDbSpecialFunctionHandler(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly IReadOnlyDictionary<string, MariaDbSpecialFunctionHandler> _handlers = CreateHandlers();

    public static bool TryEvalFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, evalArg, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, MariaDbSpecialFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, MariaDbSpecialFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalColumnCreateFunction, "COLUMN_CREATE");
        Register(handlers, TryEvalColumnAddFunction, "COLUMN_ADD");
        Register(handlers, TryEvalColumnDeleteFunction, "COLUMN_DELETE");
        Register(handlers, TryEvalColumnExistsFunction, "COLUMN_EXISTS");
        Register(handlers, TryEvalColumnCheckFunction, "COLUMN_CHECK");
        Register(handlers, TryEvalColumnJsonFunction, "COLUMN_JSON");
        Register(handlers, TryEvalColumnListFunction, "COLUMN_LIST");
        Register(handlers, TryEvalColumnGetFunction, "COLUMN_GET");
        Register(handlers, TryEvalVectorFunction, "VECTOR", "VEC_FROMTEXT");
        Register(handlers, TryEvalVecToTextFunction, "VEC_TOTEXT");
        Register(handlers, TryEvalVecDistanceFunction, "VEC_DISTANCE", "VEC_DISTANCE_EUCLIDEAN", "VEC_DISTANCE_COSINE");
        Register(handlers, TryEvalWsrepLastSeenOrWrittenFunction, "WSREP_LAST_SEEN_GTID", "WSREP_LAST_WRITTEN_GTID");
        Register(handlers, TryEvalWsrepSyncWaitFunction, "WSREP_SYNC_WAIT_UPTO_GTID");
        return handlers;
    }

    private static void Register(
        Dictionary<string, MariaDbSpecialFunctionHandler> handlers,
        MariaDbSpecialFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalColumnCheckFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = TryReadDynamicColumnObject(evalArg(0), out _) ? 1 : 0;
        return true;
    }

    private static bool TryEvalColumnExistsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("COLUMN_EXISTS() espera blob e nome de coluna.");

        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
        {
            result = 0;
            return true;
        }

        var key = ReadDynamicColumnKey(fn.Args[1], evalArg(1));
        result = obj.ContainsKey(key) ? 1 : 0;
        return true;
    }

    private static bool TryEvalColumnListFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
        {
            result = null;
            return true;
        }

        result = string.Join(",", obj.Select(static kvp => kvp.Key));
        return true;
    }

    private static bool TryEvalColumnJsonFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
        {
            result = null;
            return true;
        }

        result = obj.ToJsonString();
        return true;
    }

    private static bool TryEvalColumnCreateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0 || fn.Args.Count % 2 != 0)
            throw new InvalidOperationException("COLUMN_CREATE() espera pares nome/valor.");

        var obj = new JsonObject();
        for (var i = 0; i < fn.Args.Count; i += 2)
        {
            var key = ReadDynamicColumnKey(fn.Args[i], evalArg(i));
            obj[key] = ConvertToJsonNode(evalArg(i + 1));
        }

        result = SerializeDynamicColumns(obj);
        return true;
    }

    private static bool TryEvalColumnAddFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
            throw new InvalidOperationException("COLUMN_ADD() espera blob seguido de pares nome/valor.");

        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
            obj = new JsonObject();

        for (var i = 1; i < fn.Args.Count; i += 2)
        {
            var key = ReadDynamicColumnKey(fn.Args[i], evalArg(i));
            obj[key] = ConvertToJsonNode(evalArg(i + 1));
        }

        result = SerializeDynamicColumns(obj);
        return true;
    }

    private static bool TryEvalColumnDeleteFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("COLUMN_DELETE() espera blob e ao menos um nome.");

        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            var key = ReadDynamicColumnKey(fn.Args[i], evalArg(i));
            obj.Remove(key);
        }

        result = SerializeDynamicColumns(obj);
        return true;
    }

    private static bool TryEvalColumnGetFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("COLUMN_GET() espera blob e nome.");

        if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
        {
            result = null;
            return true;
        }

        var key = ReadDynamicColumnKey(fn.Args[1], evalArg(1));
        if (!obj.TryGetPropertyValue(key, out var value))
        {
            result = null;
            return true;
        }

        result = ConvertFromJsonNode(value);
        return true;
    }

    private static bool TryEvalVectorFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = TryParseVector(evalArg(0), out var vector) ? vector : null;
        return true;
    }

    private static bool TryEvalVecToTextFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = TryParseVector(evalArg(0), out var vector) ? VectorToText(vector) : null;
        return true;
    }

    private static bool TryEvalVecDistanceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera dois vetores.");

        if (!TryParseVector(evalArg(0), out var left) || !TryParseVector(evalArg(1), out var right))
        {
            result = null;
            return true;
        }

        result = fn.Name.Equals("VEC_DISTANCE_COSINE", StringComparison.OrdinalIgnoreCase)
            ? VectorDistanceCosine(left, right)
            : VectorDistanceEuclidean(left, right);
        return true;
    }

    private static bool TryEvalWsrepLastSeenOrWrittenFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = "0-0-0";
        return true;
    }

    private static bool TryEvalWsrepSyncWaitFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 1)
            throw new InvalidOperationException("WSREP_SYNC_WAIT_UPTO_GTID() espera um GTID.");

        var gtid = evalArg(0)?.ToString() ?? string.Empty;
        if (!IsValidGtid(gtid))
        {
            result = 0;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryReadDynamicColumnObject(object? value, out JsonObject obj)
    {
        obj = new JsonObject();
        if (value is null or DBNull)
            return true;

        var text = value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            JsonElement element => element.GetRawText(),
            JsonNode node => node.ToJsonString(),
            _ => value.ToString() ?? string.Empty
        };

        if (string.IsNullOrWhiteSpace(text))
            return true;

        try
        {
            var node = JsonNode.Parse(text);
            if (node is JsonObject jsonObject)
            {
                obj = jsonObject;
                return true;
            }
        }
        catch
        {
        }

        return false;
    }

    private static byte[] SerializeDynamicColumns(JsonObject obj)
        => Encoding.UTF8.GetBytes(obj.ToJsonString());

    private static string ReadDynamicColumnKey(SqlExpr expr, object? value)
    {
        if (expr is IdentifierExpr identifier)
            return identifier.Name;

        if (expr is LiteralExpr { Value: not null } literal)
            return literal.Value.ToString() ?? string.Empty;

        if (expr is RawSqlExpr raw)
        {
            var text = raw.Sql.Trim();
            var asIndex = text.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
            if (asIndex >= 0)
                text = text[..asIndex].TrimEnd();

            var spaceIndex = text.IndexOf(' ');
            if (spaceIndex >= 0)
                text = text[..spaceIndex];

            return text.Trim('`', '"', '\'');
        }

        return value?.ToString() ?? string.Empty;
    }

    private static JsonNode? ConvertToJsonNode(object? value)
    {
        if (value is null or DBNull)
            return null;

        if (value is JsonNode node)
            return node.DeepClone();

        if (value is JsonElement element)
            return JsonNode.Parse(element.GetRawText());

        if (value is byte[] bytes)
            return JsonNode.Parse(Encoding.UTF8.GetString(bytes));

        if (value is string text)
        {
            try
            {
                return JsonNode.Parse(text);
            }
            catch
            {
                return JsonValue.Create(text);
            }
        }

        return JsonValue.Create(value);
    }

    private static object? ConvertFromJsonNode(JsonNode? node)
    {
        if (node is null)
            return null;

        return node switch
        {
            JsonValue jsonValue when jsonValue.TryGetValue<string>(out var text) => text,
            JsonValue jsonValue when jsonValue.TryGetValue<long>(out var longValue) => longValue,
            JsonValue jsonValue when jsonValue.TryGetValue<decimal>(out var decimalValue) => decimalValue,
            JsonValue jsonValue when jsonValue.TryGetValue<double>(out var doubleValue) => doubleValue,
            JsonValue jsonValue when jsonValue.TryGetValue<bool>(out var boolValue) => boolValue,
            _ => node.ToJsonString()
        };
    }

    private static bool TryParseVector(object? value, out float[] vector)
    {
        vector = [];
        if (value is null or DBNull)
            return false;

        if (value is byte[] bytes)
        {
            if (bytes.Length % 4 != 0)
                return false;

            vector = new float[bytes.Length / 4];
            for (var i = 0; i < vector.Length; i++)
                vector[i] = BitConverter.ToSingle(bytes, i * 4);
            return true;
        }

        var text = value.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        try
        {
            var node = JsonNode.Parse(text);
            if (node is not JsonArray array)
                return false;

            var values = new List<float>(array.Count);
            foreach (var item in array)
            {
                if (item is null)
                    return false;

                if (!float.TryParse(item.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
                    return false;

                values.Add(parsed);
            }

            vector = [.. values];
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string VectorToText(float[] vector)
        => "[" + string.Join(",", vector.Select(static value => value.ToString("R", CultureInfo.InvariantCulture))) + "]";

    private static double VectorDistanceEuclidean(float[] left, float[] right)
    {
        if (left.Length != right.Length)
            throw new InvalidOperationException("VEC_DISTANCE espera vetores do mesmo tamanho.");

        double sum = 0;
        for (var i = 0; i < left.Length; i++)
        {
            var delta = left[i] - right[i];
            sum += delta * delta;
        }

        return Math.Sqrt(sum);
    }

    private static double VectorDistanceCosine(float[] left, float[] right)
    {
        if (left.Length != right.Length)
            throw new InvalidOperationException("VEC_DISTANCE_COSINE espera vetores do mesmo tamanho.");

        double dot = 0;
        double leftNorm = 0;
        double rightNorm = 0;
        for (var i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            leftNorm += left[i] * left[i];
            rightNorm += right[i] * right[i];
        }

        if (leftNorm == 0 || rightNorm == 0)
            return 0;

        return 1 - (dot / (Math.Sqrt(leftNorm) * Math.Sqrt(rightNorm)));
    }

    private static bool IsValidGtid(string gtid)
        => !string.IsNullOrWhiteSpace(gtid)
        && Regex.IsMatch(gtid, @"^\d+-\d+-\d+$", RegexOptions.CultureInvariant);
}
