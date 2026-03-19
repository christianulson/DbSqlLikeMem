using System.Text.Json.Nodes;

namespace DbSqlLikeMem;

internal static class QueryMariaDbSpecialFunctionHelper
{
    public static bool TryEvalFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!dialect.Name.Equals("mariadb", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        return TryEvalDynamicColumnFunctions(fn, evalArg, out result)
            || TryEvalVectorFunctions(fn, evalArg, out result)
            || TryEvalWsrepFunctions(fn, evalArg, out result);
    }

    private static bool TryEvalDynamicColumnFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("COLUMN_CREATE" or "COLUMN_ADD" or "COLUMN_DELETE" or "COLUMN_EXISTS" or "COLUMN_CHECK" or "COLUMN_JSON" or "COLUMN_LIST" or "COLUMN_GET"))
        {
            result = null;
            return false;
        }

        if (name == "COLUMN_CHECK")
        {
            result = TryReadDynamicColumnObject(evalArg(0), out _) ? 1 : 0;
            return true;
        }

        if (name == "COLUMN_EXISTS")
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

        if (name == "COLUMN_LIST")
        {
            if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
            {
                result = null;
                return true;
            }

            result = string.Join(",", obj.Select(static kvp => kvp.Key));
            return true;
        }

        if (name == "COLUMN_JSON")
        {
            if (!TryReadDynamicColumnObject(evalArg(0), out var obj))
            {
                result = null;
                return true;
            }

            result = obj.ToJsonString();
            return true;
        }

        if (name == "COLUMN_CREATE")
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

        if (name == "COLUMN_ADD")
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

        if (name == "COLUMN_DELETE")
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

        if (name == "COLUMN_GET")
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

        result = null;
        return false;
    }

    private static bool TryEvalVectorFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("VECTOR" or "VEC_FROMTEXT" or "VEC_TOTEXT" or "VEC_DISTANCE" or "VEC_DISTANCE_EUCLIDEAN" or "VEC_DISTANCE_COSINE"))
        {
            result = null;
            return false;
        }

        if (name is "VECTOR" or "VEC_FROMTEXT")
        {
            result = TryParseVector(evalArg(0), out var vector) ? vector : null;
            return true;
        }

        if (name == "VEC_TOTEXT")
        {
            result = TryParseVector(evalArg(0), out var vector) ? VectorToText(vector) : null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{name}() espera dois vetores.");

        if (!TryParseVector(evalArg(0), out var left) || !TryParseVector(evalArg(1), out var right))
        {
            result = null;
            return true;
        }

        result = name switch
        {
            "VEC_DISTANCE_COSINE" => VectorDistanceCosine(left, right),
            _ => VectorDistanceEuclidean(left, right)
        };
        return true;
    }

    private static bool TryEvalWsrepFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("WSREP_LAST_SEEN_GTID" or "WSREP_LAST_WRITTEN_GTID" or "WSREP_SYNC_WAIT_UPTO_GTID"))
        {
            result = null;
            return false;
        }

        if (name is "WSREP_LAST_SEEN_GTID" or "WSREP_LAST_WRITTEN_GTID")
        {
            result = "0-0-0";
            return true;
        }

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
        && System.Text.RegularExpressions.Regex.IsMatch(gtid, @"^\d+-\d+-\d+$", System.Text.RegularExpressions.RegexOptions.CultureInvariant);
}
