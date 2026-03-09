using System.Globalization;
using System.Text.Json;

namespace DbSqlLikeMem;

internal static class QueryJsonFunctionHelper
{
    internal static object? ApplyJsonValueReturningClause(FunctionCallExpr fn, object? value)
    {
        if (IsNullish(value) || !TryGetJsonReturningTypeSql(fn, out var normalizedType))
            return value;

        return ConvertJsonValueForReturningType(value, normalizedType);
    }

    internal static object? TryReadJsonPathValue(object json, string path)
    {
        if (!TryParseSimpleJsonPathSegments(path, out var segments))
            return null;

        using var document = JsonDocument.Parse(json.ToString() ?? string.Empty);
        if (!TryReadJsonPathElement(document.RootElement, segments, out var element))
            return null;

        return ConvertJsonElementToValue(element);
    }

    private static bool TryGetJsonReturningTypeSql(FunctionCallExpr fn, out string normalizedType)
    {
        normalizedType = string.Empty;
        if (fn.Args.Count < 3 || fn.Args[2] is not RawSqlExpr raw)
            return false;

        const string prefix = "RETURNING ";
        if (!raw.Sql.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var typeSql = raw.Sql[prefix.Length..].Trim();
        if (typeSql.Length == 0)
            return false;

        normalizedType = typeSql.ToUpperInvariant();
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

    private static bool TryParseSimpleJsonPathSegments(string path, out string[] segments)
    {
        segments = [];
        if (!path.StartsWith("$.", StringComparison.Ordinal))
            return false;

        var rawSegments = path[2..].Split('.');
        if (rawSegments.Length == 0)
            return false;

        segments = new string[rawSegments.Length];
        for (var i = 0; i < rawSegments.Length; i++)
        {
            var segment = rawSegments[i].Trim().Trim('"');
            if (segment.Length == 0)
                return false;

            segments[i] = segment;
        }

        return true;
    }

    private static bool TryReadJsonPathElement(
        JsonElement element,
        IReadOnlyList<string> segments,
        out JsonElement value)
    {
        value = element;
        for (var i = 0; i < segments.Count; i++)
        {
            if (value.ValueKind != JsonValueKind.Object
                || !value.TryGetProperty(segments[i], out var next))
            {
                value = default;
                return false;
            }

            value = next;
        }

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
