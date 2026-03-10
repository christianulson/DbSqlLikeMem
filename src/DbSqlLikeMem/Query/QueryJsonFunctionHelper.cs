using System.Text;
using System.Text.Json;

namespace DbSqlLikeMem;

internal static class QueryJsonFunctionHelper
{
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

    internal static object? ApplyJsonValueReturningClause(FunctionCallExpr fn, object? value)
    {
        if (IsNullish(value) || !TryGetJsonReturningTypeSql(fn, out var normalizedType))
            return value;

        return ConvertJsonValueForReturningType(value, normalizedType);
    }

    internal static object? TryReadJsonPathValue(object json, string path)
    {
        var lookup = LookupJsonPath(json, path);
        if (!lookup.Success)
            return null;

        return ConvertJsonElementToValue(lookup.Value);
    }

    internal static bool TryReadJsonPathElement(object json, string path, out JsonElement element)
    {
        element = default;
        var lookup = LookupJsonPath(json, path);
        if (!lookup.Success)
            return false;

        element = lookup.Value;
        return true;
    }

    internal static bool TryReadJsonPathElement(JsonElement element, string path, out JsonElement value)
    {
        value = default;
        var lookup = LookupJsonPath(element, path);
        if (!lookup.Success)
            return false;

        value = lookup.Value;
        return true;
    }

    internal static JsonPathLookupResult LookupJsonPath(object json, string path)
    {
        if (!TryParseJsonPathSpec(path, out var spec, out _))
            return new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.InvalidPath, path, default);

        using var document = JsonDocument.Parse(json.ToString() ?? string.Empty);
        var lookup = LookupJsonPath(document.RootElement, spec);
        return lookup.Success
            ? lookup with { Value = lookup.Value.Clone() }
            : lookup;
    }

    internal static JsonPathLookupResult LookupJsonPath(JsonElement element, string path)
    {
        if (!TryParseJsonPathSpec(path, out var spec, out _))
            return new JsonPathLookupResult(false, JsonPathMode.Lax, JsonPathLookupFailure.InvalidPath, path, default);

        return LookupJsonPath(element, spec);
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

    private static JsonPathLookupResult LookupJsonPath(JsonElement element, JsonPathSpec spec)
    {
        if (!TryReadJsonPathElement(element, spec.Steps, out var value))
            return new JsonPathLookupResult(false, spec.Mode, JsonPathLookupFailure.NotFound, spec.Path, default);

        return new JsonPathLookupResult(true, spec.Mode, JsonPathLookupFailure.None, spec.Path, value);
    }

    private static bool TryParseJsonPathSpec(string path, out JsonPathSpec spec, out string? error)
    {
        spec = default;
        error = null;

        if (string.IsNullOrWhiteSpace(path))
        {
            error = "JSON path is empty.";
            return false;
        }

        var trimmed = path.Trim();
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

        spec = new JsonPathSpec(mode, trimmed, steps);
        return true;
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

    private static bool TryParseJsonPropertyStep(
        string path,
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

        propertyName = path[start..index];
        if (propertyName.Length == 0)
        {
            error = "JSON path property name is missing.";
            return false;
        }

        return true;
    }

    private static bool TryParseQuotedJsonProperty(
        string path,
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
        string path,
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

        var parsedIndex = int.Parse(path[start..index], CultureInfo.InvariantCulture);
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
