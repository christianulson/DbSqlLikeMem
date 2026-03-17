namespace DbSqlLikeMem;

internal static class QueryOracleDb2UtilityFunctionHelper
{
    public static bool TryEvalUtilityFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalCardinalityFunction(fn, dialect, evalArg, out result)
            || TryEvalChrFunction(fn, dialect, evalArg, out result)
            || TryEvalComposeFunction(fn, dialect, evalArg, out result)
            || TryEvalDbTimeZoneFunction(fn, dialect, out result)
            || TryEvalDecomposeFunction(fn, dialect, evalArg, out result)
            || TryEvalEmptyLobFunction(fn, dialect, out result)
            || TryEvalInitCapFunction(fn, dialect, evalArg, out result)
            || TryEvalChartoRowidFunction(fn, dialect, evalArg, out result)
            || TryEvalClusterFunctions(fn, dialect, evalArg, out result);
    }

    private static bool TryEvalCardinalityFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CARDINALITY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is System.Text.Json.JsonElement element
            && element.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            result = element.GetArrayLength();
            return true;
        }

        if (value is string)
        {
            result = null;
            return true;
        }

        if (value is Array arr)
        {
            result = arr.Length;
            return true;
        }

        if (value is ICollection collection)
        {
            result = collection.Count;
            return true;
        }

        if (value is IEnumerable enumerable)
        {
            var count = 0;
            foreach (var _ in enumerable)
                count++;
            result = count;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalChrFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var code = Convert.ToInt32(value.ToDec(), System.Globalization.CultureInfo.InvariantCulture);
            if (code < 0 || code > 0x10FFFF)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(code);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalComposeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("COMPOSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormC);
        return true;
    }

    private static bool TryEvalDbTimeZoneFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("DBTIMEZONE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "+00:00";
        return true;
    }

    private static bool TryEvalDecomposeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DECOMPOSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormD);
        return true;
    }

    private static bool TryEvalEmptyLobFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!(fn.Name.Equals("EMPTY_BLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("EMPTY_CLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("EMPTY_DBCLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("EMPTY_NCLOB", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = fn.Name.Equals("EMPTY_BLOB", StringComparison.OrdinalIgnoreCase)
            ? Array.Empty<byte>()
            : string.Empty;
        return true;
    }

    private static bool TryEvalInitCapFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("INITCAP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (text.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var builder = new StringBuilder(text.Length);
        var makeUpper = true;
        foreach (var ch in text)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(makeUpper
                    ? char.ToUpperInvariant(ch)
                    : char.ToLowerInvariant(ch));
                makeUpper = false;
            }
            else
            {
                builder.Append(ch);
                makeUpper = true;
            }
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalChartoRowidFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHARTOROWID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString();
        return true;
    }

    private static bool TryEvalClusterFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CLUSTER_DETAILS" or "CLUSTER_DISTANCE" or "CLUSTER_ID" or "CLUSTER_PROBABILITY" or "CLUSTER_SET"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.SupportsOracleClusterFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        result = null;
        return true;
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
