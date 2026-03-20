namespace DbSqlLikeMem;

internal static class QueryConditionalNullFunctionHelper
{
    public static bool TryEvalConditionalAndNullFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalConditionalFunction(fn, dialect, evalArg, out result)
            || TryEvalNullSubstituteFunction(fn, dialect, evalArg, out result)
            || TryEvalNvl2Function(fn, evalArg, out result)
            || TryEvalDecodeFunction(fn, dialect, evalArg, out result)
            || TryEvalCoalesceFunction(fn, evalArg, out result)
            || TryEvalNullIfFunction(fn, dialect, evalArg, out result);
    }

    private static bool TryEvalConditionalFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isIf = fn.Name.Equals(SqlConst.IF, StringComparison.OrdinalIgnoreCase);
        var isIif = fn.Name.Equals("IIF", StringComparison.OrdinalIgnoreCase);
        if (!((isIf && dialect.SupportsIfFunction) || (isIif && dialect.SupportsIifFunction)))
        {
            result = null;
            return false;
        }

        var condition = evalArg(0).ToBool();
        result = condition ? evalArg(1) : evalArg(2);
        return true;
    }

    private static bool TryEvalNullSubstituteFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase)
            || !dialect.NullSubstituteFunctionNames.Any(name => name.Equals(fn.Name, StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(1) : value;
        return true;
    }

    private static bool TryEvalNvl2Function(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NVL2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("NVL2() espera 3 argumentos.");

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(2) : evalArg(1);
        return true;
    }

    private static bool TryEvalDecodeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DECODE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count != 2)
                throw new InvalidOperationException("DECODE() no PostgreSQL espera payload e formato.");

            var payload = evalArg(0)?.ToString();
            var format = evalArg(1)?.ToString();
            if (string.IsNullOrWhiteSpace(payload) || string.IsNullOrWhiteSpace(format))
            {
                result = null;
                return true;
            }

            try
            {
                result = format!.Trim().ToLowerInvariant() switch
                {
                    "hex" when TryNormalizeHexPayload(payload!.Trim(), out var hex) && hex.Length % 2 == 0
                        => ParseHexBinaryPayload(hex),
                    "base64" => Convert.FromBase64String(payload),
                    _ => null
                };
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DECODE() espera ao menos 3 argumentos.");

        var expr = evalArg(0);
        var pairCount = (fn.Args.Count - 1) / 2;
        var hasDefault = (fn.Args.Count - 1) % 2 == 1;

        for (int i = 0; i < pairCount; i++)
        {
            var search = evalArg(1 + i * 2);
            var resultValue = evalArg(2 + i * 2);

            if (DecodeEquals(expr, search, dialect))
            {
                result = resultValue;
                return true;
            }
        }

        result = hasDefault ? evalArg(fn.Args.Count - 1) : null;
        return true;
    }

    private static bool TryEvalCoalesceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        for (int i = 0; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (!IsNullish(value))
            {
                result = value;
                return true;
            }
        }

        result = null;
        return true;
    }

    private static bool TryEvalNullIfFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = left;
            return true;
        }

        result = left!.Compare(right!, dialect) == 0 ? null : left;
        return true;
    }

    private static bool DecodeEquals(object? left, object? right, ISqlDialect dialect)
    {
        if (IsNullish(left) && IsNullish(right))
            return true;

        if (IsNullish(left) || IsNullish(right))
            return false;

        return left!.EqualsSql(right!, dialect);
    }

    private static byte[] ParseHexBinaryPayload(string hex)
    {
        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            buffer[i / 2] = byte.Parse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return buffer;
    }

    private static bool TryNormalizeHexPayload(string trimmed, out string hex)
    {
        hex = string.Empty;

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = trimmed[2..];
            return true;
        }

        if (trimmed.Length >= 3
            && (trimmed[0] == 'x' || trimmed[0] == 'X')
            && trimmed[1] == '\''
            && trimmed[^1] == '\'')
        {
            hex = trimmed[2..^1];
            return true;
        }

        hex = trimmed;
        return true;
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
