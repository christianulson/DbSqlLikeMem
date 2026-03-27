namespace DbSqlLikeMem;

internal static class QueryConditionalNullFunctionHelper
{
    private delegate bool ConditionalNullFunctionHandler(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly Dictionary<string, ConditionalNullFunctionHandler> _handlers = CreateHandlers();

    public static bool TryEvalConditionalAndNullFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(context, fn, evalArg, out result))
        {
            return true;
        }

        return TryEvalNullSubstituteFunction(context, fn, evalArg, out result);
    }

    private static Dictionary<string, ConditionalNullFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, ConditionalNullFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, SqlConst.IF, TryEvalIfFunction);
        Register(handlers, "IIF", TryEvalIifFunction);
        Register(handlers, "NVL2", TryEvalNvl2Function);
        Register(handlers, "DECODE", TryEvalDecodeFunction);
        Register(handlers, "COALESCE", TryEvalCoalesceFunction);
        Register(handlers, "NULLIF", TryEvalNullIfFunction);
        return handlers;
    }

    private static void Register(
        Dictionary<string, ConditionalNullFunctionHandler> handlers,
        string name,
        ConditionalNullFunctionHandler handler)
    {
        handlers[name] = handler;
    }

    private static bool TryEvalIfFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.SupportsIfFunction)
        {
            result = null;
            return false;
        }

        var condition = evalArg(0).ToBool();
        result = condition ? evalArg(1) : evalArg(2);
        return true;
    }

    private static bool TryEvalIifFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        if (!context.Dialect.SupportsIifFunction)
        {
            result = null;
            return false;
        }

        var condition = evalArg(0).ToBool();
        result = condition ? evalArg(1) : evalArg(2);
        return true;
    }

    private static bool TryEvalNullSubstituteFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var supportsNullSubstitute = false;
        foreach (var functionName in context.Dialect.NullSubstituteFunctionNames)
        {
            if (functionName.Equals(fn.Name, StringComparison.OrdinalIgnoreCase))
            {
                supportsNullSubstitute = true;
                break;
            }
        }

        if (!supportsNullSubstitute)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(1) : value;
        return true;
    }

    private static bool TryEvalNvl2Function(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("NVL2() espera 3 argumentos.");

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(2) : evalArg(1);
        return true;
    }

    private static bool TryEvalDecodeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 2)
        {
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

            if (DecodeEquals(expr, search, context))
            {
                result = resultValue;
                return true;
            }
        }

        result = hasDefault ? evalArg(fn.Args.Count - 1) : null;
        return true;
    }

    private static bool TryEvalCoalesceFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = left;
            return true;
        }

        result =context.Compare(left, right) == 0 ? null : left;
        return true;
    }

    private static bool DecodeEquals(
        object? left,
        object? right,
        QueryExecutionContext context)
    {
        if (IsNullish(left) && IsNullish(right))
            return true;

        if (IsNullish(left) || IsNullish(right))
            return false;

        return left!.EqualsSql(right!, context);
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
