namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalPostgresScalarUtilityFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresScalarUtilityFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresScalarUtilityFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluatePostgresScalarUtilityFunction(
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

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresScalarUtilityFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresScalarUtilityFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalNumNullsFunction, "NUM_NULLS");
        Register(handlers, TryEvalNumNonNullsFunction, "NUM_NONNULLS");
        Register(handlers, TryEvalLcmFunction, "LCM");
        Register(handlers, TryEvalMinScaleFunction, "MIN_SCALE");
        Register(handlers, TryEvalParseIdentFunction, "PARSE_IDENT");
        return handlers;
    }

    internal static void RegisterHandlers(
        this QueryExecutionContext context)
    {
        var dialect = context.Dialect;
        dialect.AddScalarFunctions("INT", TryEvalNumNullsFunction, "NUM_NULLS");
        dialect.AddScalarFunctions("INT", TryEvalNumNonNullsFunction, "NUM_NONNULLS");
        dialect.AddScalarFunctions("BIGINT", TryEvalLcmFunction, "LCM");
        dialect.AddScalarFunctions("INT", TryEvalMinScaleFunction, "MIN_SCALE");
        dialect.AddScalarFunctions("STRING_ARRAY", TryEvalParseIdentFunction, "PARSE_IDENT");
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresScalarUtilityFunction> handlers,
        AstQueryTryEvalPostgresScalarUtilityFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalNumNullsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalNumNonNullsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => !AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalLcmFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var leftValue = evalArg(0);
        var rightValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(leftValue) || AstQueryExecutorBase.IsNullish(rightValue))
        {
            result = null;
            return true;
        }

        var left = Math.Abs(Convert.ToInt64(leftValue.ToDec(), CultureInfo.InvariantCulture));
        var right = Math.Abs(Convert.ToInt64(rightValue.ToDec(), CultureInfo.InvariantCulture));
        if (left == 0 || right == 0)
        {
            result = 0L;
            return true;
        }

        result = checked((left / ComputeGreatestCommonDivisor(left, right)) * right);
        return true;
    }

    private static bool TryEvalMinScaleFunction(
        this QueryExecutionContext context,
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

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = GetMinimumNumericScale(value!);
        return true;
    }

    private static bool TryEvalParseIdentFunction(
        this QueryExecutionContext context,
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

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (!TryParsePostgresIdentifierParts(text, out var parts))
        {
            result = null;
            return true;
        }

        result = parts.ToArray();
        return true;
    }

    private static long ComputeGreatestCommonDivisor(long left, long right)
    {
        while (right != 0)
        {
            var remainder = left % right;
            left = right;
            right = remainder;
        }

        return Math.Abs(left);
    }

    private static int GetMinimumNumericScale(object value)
    {
        var text = value switch
        {
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            double dbl => dbl.ToString("G17", CultureInfo.InvariantCulture),
            float flt => flt.ToString("G9", CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };

        var exponentIndex = text.IndexOfAny(['e', 'E']);
        if (exponentIndex >= 0)
        {
            if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDecimal))
                text = parsedDecimal.ToString(CultureInfo.InvariantCulture);
            else
                text = text[..exponentIndex];
        }

        var decimalIndex = text.IndexOf('.');
        if (decimalIndex < 0)
            return 0;

        var fractional = text[(decimalIndex + 1)..].TrimEnd('0');
        return fractional.Length;
    }

    private static bool TryParsePostgresIdentifierParts(string text, out List<string> parts)
    {
        parts = [];
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var current = new StringBuilder();
        var insideQuotes = false;
        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (insideQuotes)
            {
                if (ch == '"')
                {
                    if (i + 1 < text.Length && text[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                        continue;
                    }

                    insideQuotes = false;
                    continue;
                }

                current.Append(ch);
                continue;
            }

            if (ch == '"')
            {
                insideQuotes = true;
                continue;
            }

            if (ch == '.')
            {
                parts.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        if (insideQuotes)
            return false;

        parts.Add(current.ToString().Trim());
        return parts.Count > 0 && parts.All(static part => part.Length > 0);
    }
}
