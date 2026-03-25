namespace DbSqlLikeMem;

using System.Globalization;

internal delegate bool AstQueryTryEvalOracleDb2ConversionFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryOracleDb2ConversionFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalOracleDb2ConversionFunction> _handlers =
        CreateHandlers();

    private static readonly HashSet<string> _requiresSupportCheck = new(StringComparer.OrdinalIgnoreCase)
    {
        "TO_BINARY_DOUBLE",
        "TO_BINARY_FLOAT",
        "TO_BLOB",
        "TO_CLOB",
        "TO_DSINTERVAL",
        "TO_LOB",
        "TO_MULTI_BYTE",
        "TO_NCHAR",
        "TO_NCLOB",
        "TO_SINGLE_BYTE",
        "TO_TIMESTAMP_TZ",
        "TO_YMINTERVAL"
    };

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!IsOracleDb2Dialect(context))
        {
            result = null;
            return false;
        }

        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        if (_requiresSupportCheck.Contains(fn.Name))
            QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(context, fn.Name);

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

        return handler(fn, context, evalArg, out result);
    }

    private static Dictionary<string, AstQueryTryEvalOracleDb2ConversionFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalOracleDb2ConversionFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalConvertFunction, "CONVERT");
        Register(handlers, TryEvalToBinaryDoubleFunction, "TO_BINARY_DOUBLE");
        Register(handlers, TryEvalToBinaryFloatFunction, "TO_BINARY_FLOAT");
        Register(handlers, TryEvalToNumberFunction, "TO_NUMBER");
        Register(handlers, TryEvalToCharFunction, "TO_CHAR");
        Register(handlers, TryEvalToDateFunction, "TO_DATE");
        Register(handlers, TryEvalToTimestampFunction, "TO_TIMESTAMP");
        Register(handlers, TryEvalToTimestampTzFunction, "TO_TIMESTAMP_TZ");
        Register(handlers, TryEvalToDsIntervalFunction, "TO_DSINTERVAL");
        Register(handlers, TryEvalToYmIntervalFunction, "TO_YMINTERVAL");
        Register(handlers, TryEvalToTextFunction, "TO_BLOB", "TO_CLOB", "TO_LOB", "TO_MULTI_BYTE", "TO_NCHAR", "TO_NCLOB", "TO_SINGLE_BYTE");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalOracleDb2ConversionFunction> handlers,
        AstQueryTryEvalOracleDb2ConversionFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool IsOracleDb2Dialect(QueryExecutionContext context)
        => context.Dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            || context.Dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase);

    private static bool TryEvalConvertFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var value = evalArg(0);
        result = value is string textValue ? textValue : value!.ToString();
        return true;
    }

    private static bool TryEvalToBinaryDoubleFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = Convert.ToDouble(evalArg(0), CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalToBinaryFloatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = Convert.ToSingle(evalArg(0), CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalToNumberFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var value = evalArg(0);
        if (value is string numberText)
        {
            var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (AstQueryFormatFunctionHelper.TryParseOracleNumber(numberText, mask, out var parsedNumber))
            {
                result = parsedNumber;
                return true;
            }
        }

        result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalToCharFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var value = evalArg(0);

        if (value is DateTime dateValue)
        {
            if (fn.Args.Count > 1 && evalArg(1) is string fmt)
            {
                var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                result = dateValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
            }
            else
            {
                result = dateValue.ToString(CultureInfo.InvariantCulture);
            }

            return true;
        }

        if (value is DateTimeOffset dtoValue)
        {
            if (fn.Args.Count > 1 && evalArg(1) is string fmt)
            {
                var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                result = dtoValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
            }
            else
            {
                result = dtoValue.ToString(CultureInfo.InvariantCulture);
            }

            return true;
        }

        if (AstQueryFormatFunctionHelper.IsNumericValue(value))
        {
            var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (!string.IsNullOrWhiteSpace(mask))
            {
                result = AstQueryFormatFunctionHelper.FormatOracleNumber(value!, mask!);
                return true;
            }
        }

        result = value!.ToString();
        return true;
    }

    private static bool TryEvalToDateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return TryEvalOracleDateTimeFunction(fn, evalArg, allowOffset: false, out result);
    }

    private static bool TryEvalToTimestampFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return TryEvalOracleDateTimeFunction(fn, evalArg, allowOffset: false, out result);
    }

    private static bool TryEvalToTimestampTzFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return TryEvalOracleDateTimeFunction(fn, evalArg, allowOffset: true, out result);
    }

    private static bool TryEvalOracleDateTimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool allowOffset,
        out object? result)
    {
        var value = evalArg(0);
        if (value is DateTime dt)
        {
            result = dt;
            return true;
        }

        var textValue = value?.ToString() ?? string.Empty;
        var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
        if (allowOffset)
        {
            if (AstQueryFormatFunctionHelper.TryParseOracleDateTimeOffset(textValue, maskValue, out var parsedOffset))
            {
                result = parsedOffset;
                return true;
            }

            result = null;
            return true;
        }

        if (AstQueryFormatFunctionHelper.TryParseOracleDateTime(textValue, maskValue, out var parsed))
        {
            result = parsed;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalToDsIntervalFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        if (AstQueryExecutorBase.TryCoerceTimeSpan(evalArg(0), out var parsedSpan))
        {
            result = parsedSpan;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalToYmIntervalFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        if (AstQueryExecutorBase.TryCoerceTimeSpan(evalArg(0), out var parsedSpan))
        {
            result = parsedSpan;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalToTextFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = evalArg(0)?.ToString();
        return true;
    }
}

