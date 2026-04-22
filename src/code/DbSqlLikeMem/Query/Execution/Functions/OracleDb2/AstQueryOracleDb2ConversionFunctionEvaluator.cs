namespace DbSqlLikeMem;

using System.Globalization;

internal delegate bool AstQueryTryEvalOracleDb2ConversionFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        if (_requiresSupportCheck.Contains(fn.Name))
            context.EnsureOracleDb2FunctionSupported(fn.Name);

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

        return handler(context, fn, evalArg, out result);
    }

    private static Dictionary<string, AstQueryTryEvalOracleDb2ConversionFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalOracleDb2ConversionFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalConvertFunction, "CONVERT");
        Register(handlers, TryEvalToBinaryDoubleFunction, "TO_BINARY_DOUBLE");
        Register(handlers, TryEvalToBinaryFloatFunction, "TO_BINARY_FLOAT");
        Register(handlers, TryEvalToNumberFunction, "TO_NUMBER", "DEC");
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

    private static bool TryEvalConvertFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var value = evalArg(0);
        result = value is string textValue ? textValue : value!.ToString();
        return true;
    }

    private static bool TryEvalToBinaryDoubleFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = Convert.ToDouble(evalArg(0), CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalToBinaryFloatFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = Convert.ToSingle(evalArg(0), CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalToNumberFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return AstQueryToNumberFunctionEvaluator.TryEvalToNumberFunction(fn, evalArg, out result);
    }

    internal static bool TryEvalToCharFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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

            if (IsDb2Provider(context)
                && TryGetNumericScale(fn.Args[0], out var scale)
                && scale > 0
                && AstQueryExecutorBase.TryConvertNumericToDecimal(value, out var decimalValue))
            {
                result = decimalValue.ToString($"F{scale}", CultureInfo.InvariantCulture);
                return true;
            }

            result = IsDb2Provider(context)
                ? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
                : AstQueryFormatFunctionHelper.FormatOracleNumber(value!);
            return true;
        }

        result = value!.ToString();
        return true;
    }

    private static bool IsDb2Provider(QueryExecutionContext context)
        => string.Equals(context.Connection.ProviderExecutionDialect.Name, "db2", StringComparison.OrdinalIgnoreCase);

    private static bool TryGetNumericScale(SqlExpr expression, out int scale)
    {
        switch (expression)
        {
            case LiteralExpr { Value: decimal decimalValue }:
                scale = GetDecimalScale(decimalValue);
                return true;

            case FunctionCallExpr functionCall:
                return TryGetNumericScaleFromCall(functionCall.Name, functionCall.Args, out scale);

            case CallExpr call:
                return TryGetNumericScaleFromCall(call.Name, call.Args, out scale);

            case UnaryExpr unary:
                return TryGetNumericScale(unary.Expr, out scale);

            case BinaryExpr binary:
                var hasLeftScale = TryGetNumericScale(binary.Left, out var leftScale);
                var hasRightScale = TryGetNumericScale(binary.Right, out var rightScale);
                if (hasLeftScale || hasRightScale)
                {
                    scale = Math.Max(leftScale, rightScale);
                    return true;
                }

                break;
        }

        scale = 0;
        return false;
    }

    private static bool TryGetNumericScaleFromCall(string name, IReadOnlyList<SqlExpr> args, out int scale)
    {
        if (string.Equals(name, "ROUND", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count > 1 && TryReadIntegerLiteral(args[1], out scale))
                return true;
        }

        if (string.Equals(name, "CAST", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            if (TryGetDecimalScaleFromTypeExpression(args, out scale))
                return true;
        }

        if (string.Equals(name, "COALESCE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "NVL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "NVL2", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "IFNULL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "ISNULL", StringComparison.OrdinalIgnoreCase))
        {
            var hasScale = false;
            var maxScale = 0;
            for (var i = 0; i < args.Count; i++)
            {
                if (!TryGetNumericScale(args[i], out var argScale))
                    continue;

                hasScale = true;
                if (argScale > maxScale)
                    maxScale = argScale;
            }

            if (hasScale)
            {
                scale = maxScale;
                return true;
            }
        }

        scale = 0;
        return false;
    }

    private static bool TryGetDecimalScaleFromTypeExpression(IReadOnlyList<SqlExpr> args, out int scale)
    {
        scale = 0;
        if (args.Count < 2)
            return false;

        if (args[1] is not RawSqlExpr rawType)
            return false;

        var typeSql = rawType.Sql;
        if (!typeSql.Contains("(", StringComparison.Ordinal))
            return false;

        var commaIndex = typeSql.IndexOf(',');
        var closeIndex = typeSql.IndexOf(')', commaIndex >= 0 ? commaIndex : 0);
        if (commaIndex < 0 || closeIndex < 0 || commaIndex + 1 >= closeIndex)
            return false;

        var scaleText = typeSql[(commaIndex + 1)..closeIndex].Trim();
        return int.TryParse(scaleText, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale);
    }

    private static bool TryReadIntegerLiteral(SqlExpr expression, out int value)
    {
        value = 0;

        if (expression is LiteralExpr { Value: int intValue })
        {
            value = intValue;
            return true;
        }

        if (expression is LiteralExpr { Value: long longValue } && longValue is >= int.MinValue and <= int.MaxValue)
        {
            value = (int)longValue;
            return true;
        }

        if (expression is LiteralExpr { Value: decimal decimalValue })
        {
            value = (int)decimalValue;
            return true;
        }

        if (expression is LiteralExpr { Value: double doubleValue })
        {
            value = (int)doubleValue;
            return true;
        }

        if (expression is LiteralExpr { Value: string text }
            && int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static int GetDecimalScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }

    private static bool TryEvalToDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return TryEvalOracleDateTimeFunction(fn, evalArg, allowOffset: false, out result);
    }

    private static bool TryEvalToTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return TryEvalOracleDateTimeFunction(fn, evalArg, allowOffset: false, out result);
    }

    private static bool TryEvalToTimestampTzFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = evalArg(0)?.ToString();
        return true;
    }
}

