namespace DbSqlLikeMem;

internal static class AstQueryGeneralDateArithmeticFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
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

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalAddDateFunction, "ADDDATE");
        Register(handlers, TryEvalAddTimeFunction, "ADDTIME");
        Register(handlers, TryEvalLastDayFunction, "LAST_DAY", "EOMONTH");
        Register(handlers, TryEvalSubTimeFunction, "SUBTIME");
        return handlers;
    }

    private static void Register(
        Dictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers.Add(name, handler);
    }

    private static bool TryEvalAddDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADDDATE() espera 2 argumentos.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var addValue = evalArg(1);
        if (addValue is IntervalValue interval)
        {
            result = dateTime.Add(interval.Span);
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(addValue, out var dayOffset))
        {
            result = dateTime.AddDays(dayOffset);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalAddTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADDTIME() espera 2 argumentos.");

        var baseValue = evalArg(0);
        var addValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(addValue))
        {
            result = null;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime)
            && AstQueryExecutorBase.TryCoerceTimeSpan(addValue, out var addSpan))
        {
            result = dateTime.Add(addSpan);
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceTimeSpan(baseValue, out var baseSpan)
            && AstQueryExecutorBase.TryCoerceTimeSpan(addValue, out var addSpan2))
        {
            result = baseSpan.Add(addSpan2);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalLastDayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "EOMONTH", StringComparison.OrdinalIgnoreCase) && fn.Args.Count > 1)
        {
            var offsetValue = evalArg(1);
            if (!AstQueryExecutorBase.IsNullish(offsetValue))
            {
                var offset = Convert.ToInt32(offsetValue.ToDec(), CultureInfo.InvariantCulture);
                dateTime = dateTime.AddMonths(offset);
            }
        }

        var lastDay = DateTime.DaysInMonth(dateTime.Year, dateTime.Month);
        result = new DateTime(dateTime.Year, dateTime.Month, lastDay, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Kind);
        return true;
    }

    private static bool TryEvalSubTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("SUBTIME() espera base e intervalo.");

        var baseValue = evalArg(0);
        var intervalValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(intervalValue))
        {
            result = null;
            return true;
        }

        if (baseValue is TimeSpan baseTimeSpan && AstQueryExecutorBase.TryCoerceTimeSpan(intervalValue, out var span))
        {
            result = baseTimeSpan.Subtract(span);
            return true;
        }

        if (baseValue is string baseText
            && AstQueryExecutorBase.LooksLikeTimeOnly(baseText)
            && AstQueryExecutorBase.TryCoerceTimeSpan(baseText, out var baseSpanText)
            && AstQueryExecutorBase.TryCoerceTimeSpan(intervalValue, out var spanText))
        {
            result = baseSpanText.Subtract(spanText);
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime) && AstQueryExecutorBase.TryCoerceTimeSpan(intervalValue, out var spanDate))
        {
            result = dateTime.Subtract(spanDate);
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceTimeSpan(baseValue, out var baseSpan) && AstQueryExecutorBase.TryCoerceTimeSpan(intervalValue, out var span2))
        {
            result = baseSpan.Subtract(span2);
            return true;
        }

        result = null;
        return true;
    }
}
