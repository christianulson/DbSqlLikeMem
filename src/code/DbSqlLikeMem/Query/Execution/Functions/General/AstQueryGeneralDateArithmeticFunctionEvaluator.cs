using static DbSqlLikeMem.AstQueryExecutorBase;

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
        Register(handlers, TryEvalAddDateFunction, "ADDDATE", "DATE_ADD", "DATE_SUB", "SUBDATE");
        Register(handlers, TryEvalAddTimeFunction, "ADDTIME");
        Register(handlers, TryEvalSubTimeFunction, "SUBTIME");
        Register(handlers, TryEvalFirebirdDateAddFunction, "DATEADD");
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
        var isSubtraction = fn.Name.Equals("DATE_SUB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SUBDATE", StringComparison.OrdinalIgnoreCase);
        if (addValue is IntervalValue interval)
        {
            result = isSubtraction
                ? dateTime.Subtract(interval.Span)
                : dateTime.Add(interval.Span);
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(addValue, out var dayOffset))
        {
            result = isSubtraction
                ? dateTime.AddDays(-dayOffset)
                : dateTime.AddDays(dayOffset);
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

    private static bool TryEvalFirebirdDateAddFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DATEADD() espera 3 argumentos.");

        var unitText = fn.Args[0] is RawSqlExpr rawUnit
            ? rawUnit.Sql
            : Convert.ToString(evalArg(0), CultureInfo.InvariantCulture) ?? string.Empty;
        var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
        if (unit == TemporalUnit.Unknown)
        {
            result = null;
            return true;
        }

        var amountValue = evalArg(1);
        var baseValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(baseValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            if (!TryResolveTemporalBaseValue(context, fn.Args[2], out baseValue)
                || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out dateTime))
            {
                result = null;
                return true;
            }
        }

        if (AstQueryExecutorBase.IsNullish(amountValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryConvertNumericToDouble(amountValue, out var amount))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            TemporalUnit.Year => dateTime.AddYears((int)Math.Truncate(amount)),
            TemporalUnit.Month => dateTime.AddMonths((int)Math.Truncate(amount)),
            TemporalUnit.Week => dateTime.AddDays(amount * 7d),
            TemporalUnit.Day => dateTime.AddDays(amount),
            TemporalUnit.Hour => dateTime.AddHours(amount),
            TemporalUnit.Minute => dateTime.AddMinutes(amount),
            TemporalUnit.Second => dateTime.AddSeconds(amount),
            TemporalUnit.Millisecond => dateTime.AddMilliseconds(amount),
            TemporalUnit.Microsecond => dateTime.AddTicks((long)Math.Truncate(amount * 10d)),
            _ => null
        };
        return true;
    }

    private static bool TryResolveTemporalBaseValue(
        QueryExecutionContext context,
        SqlExpr expr,
        out object? value)
    {
        value = null;

        switch (expr)
        {
            case RawSqlExpr rawBase:
                return context.IsKnownTemporalFunctionName(rawBase.Sql)
                    && context.TryEvaluateZeroArgIdentifier(rawBase.Sql, out value);
            case IdentifierExpr identifierBase:
                return context.TryEvaluateZeroArgIdentifier(identifierBase.Name, out value);
            case FunctionCallExpr callBase when callBase.Args.Count == 0:
                return context.TryEvaluateZeroArgCall(callBase.Name, out value);
            case CallExpr callBase when callBase.Args.Count == 0:
                return context.TryEvaluateZeroArgCall(callBase.Name, out value);
            default:
                return false;
        }
    }
}
