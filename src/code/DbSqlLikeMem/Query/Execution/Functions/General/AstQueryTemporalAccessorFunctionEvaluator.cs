using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryTemporalAccessorFunctionEvaluator
{
    private delegate bool TemporalAccessorFunctionHandler(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result);

    private static readonly IReadOnlyDictionary<string, TemporalAccessorFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        result = null;

        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, row, group, ctes, evalArg, getTemporalUnit, resolveTemporalUnit, out result);

        return false;
    }

    private static Dictionary<string, TemporalAccessorFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, TemporalAccessorFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(
            handlers,
            TryEvalDaysFunction,
            "DAYS");
        Register(
            handlers,
            TryEvalDatePartFunction,
            "DAY", "MONTH", SqlConst.YEAR, "HOUR", "MINUTE", "SECOND");
        Register(
            handlers,
            TryEvalExtractFunction,
            "EXTRACT");
        return handlers;
    }

    private static void Register(
        IDictionary<string, TemporalAccessorFunctionHandler> handlers,
        TemporalAccessorFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalDaysFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = row;
        _ = group;
        _ = ctes;
        _ = getTemporalUnit;
        _ = resolveTemporalUnit;
        if (fn.Args.Count != 1)
            throw new InvalidOperationException("DAYS() espera 1 argumento.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        result = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
        return true;
    }

    private static bool TryEvalDatePartFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = row;
        _ = group;
        _ = ctes;
        _ = getTemporalUnit;
        _ = resolveTemporalUnit;

        var name = fn.Name.ToUpperInvariant();
        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = name switch
        {
            "DAY" => dateTime.Day,
            "MONTH" => dateTime.Month,
            "YEAR" => dateTime.Year,
            "HOUR" => dateTime.Hour,
            "MINUTE" => dateTime.Minute,
            "SECOND" => dateTime.Second,
            _ => null
        };
        return true;
    }

    private static bool TryEvalExtractFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = resolveTemporalUnit;
        if (fn.Args.Count < 2)
        {
            result = null;
            return false;
        }

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var value = evalArg(1);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (TryCoerceDateTime(value, out var dateTime))
        {
            result = unit switch
            {
                TemporalUnit.Day => dateTime.Day,
                TemporalUnit.Month => dateTime.Month,
                TemporalUnit.Year => dateTime.Year,
                TemporalUnit.Week => GetIsoWeekOfYear(dateTime),
                TemporalUnit.Weekday => (int)dateTime.DayOfWeek,
                TemporalUnit.Yearday => dateTime.DayOfYear - 1,
                TemporalUnit.Hour => dateTime.Hour,
                TemporalUnit.Minute => dateTime.Minute,
                TemporalUnit.Second => dateTime.Second,
                TemporalUnit.Millisecond => dateTime.Millisecond,
                TemporalUnit.Microsecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L),
                _ => null
            };
            return true;
        }

        if (TryCoerceTimeSpan(value, out var timeSpan))
        {
            result = unit switch
            {
                TemporalUnit.Day => (int)Math.Truncate(timeSpan.TotalDays),
                TemporalUnit.Hour => timeSpan.Hours,
                TemporalUnit.Minute => timeSpan.Minutes,
                TemporalUnit.Second => timeSpan.Seconds,
                TemporalUnit.Millisecond => timeSpan.Milliseconds,
                TemporalUnit.Microsecond => (int)((timeSpan.Ticks % TimeSpan.TicksPerSecond) / 10L),
                _ => null
            };
            return true;
        }

        if (TryConvertNumericToDouble(value, out var numeric))
        {
            result = unit switch
            {
                TemporalUnit.Day => (int)Math.Truncate(numeric),
                _ => null
            };
            return true;
        }

        result = null;
        return true;
    }
}
