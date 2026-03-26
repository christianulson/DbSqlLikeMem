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
            TryEvalDateNameFunction,
            "DATENAME");
        Register(
            handlers,
            TryEvalDatePartFunction,
            "DATEPART", "DAY", "MONTH", SqlConst.YEAR, "HOUR", "MINUTE", "SECOND");
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
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        result = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
        return true;
    }

    private static bool TryEvalDateNameFunction(
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
            throw new InvalidOperationException("DATENAME() espera 2 argumentos.");

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            TemporalUnit.Year => dateTime.Year.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Month => dateTime.ToString("MMMM", CultureInfo.InvariantCulture),
            TemporalUnit.Day => dateTime.Day.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Hour => dateTime.Hour.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Minute => dateTime.Minute.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Second => dateTime.Second.ToString(CultureInfo.InvariantCulture),
            _ => null
        };
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
        var name = fn.Name.ToUpperInvariant();
        if (name == "DATEPART" && fn.Args.Count < 2)
            throw new InvalidOperationException("DATEPART() espera 2 argumentos.");

        var unit = name == "DATEPART" ? getTemporalUnit(fn.Args[0], row, group, ctes) : resolveTemporalUnit(name);
        var value = evalArg(name == "DATEPART" ? 1 : 0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            TemporalUnit.Year => dateTime.Year,
            TemporalUnit.Month => dateTime.Month,
            TemporalUnit.Day => dateTime.Day,
            TemporalUnit.Hour => dateTime.Hour,
            TemporalUnit.Minute => dateTime.Minute,
            TemporalUnit.Second => dateTime.Second,
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
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = unit switch
            {
                TemporalUnit.Day => dateTime.Day,
                TemporalUnit.Month => dateTime.Month,
                TemporalUnit.Year => dateTime.Year,
                TemporalUnit.Hour => dateTime.Hour,
                TemporalUnit.Minute => dateTime.Minute,
                TemporalUnit.Second => dateTime.Second,
                _ => null
            };
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(value, out var numeric))
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
