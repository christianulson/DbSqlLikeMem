using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryDb2DateFunctionEvaluator
{
    private delegate bool AstQueryTryEvalDb2DateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result);

    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalDb2DateFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(fn, context, evalArg, resolveTemporalUnit, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryTryEvalDb2DateFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalDb2DateFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalDb2DateAliasFunctionHandler, "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFWEEK_ISO", "DAYOFYEAR", "WEEK_ISO");
        Register(handlers, TryEvalDb2DateAddAliasFunctionHandler, "ADD_DAYS", "ADD_HOURS", "ADD_MINUTES", "ADD_SECONDS", "ADD_MONTHS", "ADD_YEARS");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalDb2DateFunction> handlers,
        AstQueryTryEvalDb2DateFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalDb2DateAliasFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = context.Dialect;
        _ = resolveTemporalUnit;
        return TryEvalDb2DateAliasFunction(fn.Name.ToUpperInvariant(), evalArg, out result);
    }

    private static bool TryEvalDb2DateAddAliasFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = context.Dialect;
        return TryEvalDb2DateAddAliasFunction(fn, fn.Name.ToUpperInvariant(), evalArg, resolveTemporalUnit, out result);
    }

    private static bool TryEvalDb2DateAliasFunction(
        string name,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = name switch
        {
            "DAYNAME" => dateTime.ToString("dddd", CultureInfo.InvariantCulture),
            "DAYOFMONTH" => dateTime.Day,
            "DAYOFWEEK" => (int)dateTime.DayOfWeek + 1,
            "DAYOFWEEK_ISO" => ((int)dateTime.DayOfWeek + 6) % 7 + 1,
            "DAYOFYEAR" => dateTime.DayOfYear,
            "WEEK_ISO" => AstQueryExecutorBase.GetIsoWeekOfYear(dateTime),
            _ => null
        };
        return true;
    }

    private static bool TryEvalDb2DateAddAliasFunction(
        FunctionCallExpr fn,
        string name,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera data e quantidade.");

        var baseValue = evalArg(0);
        var amountValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(amountValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        try
        {
            var amount = Convert.ToInt32(amountValue.ToDec());
            var unit = resolveTemporalUnit(name["ADD_".Length..]);
            result = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }
}
