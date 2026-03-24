using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryDb2DateFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        if (name is "DAYNAME" or "DAYOFMONTH" or "DAYOFWEEK" or "DAYOFWEEK_ISO" or "DAYOFYEAR" or "WEEK_ISO")
            return TryEvalDb2DateAliasFunction(name, evalArg, out result);

        if (name is "ADD_DAYS" or "ADD_HOURS" or "ADD_MINUTES" or "ADD_SECONDS" or "ADD_MONTHS" or "ADD_YEARS")
            return TryEvalDb2DateAddAliasFunction(fn, name, evalArg, resolveTemporalUnit, out result);

        return false;
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
