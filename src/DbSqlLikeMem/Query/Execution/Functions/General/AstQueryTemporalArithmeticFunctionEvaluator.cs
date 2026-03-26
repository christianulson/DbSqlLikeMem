using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryTemporalArithmeticFunctionEvaluator
{
    private delegate bool AstQueryTryEvalTemporalArithmeticFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result);

    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalTemporalArithmeticFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(fn, context, row, group, ctes, evalArg, evalExpr, getTemporalUnit, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryTryEvalTemporalArithmeticFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalTemporalArithmeticFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalMySqlDateAddSubFunction, "DATE_ADD", "DATE_SUB");
        Register(handlers, TryEvalTimestampAddStyleFunction, "TIMESTAMPADD", "DATEADD");
        Register(handlers, TryEvalDateDiffBigFunction, "DATEDIFF_BIG");
        Register(handlers, TryEvalTimestampDiffFunction, "TIMESTAMPDIFF");
        Register(handlers, TryEvalDateDiffFunction, "DATEDIFF");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalTemporalArithmeticFunction> handlers,
        AstQueryTryEvalTemporalArithmeticFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalMySqlDateAddSubFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        var isAdd = string.Equals(fn.Name, "DATE_ADD", StringComparison.OrdinalIgnoreCase);
        var isSub = string.Equals(fn.Name, "DATE_SUB", StringComparison.OrdinalIgnoreCase);

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalExpression = fn.Args.Count > 1 ? fn.Args[1] : null;
        if (intervalExpression is not CallExpr intervalCall
            || !intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
            || intervalCall.Args.Count < 2)
        {
            result = dateTime;
            return true;
        }

        var amountObject = evalExpr(intervalCall.Args[0], row, group, ctes);
        var unit = getTemporalUnit(intervalCall.Args[1], row, group, ctes);
        if (unit == TemporalUnit.Unknown)
            unit = TemporalUnit.Day;

        var amount = Convert.ToInt32((amountObject ?? 0m).ToDec());
        if (isSub)
            amount = -amount;

        result = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
        return true;
    }

    private static bool TryEvalTimestampAddStyleFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        _ = evalExpr;
        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && !definition.AllowsCall)
        {
            result = null;
            return true;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(fn, out var dateAddDefinition)
            || dateAddDefinition is null
            || !dateAddDefinition.AllowsCall)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var baseValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var amountObject = evalArg(1);
        result = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, Convert.ToInt32((amountObject ?? 0m).ToDec()));
        return true;
    }

    private static bool TryEvalDateDiffBigFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalExpr;
        if (fn.Args.Count != 3)
            throw new InvalidOperationException("DATEDIFF_BIG() espera 3 argumentos.");

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(startValue) || AstQueryExecutorBase.IsNullish(endValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var start)
            || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifference(start, end, unit);
        return true;
    }

    private static bool TryEvalTimestampDiffFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalExpr;
        if (fn.Args.Count != 3)
            throw new InvalidOperationException("TIMESTAMPDIFF() espera 3 argumentos.");

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(startValue) || AstQueryExecutorBase.IsNullish(endValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var start)
            || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifferenceOrNull(start, end, unit);
        return true;
    }

    private static bool TryEvalDateDiffFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        _ = evalExpr;
        if (fn.Args.Count == 2)
        {
            var startValueMySql = evalArg(0);
            var endValueMySql = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(startValueMySql) || AstQueryExecutorBase.IsNullish(endValueMySql)
                || !AstQueryExecutorBase.TryCoerceDateTime(startValueMySql, out var startMySql)
                || !AstQueryExecutorBase.TryCoerceDateTime(endValueMySql, out var endMySql))
            {
                result = null;
                return true;
            }

            result = (int)(startMySql.Date - endMySql.Date).TotalDays;
            return true;
        }

        if (fn.Args.Count != 3)
            throw new InvalidOperationException("DATEDIFF() espera 3 argumentos.");

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(startValue) || AstQueryExecutorBase.IsNullish(endValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var start)
            || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifferenceOrNull(start, end, unit);
        return true;
    }

    private static int DiffMonths(DateTime start, DateTime end)
    {
        var months = (end.Year - start.Year) * 12 + (end.Month - start.Month);
        if (end.Day < start.Day)
            months -= 1;
        return months;
    }

    private static int DiffYears(DateTime start, DateTime end)
    {
        var years = end.Year - start.Year;
        if (end.Month < start.Month || (end.Month == start.Month && end.Day < start.Day))
            years -= 1;
        return years;
    }

    private static long? GetTemporalDifference(DateTime start, DateTime end, TemporalUnit unit) => unit switch
    {
        TemporalUnit.Day => (long)(end.Date - start.Date).TotalDays,
        TemporalUnit.Hour => (long)(end - start).TotalHours,
        TemporalUnit.Minute => (long)(end - start).TotalMinutes,
        TemporalUnit.Second => (long)(end - start).TotalSeconds,
        TemporalUnit.Month => DiffMonths(start, end),
        TemporalUnit.Year => DiffYears(start, end),
        _ => null
    };

    private static int? GetTemporalDifferenceOrNull(DateTime start, DateTime end, TemporalUnit unit)
    {
        var difference = GetTemporalDifference(start, end, unit);
        return difference is null ? null : (int)difference.Value;
    }
}
