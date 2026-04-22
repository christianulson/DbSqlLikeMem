using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryTemporalArithmeticFunctionEvaluator
{
    private delegate bool AstQueryTryEvalTemporalArithmeticFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(context, fn, row, group, ctes, evalArg, evalExpr, getTemporalUnit, out result))
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
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

        result = ApplyDateDelta(dateTime, unit, amount);
        return true;
    }

    private static bool TryEvalTimestampAddStyleFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var amountObject = evalArg(1);
        result = ApplyDateDelta(dateTime, unit, Convert.ToInt32((amountObject ?? 0m).ToDec()));
        return true;
    }

    private static bool TryEvalDateDiffBigFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifference(start, end, unit);
        return true;
    }

    private static bool TryEvalTimestampDiffFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifferenceOrNull(start, end, unit);
        return true;
    }

    private static bool TryEvalDateDiffFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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
            if (IsNullish(startValueMySql) || IsNullish(endValueMySql)
                || !TryCoerceDateTime(startValueMySql, out var startMySql)
                || !TryCoerceDateTime(endValueMySql, out var endMySql))
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
        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = GetTemporalDifferenceOrNull(start, end, unit);
        return true;
    }

    private static int DiffMonths(DateTime start, DateTime end)
        => (end.Year - start.Year) * 12 + end.Month - start.Month;

    private static int GetTemporalDifference(DateTime start, DateTime end, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Year => end.Year - start.Year,
            TemporalUnit.Month => DiffMonths(start, end),
            TemporalUnit.Day => (int)(end.Date - start.Date).TotalDays,
            TemporalUnit.Hour => (int)(end - start).TotalHours,
            TemporalUnit.Minute => (int)(end - start).TotalMinutes,
            TemporalUnit.Second => (int)(end - start).TotalSeconds,
            _ => (int)(end - start).TotalSeconds
        };

    private static int? GetTemporalDifferenceOrNull(DateTime start, DateTime end, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Year => end.Year - start.Year,
            TemporalUnit.Month => DiffMonths(start, end),
            TemporalUnit.Day => (int)(end.Date - start.Date).TotalDays,
            TemporalUnit.Hour => (int)(end - start).TotalHours,
            TemporalUnit.Minute => (int)(end - start).TotalMinutes,
            TemporalUnit.Second => (int)(end - start).TotalSeconds,
            _ => null
        };
}
