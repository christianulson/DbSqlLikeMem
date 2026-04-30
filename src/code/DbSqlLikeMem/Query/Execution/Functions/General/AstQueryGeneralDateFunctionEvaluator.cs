namespace DbSqlLikeMem;

internal static class AstQueryGeneralDateFunctionEvaluator
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

        Register(handlers, TryEvalDateConstructionFunction, "DATE", "TIMESTAMP", "DATETIME", "TIME");
        Register(handlers, TryEvalDatePartFunction, "DAY", "MONTH", "YEAR", "HOUR", "MINUTE", "SECOND");

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

    private static bool TryEvalDateConstructionFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isDate = string.Equals(fn.Name, "DATE", StringComparison.OrdinalIgnoreCase);
        var isTimestamp = string.Equals(fn.Name, "TIMESTAMP", StringComparison.OrdinalIgnoreCase);
        var isDateTime = string.Equals(fn.Name, "DATETIME", StringComparison.OrdinalIgnoreCase);
        var isTime = string.Equals(fn.Name, "TIME", StringComparison.OrdinalIgnoreCase);
        if (!(isDate || isTimestamp || isDateTime || isTime)
            || fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            var modifier = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(modifier)
                || !AstQueryExecutorBase.TryParseDateModifier(modifier!, out var unit, out var amount))
            {
                continue;
            }

            dateTime = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
        }

        if (isTime)
        {
            result = dateTime.TimeOfDay;
            return true;
        }

        result = isDate
            ? dateTime.Date
            : dateTime;
        return true;
    }

    private static bool TryEvalDatePartFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = fn.Name.ToUpperInvariant() switch
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
}
