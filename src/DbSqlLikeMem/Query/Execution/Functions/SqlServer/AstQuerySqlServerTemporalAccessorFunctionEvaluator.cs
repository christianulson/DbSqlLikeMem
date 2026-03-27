using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySqlServerTemporalAccessorFunctionEvaluator
{
    private delegate bool SqlServerTemporalAccessorFunctionHandler(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result);

    private static readonly IReadOnlyDictionary<string, SqlServerTemporalAccessorFunctionHandler> _handlers = CreateHandlers();

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

    private static Dictionary<string, SqlServerTemporalAccessorFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, SqlServerTemporalAccessorFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalDateNameFunction, "DATENAME");
        Register(handlers, TryEvalDatePartFunction, "DATEPART");
        return handlers;
    }

    private static void Register(
        IDictionary<string, SqlServerTemporalAccessorFunctionHandler> handlers,
        SqlServerTemporalAccessorFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
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

        var unit = getTemporalUnit(fn.Args[0], row, group, ctes);
        var value = evalArg(1);
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
}
