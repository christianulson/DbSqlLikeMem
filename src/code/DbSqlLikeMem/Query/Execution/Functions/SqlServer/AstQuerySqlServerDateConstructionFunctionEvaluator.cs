namespace DbSqlLikeMem;

internal static class AstQuerySqlServerDateConstructionFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluateSqlServerDateConstructionFunction(
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

        Register(handlers, TryEvalDateFromPartsFunction, "DATEFROMPARTS");
        Register(handlers, TryEvalDateTimeFromPartsFunction, "DATETIMEFROMPARTS");
        Register(handlers, TryEvalDateTime2FromPartsFunction, "DATETIME2FROMPARTS");
        Register(handlers, TryEvalDateTimeOffsetFromPartsFunction, "DATETIMEOFFSETFROMPARTS");
        Register(handlers, TryEvalTimeFromPartsFunction, "TIMEFROMPARTS");
        Register(handlers, TryEvalSmallDateTimeFromPartsFunction, "SMALLDATETIMEFROMPARTS");

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

    private static bool TryEvalDateFromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DATEFROMPARTS() espera ano, mês e dia.");

        var yearValue = evalArg(0);
        var monthValue = evalArg(1);
        var dayValue = evalArg(2);
        if (IsNullish(yearValue) || IsNullish(monthValue) || IsNullish(dayValue))
        {
            result = null;
            return true;
        }

        try
        {
            result = new DateTime(
                Convert.ToInt32(yearValue!.ToDec()),
                Convert.ToInt32(monthValue!.ToDec()),
                Convert.ToInt32(dayValue!.ToDec()));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTimeFromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 6)
            throw new InvalidOperationException("DATETIMEFROMPARTS() espera ao menos 6 argumentos.");

        var values = ReadValues(evalArg, 6);
        if (ContainsNullish(values))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, second);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTime2FromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 7)
            throw new InvalidOperationException("DATETIME2FROMPARTS() espera ao menos 7 argumentos.");

        var values = ReadValues(evalArg, 7);
        if (ContainsNullish(values))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            var fraction = Convert.ToInt32(values[6]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, second)
                .AddTicks(fraction * 10L);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTimeOffsetFromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 8)
            throw new InvalidOperationException("DATETIMEOFFSETFROMPARTS() espera ao menos 8 argumentos.");

        var values = ReadValues(evalArg, 8);
        if (ContainsNullish(values))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            var fraction = Convert.ToInt32(values[6]!.ToDec());
            var offsetMinutes = Convert.ToInt32(values[7]!.ToDec());
            var offset = TimeSpan.FromMinutes(offsetMinutes);
            result = new DateTimeOffset(
                new DateTime(year, month, day, hour, minute, second).AddTicks(fraction * 10L),
                offset);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTimeFromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 5)
            throw new InvalidOperationException("TIMEFROMPARTS() espera ao menos 5 argumentos.");

        var values = ReadValues(evalArg, 5);
        if (ContainsNullish(values))
        {
            result = null;
            return true;
        }

        try
        {
            var hour = Convert.ToInt32(values[0]!.ToDec());
            var minute = Convert.ToInt32(values[1]!.ToDec());
            var second = Convert.ToInt32(values[2]!.ToDec());
            var fractions = Convert.ToInt32(values[3]!.ToDec());
            _ = Convert.ToInt32(values[4]!.ToDec());
            result = new TimeSpan(0, hour, minute, second).Add(TimeSpan.FromTicks(fractions * 10L));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSmallDateTimeFromPartsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 5)
            throw new InvalidOperationException("SMALLDATETIMEFROMPARTS() espera ao menos 5 argumentos.");

        var values = ReadValues(evalArg, 5);
        if (ContainsNullish(values))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, 0);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static object?[] ReadValues(Func<int, object?> evalArg, int count)
    {
        var values = new object?[count];
        for (var i = 0; i < count; i++)
            values[i] = evalArg(i);

        return values;
    }

    private static bool ContainsNullish(object?[] values)
    {
        foreach (var value in values)
        {
            if (IsNullish(value))
                return true;
        }

        return false;
    }

    private static bool IsNullish(object? value) => value is null || value is DBNull;
}
