namespace DbSqlLikeMem;

using System;
using System.Globalization;

internal static class AstQueryPostgresDateFunctionEvaluator
{
    internal static bool TryEvaluatePostgresDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;
        if (context.Dialect.Functions.TryGetValue(fn.Name, out var handler)
            && handler.AstExecutor != null)
            return handler.AstExecutor(context, fn, evalArg, out result);

        return false;
    }

    internal static void CreateHandlers(
        this QueryExecutionContext context)
    {
        var dialect = context.Dialect;
        dialect.AddScalarFunctions("DATE", TryEvalDateTruncFunction, "DATE_TRUNC");
        dialect.AddScalarFunctions("DATE", TryEvalDatePartFunction, "DATE_PART");
        dialect.AddScalarFunctions("AGE", TryEvalAgeFunction, "AGE");
        dialect.AddScalarFunctions("INTERVAL", TryEvalMakeIntervalFunction, "MAKE_INTERVAL");
        dialect.AddScalarFunctions("DATE", TryEvalMakeDateFunction, "MAKE_DATE");
        dialect.AddScalarFunctions("TIME", TryEvalMakeTimeFunction, "MAKE_TIME");
        dialect.AddScalarFunctions("TIMESTAMP", TryEvalMakeTimestampFunction, "MAKE_TIMESTAMP");
        dialect.AddScalarFunctions("TIMESTAMP", TryEvalMakeTimestamptzFunction, "MAKE_TIMESTAMPTZ");
        dialect.AddScalarFunctions("DATE", TryEvalToDateFunction, "TO_DATE");
        dialect.AddScalarFunctions("VARCHAR", TryEvalToCharFunction, "TO_CHAR");
    }

    private static bool TryEvalDateTruncFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATE_TRUNC() espera unidade e data.");

        var unitText = evalArg(0)?.ToString() ?? string.Empty;
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value) || string.IsNullOrWhiteSpace(unitText) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
        result = AstQueryExecutorBase.TruncateDateTime(dateTime, unit);
        return true;
    }

    private static bool TryEvalDatePartFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATE_PART() espera unidade e data.");

        var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(evalArg(0)?.ToString() ?? string.Empty);
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value)
            || unit == AstQueryExecutorBase.TemporalUnit.Unknown
            || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var temporalPart = AstQueryExecutorBase.GetTemporalPartValue(dateTime, unit);
        result = temporalPart is null ? null : (double)temporalPart.Value;
        return true;
    }

    private static bool TryEvalAgeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var left = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(left)
            || !AstQueryExecutorBase.TryCoerceDateTime(left, out var leftDate))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 1)
        {
            result = DateTime.Now - leftDate;
            return true;
        }

        var right = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(right)
            || !AstQueryExecutorBase.TryCoerceDateTime(right, out var rightDate))
        {
            result = null;
            return true;
        }

        result = leftDate - rightDate;
        return true;
    }

    private static bool TryEvalMakeIntervalFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var years = fn.Args.Count > 0 ? Convert.ToInt32(evalArg(0).ToDec()) : 0;
        var months = fn.Args.Count > 1 ? Convert.ToInt32(evalArg(1).ToDec()) : 0;
        var weeks = fn.Args.Count > 2 ? Convert.ToInt32(evalArg(2).ToDec()) : 0;
        var days = fn.Args.Count > 3 ? Convert.ToInt32(evalArg(3).ToDec()) : 0;
        var hours = fn.Args.Count > 4 ? Convert.ToInt32(evalArg(4).ToDec()) : 0;
        var mins = fn.Args.Count > 5 ? Convert.ToInt32(evalArg(5).ToDec()) : 0;
        var secs = fn.Args.Count > 6 ? Convert.ToDouble(evalArg(6), CultureInfo.InvariantCulture) : 0d;

        result = TimeSpan.FromDays((years * 365) + (months * 30) + (weeks * 7) + days)
            .Add(TimeSpan.FromHours(hours))
            .Add(TimeSpan.FromMinutes(mins))
            .Add(TimeSpan.FromSeconds(secs));
        return true;
    }

    private static bool TryEvalMakeDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("MAKE_DATE() espera ano, mes e dia.");

        var year = Convert.ToInt32(evalArg(0).ToDec());
        var month = Convert.ToInt32(evalArg(1).ToDec());
        var day = Convert.ToInt32(evalArg(2).ToDec());
        result = new DateTime(year, month, day);
        return true;
    }

    private static bool TryEvalMakeTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 3)
            throw new InvalidOperationException("MAKE_TIME() espera hora, minuto e segundo.");

        var hour = Convert.ToInt32(evalArg(0).ToDec());
        var minute = Convert.ToInt32(evalArg(1).ToDec());
        var second = Convert.ToInt32(evalArg(2).ToDec());
        result = new TimeSpan(hour, minute, second);
        return true;
    }

    private static bool TryEvalMakeTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 6)
            throw new InvalidOperationException("MAKE_TIMESTAMP() espera data e hora.");

        var year = Convert.ToInt32(evalArg(0).ToDec());
        var month = Convert.ToInt32(evalArg(1).ToDec());
        var day = Convert.ToInt32(evalArg(2).ToDec());
        var hour = Convert.ToInt32(evalArg(3).ToDec());
        var minute = Convert.ToInt32(evalArg(4).ToDec());
        var second = Convert.ToInt32(evalArg(5).ToDec());
        result = new DateTime(year, month, day, hour, minute, second);
        return true;
    }

    private static bool TryEvalMakeTimestamptzFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 6)
            throw new InvalidOperationException("MAKE_TIMESTAMPTZ() espera data e hora.");

        var year = Convert.ToInt32(evalArg(0).ToDec());
        var month = Convert.ToInt32(evalArg(1).ToDec());
        var day = Convert.ToInt32(evalArg(2).ToDec());
        var hour = Convert.ToInt32(evalArg(3).ToDec());
        var minute = Convert.ToInt32(evalArg(4).ToDec());
        var second = Convert.ToInt32(evalArg(5).ToDec());
        result = new DateTimeOffset(year, month, day, hour, minute, second, DateTimeOffset.Now.Offset);
        return true;
    }

    private static bool TryEvalToDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is DateTime dateValue)
        {
            result = dateValue.Date;
            return true;
        }

        var textValue = value?.ToString() ?? string.Empty;
        var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
        if (AstQueryFormatFunctionHelper.TryParseOracleDateTime(textValue, maskValue, out var parsed))
        {
            result = parsed.Date;
            return true;
        }

        if (AstQueryExecutorBase.TryParseCachedDateTime(textValue, DateTimeStyles.AllowWhiteSpaces, out var fallbackParsed))
        {
            result = fallbackParsed.Date;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalToCharFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is DateTime dateValue)
        {
            if (fn.Args.Count > 1 && evalArg(1) is string fmt)
            {
                var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                result = dateValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
            }
            else
            {
                result = dateValue.ToString(CultureInfo.InvariantCulture);
            }

            return true;
        }

        if (value is DateTimeOffset dtoValue)
        {
            if (fn.Args.Count > 1 && evalArg(1) is string fmt)
            {
                var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                result = dtoValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
            }
            else
            {
                result = dtoValue.ToString(CultureInfo.InvariantCulture);
            }

            return true;
        }

        if (AstQueryFormatFunctionHelper.IsNumericValue(value))
        {
            var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (!string.IsNullOrWhiteSpace(mask))
            {
                result = AstQueryFormatFunctionHelper.FormatPostgreSqlNumber(value!, mask!);
                return true;
            }
        }

        result = value!.ToString();
        return true;
    }
}
