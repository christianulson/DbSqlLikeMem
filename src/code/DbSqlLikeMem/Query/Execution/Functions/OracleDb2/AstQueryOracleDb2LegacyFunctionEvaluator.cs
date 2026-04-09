namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

internal static class AstQueryOracleDb2LegacyFunctionEvaluator
{
    private delegate bool OracleDb2LegacyFunctionHandler(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly IReadOnlyDictionary<string, OracleDb2LegacyFunctionHandler> _handlers = CreateHandlers();
    private static readonly HashSet<string> _conIdFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "CON_DBID_TO_ID",
        "CON_GUID_TO_ID",
        "CON_NAME_TO_ID",
        "CON_UID_TO_ID"
    };

    private static readonly HashSet<string> _nlsFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "NLS_CHARSET_DECL_LEN",
        "NLS_CHARSET_ID",
        "NLS_CHARSET_NAME",
        "NLS_COLLATION_ID",
        "NLS_COLLATION_NAME",
        "NLS_INITCAP",
        "NLS_LOWER",
        "NLS_UPPER",
        "NLSSORT"
    };

    private static Dictionary<string, OracleDb2LegacyFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, OracleDb2LegacyFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalCollationFunction, "COLLATION");
        Register(handlers, TryEvalConIdFunctions, "CON_DBID_TO_ID", "CON_GUID_TO_ID", "CON_NAME_TO_ID", "CON_UID_TO_ID");
        Register(handlers, TryEvalCubeTableFunction, "CUBE_TABLE");
        Register(handlers, TryEvalCvFunction, SqlConst.CV);
        Register(handlers, TryEvalDataObjToPartitionFunctions, "DATAOBJ_TO_MAT_PARTITION", "DATAOBJ_TO_PARTITION");
        Register(handlers, TryEvalDepthFunction, "DEPTH");
        Register(handlers, TryEvalDerefFunction, "DEREF");
        Register(handlers, TryEvalDumpFunction, "DUMP");
        Register(handlers, TryEvalExistsNodeFunction, "EXISTSNODE");
        Register(handlers, TryEvalFromTzFunction, "FROM_TZ");
        Register(handlers, TryEvalGroupIdFunction, "GROUP_ID");
        Register(handlers, TryEvalHexToRawFunction, "HEXTORAW");
        Register(handlers, TryEvalIterationNumberFunction, "ITERATION_NUMBER");
        Register(handlers, TryEvalJsonDataGuideFunction, "JSON_DATAGUIDE");
        Register(handlers, TryEvalJsonTransformFunction, "JSON_TRANSFORM");
        Register(handlers, TryEvalLnnvlFunction, "LNNVL");
        Register(handlers, TryEvalLocalTimestampFunction, "LOCALTIMESTAMP");
        Register(handlers, TryEvalLocalTimeFunction, "LOCALTIME");
        Register(handlers, TryEvalLowerFunction, "LOWER");
        Register(handlers, TryEvalLtrimFunction, "LTRIM");
        Register(handlers, TryEvalDivFunction, "DIV");
        Register(handlers, TryEvalTimestampAddFunction, "TIMESTAMPADD");
        Register(handlers, TryEvalTimestampDiffFunction, "TIMESTAMPDIFF");
        Register(handlers, TryEvalMonthsBetweenFunction, "MONTHS_BETWEEN");
        Register(handlers, TryEvalMidnightSecondsFunction, "MIDNIGHT_SECONDS");
        Register(handlers, TryEvalNanvlFunction, "NANVL");
        Register(handlers, TryEvalNewTimeFunction, "NEW_TIME");
        Register(handlers, TryEvalNextDayFunction, "NEXT_DAY");
        Register(handlers, TryEvalNlsFunctions, "NLS_CHARSET_DECL_LEN", "NLS_CHARSET_ID", "NLS_CHARSET_NAME", "NLS_COLLATION_ID", "NLS_COLLATION_NAME", "NLS_INITCAP", "NLS_LOWER", "NLS_UPPER", "NLSSORT");
        Register(handlers, TryEvalNumIntervalFunctions, "NUMTODSINTERVAL", "NUMTOYMINTERVAL");
        Register(handlers, TryEvalMakeRefFunction, "MAKE_REF");
        Register(handlers, TryEvalDb2DateTruncFunction, "DATE_TRUNC");
        Register(handlers, TryEvalTranslateFunctions, "TRANSLATE", "TRANSLATE...USING");

        return handlers;
    }

    private static void Register(
        IDictionary<string, OracleDb2LegacyFunctionHandler> handlers,
        OracleDb2LegacyFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(context, fn, evalArg, out result))
        {
            return true;
        }

        if (TryEvalDialectSpecificCastFunction(context, fn, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalCollationFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "COLLATION")
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = "BINARY";
        return true;
    }

    private static bool TryEvalConIdFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_conIdFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Convert.ToInt64(value.ToDec(), CultureInfo.InvariantCulture);
        }
        catch
        {
            result = null;
        }

        return true;
    }

    internal static bool TryEvalDialectSpecificCastFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var normalizedName = fn.Name.ToUpperInvariant();
        if (normalizedName is not ("BIGINT" or "BPCHAR" or "DBCLOB" or "DEC" or "DECIMAL" or "DOUBLE" or "DOUBLE_PRECISION" or "FLOAT" or "FLOAT4" or "FLOAT8" or "GRAPHIC" or "INT" or "INTEGER" or "REAL" or "SMALLINT" or "VARGRAPHIC" or "VARCHAR"))
            return false;

        if (fn.Args.Count == 0)
            return false;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
            return true;

        try
        {
            result = normalizedName switch
            {
                "BIGINT" => CoerceToInt64(value!),
                "SMALLINT" => CoerceToInt16(value!),
                "INT" or "INTEGER" => CoerceToInt32(value!),
                "DEC" or "DECIMAL" => CoerceToDecimal(value!),
                "DOUBLE" or "DOUBLE_PRECISION" or "FLOAT" or "FLOAT4" or "FLOAT8" or "REAL" => CoerceToDouble(value!),
                "BPCHAR" or "DBCLOB" or "GRAPHIC" or "VARGRAPHIC" => value?.ToString(),
                "VARCHAR" => value is byte[] bytes ? bytes : value?.ToString(),
                _ => null
            };
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            AstQueryExecutionRuntimeHelper.LogFunctionEvaluationFailure(e);
            result = null;
        }
#pragma warning restore CA1031

        return true;
    }

    private static bool TryEvalCubeTableFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "CUBE_TABLE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = null;
        return true;
    }

    private static bool TryEvalCvFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, SqlConst.CV, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDataObjToPartitionFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "DATAOBJ_TO_MAT_PARTITION", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "DATAOBJ_TO_PARTITION", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDepthFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "DEPTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalDerefFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "DEREF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = evalArg(0);
        return true;
    }

    private static bool TryEvalDumpFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "DUMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = $"Typ=1 Len={text.Length}";
        return true;
    }

    private static bool TryEvalExistsNodeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "EXISTSNODE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalFromTzFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "FROM_TZ", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FROM_TZ() espera data e fuso.");

        var baseValue = evalArg(0);
        var tzValue = evalArg(1)?.ToString();
        if (AstQueryExecutorBase.IsNullish(baseValue) || string.IsNullOrWhiteSpace(tzValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!SqlTemporalFunctionEvaluator.TryParseOffset(tzValue!, out var offset))
        {
            result = null;
            return true;
        }

        result = new DateTimeOffset(dateTime, offset);
        return true;
    }

    private static bool TryEvalGroupIdFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "GROUP_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = 0;
        return true;
    }

    private static bool TryEvalHexToRawFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "HEXTORAW", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            result = null;
            return true;
        }

        if (!AstQueryRuntimeHelper.TryNormalizeHexPayload(value, out var hex) || hex.Length % 2 != 0)
        {
            result = null;
            return true;
        }

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!byte.TryParse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
            {
                result = null;
                return true;
            }

            buffer[i / 2] = part;
        }

        result = buffer;
        return true;
    }

    private static bool TryEvalIterationNumberFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "ITERATION_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalJsonDataGuideFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "JSON_DATAGUIDE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = "{}";
        return true;
    }

    private static bool TryEvalJsonTransformFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "JSON_TRANSFORM")
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value is string text ? text : value!.ToString();
        return true;
    }

    private static bool TryEvalLnnvlFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "LNNVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 1;
            return true;
        }

        result = value.ToBool() ? 0 : 1;
        return true;
    }

    private static bool TryEvalLocalTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "LOCALTIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.Now;
        return true;
    }

    private static bool TryEvalLocalTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "LOCALTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.Now.TimeOfDay;
        return true;
    }

    private static bool TryEvalLowerFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "LOWER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString()?.ToLowerInvariant();
        return true;
    }

    private static bool TryEvalLtrimFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "LTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString()?.TrimStart();
        return true;
    }

    private static bool TryEvalDivFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "DIV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DIV() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            var r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            result = r == 0m ? null : decimal.Truncate(l / r);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTimestampAddFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "TIMESTAMPADD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("TIMESTAMPADD() espera 3 argumentos.");

        var intervalValue = evalArg(0);
        var amountValue = evalArg(1);
        var timestampValue = evalArg(2);
        if (!TryResolveDb2IntervalCode(intervalValue, out var intervalCode)
            || AstQueryExecutorBase.IsNullish(amountValue)
            || AstQueryExecutorBase.IsNullish(timestampValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(timestampValue, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!TryApplyDb2TimestampDelta(dateTime, intervalCode, Convert.ToInt32((amountValue ?? 0m).ToDec()), out var added))
        {
            result = null;
            return true;
        }

        result = added;
        return true;
    }

    private static bool TryEvalTimestampDiffFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "TIMESTAMPDIFF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("TIMESTAMPDIFF() espera 3 argumentos.");

        var intervalValue = evalArg(0);
        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (!TryResolveDb2IntervalCode(intervalValue, out var intervalCode)
            || AstQueryExecutorBase.IsNullish(startValue)
            || AstQueryExecutorBase.IsNullish(endValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var startDate)
            || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var endDate))
        {
            result = null;
            return true;
        }

        result = TryGetDb2TimestampDifference(startDate, endDate, intervalCode);
        return true;
    }

    private static bool TryResolveDb2IntervalCode(object? value, out int intervalCode)
    {
        intervalCode = 0;

        if (AstQueryExecutorBase.IsNullish(value))
            return false;

        if (value is string text)
            return TryResolveDb2IntervalCode(text, out intervalCode);

        try
        {
            intervalCode = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            return intervalCode > 0;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryResolveDb2IntervalCode(string text, out int intervalCode)
    {
        intervalCode = text.Trim().ToUpperInvariant() switch
        {
            "MICROSECOND" or "MICROSECONDS" => 1,
            "SECOND" or "SECONDS" => 2,
            "MINUTE" or "MINUTES" => 4,
            "HOUR" or "HOURS" => 8,
            "DAY" or "DAYS" => 16,
            "WEEK" or "WEEKS" => 32,
            "MONTH" or "MONTHS" => 64,
            "QUARTER" or "QUARTERS" => 128,
            "YEAR" or "YEARS" => 256,
            _ => 0
        };

        return intervalCode > 0;
    }

    private static bool TryApplyDb2TimestampDelta(DateTime dateTime, int intervalCode, int amount, out DateTime result)
    {
        try
        {
            result = intervalCode switch
            {
                1 => dateTime.AddTicks(checked(amount * 10L)),
                2 => dateTime.AddSeconds(amount),
                4 => dateTime.AddMinutes(amount),
                8 => dateTime.AddHours(amount),
                16 => dateTime.AddDays(amount),
                32 => dateTime.AddDays(amount * 7d),
                64 => dateTime.AddMonths(amount),
                128 => dateTime.AddMonths(amount * 3),
                256 => dateTime.AddYears(amount),
                _ => dateTime
            };

            return intervalCode is 1 or 2 or 4 or 8 or 16 or 32 or 64 or 128 or 256;
        }
        catch
        {
            result = default;
            return false;
        }
    }

    private static object? TryGetDb2TimestampDifference(DateTime start, DateTime end, int intervalCode)
        => intervalCode switch
        {
            1 => (end - start).Ticks / 10L,
            2 => (int)(end - start).TotalSeconds,
            4 => (int)(end - start).TotalMinutes,
            8 => (int)(end - start).TotalHours,
            16 => (int)(end.Date - start.Date).TotalDays,
            32 => (int)Math.Truncate((end.Date - start.Date).TotalDays / 7d),
            64 => (end.Year - start.Year) * 12 + end.Month - start.Month,
            128 => ((end.Year - start.Year) * 12 + end.Month - start.Month) / 3,
            256 => end.Year - start.Year,
            _ => null
        };

    private static bool TryEvalMonthsBetweenFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MONTHS_BETWEEN", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MONTHS_BETWEEN() espera duas datas.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(left, out var leftDate) || !AstQueryExecutorBase.TryCoerceDateTime(right, out var rightDate))
        {
            result = null;
            return true;
        }

        var monthsLeft = leftDate.Year * 12 + leftDate.Month;
        var monthsRight = rightDate.Year * 12 + rightDate.Month;
        var monthDiff = monthsLeft - monthsRight;
        var dayDiff = (leftDate.Day - rightDate.Day) / 31m;
        result = monthDiff + dayDiff;
        return true;
    }

    private static bool TryEvalMidnightSecondsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MIDNIGHT_SECONDS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

        if (value is TimeSpan timeSpan)
        {
            result = (int)timeSpan.TotalSeconds;
            return true;
        }

        if (value is DateTime dateTime)
        {
            result = (int)dateTime.TimeOfDay.TotalSeconds;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            result = (int)dto.TimeOfDay.TotalSeconds;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceTimeSpan(value, out var parsedTime))
        {
            result = (int)parsedTime.TotalSeconds;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(value, out var parsedDate))
        {
            result = (int)parsedDate.TimeOfDay.TotalSeconds;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalNanvlFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "NANVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("NANVL() espera 2 argumentos.");

        var first = evalArg(0);
        var second = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(first))
        {
            result = second;
            return true;
        }

        var number = Convert.ToDouble(first, CultureInfo.InvariantCulture);
        result = double.IsNaN(number) ? second : first;
        return true;
    }

    private static bool TryEvalNewTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "NEW_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("NEW_TIME() espera data e dois fusos.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var fromTz = evalArg(1)?.ToString() ?? string.Empty;
        var toTz = evalArg(2)?.ToString() ?? string.Empty;
        if (!SqlTemporalFunctionEvaluator.TryParseOffset(fromTz, out var fromOffset) || !SqlTemporalFunctionEvaluator.TryParseOffset(toTz, out var toOffset))
        {
            result = null;
            return true;
        }

        var dto = new DateTimeOffset(dateTime, fromOffset);
        result = dto.ToOffset(toOffset).DateTime;
        return true;
    }

    private static bool TryEvalNextDayFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "NEXT_DAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("NEXT_DAY() espera data e nome do dia.");

        var baseValue = evalArg(0);
        var dayValue = evalArg(1)?.ToString();
        if (AstQueryExecutorBase.IsNullish(baseValue) || string.IsNullOrWhiteSpace(dayValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!AstQueryRuntimeHelper.TryParseOracleDayOfWeek(dayValue!, out var targetDay))
        {
            result = null;
            return true;
        }

        var current = dateTime.Date;
        var daysAhead = ((int)targetDay - (int)current.DayOfWeek + 7) % 7;
        if (daysAhead == 0)
            daysAhead = 7;

        result = current.AddDays(daysAhead);
        return true;
    }

    private static bool TryEvalNlsFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_nlsFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        return name switch
        {
            "NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID" => SetNlsConstantResult(0, out result),
            "NLS_CHARSET_NAME" => SetNlsConstantResult("AL32UTF8", out result),
            "NLS_COLLATION_ID" => SetNlsConstantResult(0, out result),
            "NLS_COLLATION_NAME" => SetNlsConstantResult("BINARY", out result),
            "NLS_INITCAP" => TryEvalNlsTextResult(evalArg, AstQueryRuntimeHelper.ApplyInitCap, out result),
            "NLS_LOWER" => TryEvalNlsTextResult(evalArg, static text => text.ToLowerInvariant(), out result),
            "NLS_UPPER" => TryEvalNlsTextResult(evalArg, static text => text.ToUpperInvariant(), out result),
            "NLSSORT" => TryEvalNlsTextResult(evalArg, static text => text, out result),
            _ => SetNlsUnsupportedResult(out result)
        };
    }

    private static bool SetNlsConstantResult(object? value, out object? result)
    {
        result = value;
        return true;
    }

    private static bool TryEvalNlsTextResult(
        Func<int, object?> evalArg,
        Func<string, string> transform,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = transform(text);
        return true;
    }

    private static bool SetNlsUnsupportedResult(out object? result)
    {
        result = null;
        return false;
    }

    private static bool TryEvalNumIntervalFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "NUMTOYMINTERVAL", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() espera número e unidade.");

        var numberValue = evalArg(0);
        var unitValue = evalArg(1)?.ToString();
        if (AstQueryExecutorBase.IsNullish(numberValue) || string.IsNullOrWhiteSpace(unitValue))
        {
            result = null;
            return true;
        }

        var number = Convert.ToDouble(numberValue, CultureInfo.InvariantCulture);
        var unit = unitValue!.Trim().ToUpperInvariant();
        if (string.Equals(fn.Name, "NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase))
        {
            result = unit switch
            {
                "DAY" or "DAYS" => TimeSpan.FromDays(number),
                "HOUR" or "HOURS" => TimeSpan.FromHours(number),
                "MINUTE" or "MINUTES" => TimeSpan.FromMinutes(number),
                "SECOND" or "SECONDS" => TimeSpan.FromSeconds(number),
                _ => (TimeSpan?)null
            };
            return true;
        }

        result = unit switch
        {
            SqlConst.YEAR or "YEARS" => TimeSpan.FromDays(365d * number),
            "MONTH" or "MONTHS" => TimeSpan.FromDays(30d * number),
            _ => (TimeSpan?)null
        };
        return true;
    }

    private static bool TryEvalMakeRefFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MAKE_REF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDb2DateTruncFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "DATE_TRUNC", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

    private static bool TryEvalTranslateFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TRANSLATE", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "TRANSLATE...USING", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var from = evalArg(1)?.ToString() ?? string.Empty;
        var to = evalArg(2)?.ToString() ?? string.Empty;

        var builder = new StringBuilder(source.Length);
        foreach (var ch in source)
        {
            var index = from.IndexOf(ch);
            if (index < 0)
            {
                builder.Append(ch);
                continue;
            }

            if (index < to.Length)
                builder.Append(to[index]);
        }

        result = builder.ToString();
        return true;
    }

    private static long CoerceToInt64(object value)
    {
        if (value is long longValue)
            return longValue;

        if (value is int intValue)
            return intValue;

        if (value is short shortValue)
            return shortValue;

        if (value is decimal decimalValue)
            return (long)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return parsedLong;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (long)parsedDecimal;

        return 0L;
    }

    private static int CoerceToInt32(object value)
    {
        if (value is int intValue)
            return intValue;

        if (value is long longValue)
            return (int)longValue;

        if (value is short shortValue)
            return shortValue;

        if (value is decimal decimalValue)
            return (int)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
            return parsedInt;

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return (int)parsedLong;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (int)parsedDecimal;

        return 0;
    }

    private static short CoerceToInt16(object value)
    {
        if (value is short shortValue)
            return shortValue;

        if (value is int intValue)
            return (short)intValue;

        if (value is long longValue)
            return (short)longValue;

        if (value is decimal decimalValue)
            return (short)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (short.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedShort))
            return parsedShort;

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
            return (short)parsedInt;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (short)parsedDecimal;

        return 0;
    }

    private static decimal CoerceToDecimal(object value)
    {
        if (value is decimal decimalValue)
            return decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return parsedDecimal;

        return 0m;
    }

    private static double CoerceToDouble(object value)
    {
        if (value is double doubleValue)
            return doubleValue;

        if (value is float floatValue)
            return floatValue;

        if (value is decimal decimalValue)
            return (double)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDouble))
            return parsedDouble;

        return 0d;
    }
}
