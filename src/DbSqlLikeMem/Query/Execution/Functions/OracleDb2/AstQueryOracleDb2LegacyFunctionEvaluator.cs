namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

internal static class AstQueryOracleDb2LegacyFunctionEvaluator
{
    private delegate bool OracleDb2LegacyFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly IReadOnlyDictionary<string, OracleDb2LegacyFunctionHandler> _handlers = CreateHandlers();
    private static readonly IReadOnlyDictionary<string, DayOfWeek> _oracleDayOfWeekMap = new Dictionary<string, DayOfWeek>(StringComparer.OrdinalIgnoreCase)
    {
        ["SUNDAY"] = DayOfWeek.Sunday,
        ["MONDAY"] = DayOfWeek.Monday,
        ["TUESDAY"] = DayOfWeek.Tuesday,
        ["WEDNESDAY"] = DayOfWeek.Wednesday,
        ["THURSDAY"] = DayOfWeek.Thursday,
        ["FRIDAY"] = DayOfWeek.Friday,
        ["SATURDAY"] = DayOfWeek.Saturday,
        ["SUN"] = DayOfWeek.Sunday,
        ["MON"] = DayOfWeek.Monday,
        ["TUE"] = DayOfWeek.Tuesday,
        ["WED"] = DayOfWeek.Wednesday,
        ["THU"] = DayOfWeek.Thursday,
        ["FRI"] = DayOfWeek.Friday,
        ["SAT"] = DayOfWeek.Saturday
    };

    private static Dictionary<string, OracleDb2LegacyFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, OracleDb2LegacyFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalCollationFunction, "COLLATION");
        Register(handlers, TryEvalConIdFunctions, "CON_DBID_TO_ID", "CON_GUID_TO_ID", "CON_NAME_TO_ID", "CON_UID_TO_ID");
        Register(handlers, TryEvalCubeTableFunction, "CUBE_TABLE");
        Register(handlers, TryEvalCvFunction, "CV");
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
        Register(handlers, TryEvalModFunction, "MOD");
        Register(handlers, TryEvalDivFunction, "DIV");
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(fn, context, evalArg, out result))
        {
            return true;
        }

        if (TryEvalDialectSpecificCastFunction(fn, context, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalCollationFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        var name = fn.Name.ToUpperInvariant();
        if (name is not "COLLATION")
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(context, name);

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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CON_DBID_TO_ID" or "CON_GUID_TO_ID" or "CON_NAME_TO_ID" or "CON_UID_TO_ID"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(context, name);

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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        result = null;

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            return false;

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
                "VARCHAR" => value?.ToString(),
                _ => null
            };
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            AstQueryExecutorBase.LogFunctionEvaluationFailure(e);
            result = null;
        }
#pragma warning restore CA1031

        return true;
    }

    private static bool TryEvalCubeTableFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("CUBE_TABLE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalCvFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("CV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDataObjToPartitionFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!(fn.Name.Equals("DATAOBJ_TO_MAT_PARTITION", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATAOBJ_TO_PARTITION", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDepthFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("DEPTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

        result = 1;
        return true;
    }

    private static bool TryEvalDerefFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("DEREF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = evalArg(0);
        return true;
    }

    private static bool TryEvalDumpFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("DUMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

        var text = value?.ToString() ?? string.Empty;
        result = $"Typ=1 Len={text.Length}";
        return true;
    }

    private static bool TryEvalExistsNodeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("EXISTSNODE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

        result = 1;
        return true;
    }

    private static bool TryEvalFromTzFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("FROM_TZ", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        var dialect = context.Dialect;
        if (!fn.Name.Equals("GROUP_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = 0;
        return true;
    }

    private static bool TryEvalHexToRawFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("HEXTORAW", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            result = null;
            return true;
        }

        if (!TryNormalizeHexPayload(value, out var hex) || hex.Length % 2 != 0)
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        var dialect = context.Dialect;
        if (!fn.Name.Equals("ITERATION_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalJsonDataGuideFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("JSON_DATAGUIDE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

        result = "{}";
        return true;
    }

    private static bool TryEvalJsonTransformFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        var name = fn.Name.ToUpperInvariant();
        if (name is not "JSON_TRANSFORM")
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(context, name);

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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("LNNVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        var dialect = context.Dialect;
        if (!fn.Name.Equals("LOCALTIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!(dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            || dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = DateTime.Now;
        return true;
    }

    private static bool TryEvalLocalTimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        var dialect = context.Dialect;
        if (!fn.Name.Equals("LOCALTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!(dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            || dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = DateTime.Now.TimeOfDay;
        return true;
    }

    private static bool TryEvalLowerFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase))
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!fn.Name.Equals("LTRIM", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalModFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!fn.Name.Equals("MOD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MOD() espera 2 argumentos.");

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
            result = r == 0m ? null : l % r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDivFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("DIV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalMonthsBetweenFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("MONTHS_BETWEEN", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("MIDNIGHT_SECONDS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("NANVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("NEW_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("NEXT_DAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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

        if (!TryParseOracleDayOfWeek(dayValue!, out var targetDay))
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID" or "NLS_CHARSET_NAME" or "NLS_COLLATION_ID"
            or "NLS_COLLATION_NAME" or "NLS_INITCAP" or "NLS_LOWER" or "NLS_UPPER" or "NLSSORT"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(context, name);

        if (name is "NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID")
        {
            result = 0;
            return true;
        }

        if (name is "NLS_CHARSET_NAME")
        {
            result = "AL32UTF8";
            return true;
        }

        if (name is "NLS_COLLATION_ID")
        {
            result = 0;
            return true;
        }

        if (name is "NLS_COLLATION_NAME")
        {
            result = "BINARY";
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (name is "NLS_INITCAP")
        {
            result = ApplyInitCap(text);
            return true;
        }

        if (name is "NLS_LOWER")
        {
            result = text.ToLowerInvariant();
            return true;
        }

        if (name is "NLS_UPPER")
        {
            result = text.ToUpperInvariant();
            return true;
        }

        result = text;
        return true;
    }

    private static bool TryEvalNumIntervalFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!(fn.Name.Equals("NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NUMTOYMINTERVAL", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        if (fn.Name.Equals("NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase))
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("MAKE_REF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDb2DateTruncFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("DATE_TRUNC", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

        var unit = AstQueryExecutorBase.ResolveTemporalUnit(unitText);
        result = AstQueryExecutorBase.TruncateDateTime(dateTime, unit);
        return true;
    }

    private static bool TryEvalTranslateFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!fn.Name.Equals("TRANSLATE", StringComparison.OrdinalIgnoreCase)
            && !fn.Name.Equals("TRANSLATE...USING", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsSqlServerScalarFunction("TRANSLATE")
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryParseOracleDayOfWeek(string value, out DayOfWeek day)
    {
        day = default;
        var normalized = value.Trim().ToUpperInvariant();
        if (_oracleDayOfWeekMap.TryGetValue(normalized, out day))
            return true;

        return false;
    }

    private static string ApplyInitCap(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var builder = new StringBuilder(value.Length);
        var makeUpper = true;
        foreach (var ch in value)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(makeUpper
                    ? char.ToUpperInvariant(ch)
                    : char.ToLowerInvariant(ch));
                makeUpper = false;
            }
            else
            {
                builder.Append(ch);
                makeUpper = true;
            }
        }

        return builder.ToString();
    }

    private static bool TryNormalizeHexPayload(string trimmed, out string hex)
    {
        hex = string.Empty;

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = trimmed[2..];
            return true;
        }

        if (trimmed.Length >= 3
            && (trimmed[0] == 'x' || trimmed[0] == 'X')
            && trimmed[1] == '\''
            && trimmed[^1] == '\'')
        {
            hex = trimmed[2..^1];
            return true;
        }

        hex = trimmed;
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
