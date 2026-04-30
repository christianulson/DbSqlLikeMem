using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static partial class SqlServerScalarFunctionRegistry
{
    internal static void Register(SqlServerDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        RegisterGeneratedScalarFunctions(dialect);
        if (version < SqlServerDialect.TranslateMinVersion)
            dialect.Functions.Remove("TRANSLATE");

        var utilityEvaluator = new AstQuerySqlServerUtilityFunctionEvaluator(
            getDialect: () => dialect,
            tryConvertNumericToDecimal: AstQueryExecutorBase.TryConvertNumericToDecimal,
            tryCoerceDateTime: AstQueryExecutorBase.TryCoerceDateTime,
            tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
            tryParseCachedDateTimeOffset: AstQueryExecutorBase.TryParseCachedDateTimeOffset);

        bool TryEvalSqlServerUtilityFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => utilityEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalSqlServerSessionContextFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => utilityEvaluator.TryEvalSessionContextFunction(context, fn, evalArg, out result);

        RegisterTemporalFunctions(dialect, version);
        RegisterMetadataFunctions(dialect, version);
        if (version >= SqlServerDialect.SessionContextMinVersion)
            dialect.AddScalarFunction(
                CreateScalarFunctionDef(
                    "SESSION_CONTEXT",
                    "VARCHAR",
                    TryEvalSqlServerSessionContextFunction));
        RegisterScalarFunctions(dialect, version, TryEvalSqlServerUtilityFunction, TryEvalSqlServerSessionContextFunction, utilityEvaluator);
        RegisterAggregateFunctions(dialect, version);
        RegisterFromPartsFunctions(dialect, version);
    }

    private static DbFunctionDef CreateScalarFunctionDef(
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executor,
        DbInvocationStyle invocationStyle = DbInvocationStyle.Call,
        SqlTemporalFunctionKind? temporalKind = null)
    {
        var definition = temporalKind is SqlTemporalFunctionKind temporal
            ? DbFunctionDef.CreateTemporal(name, returnTypeSql, temporal, invocationStyle)
            : invocationStyle switch
            {
                DbInvocationStyle.Identifier => DbFunctionDef.CreateIdentifier(name, returnTypeSql),
                _ when invocationStyle == (DbInvocationStyle.Call | DbInvocationStyle.Identifier) => DbFunctionDef.CreateCallOrIdentifier(name, returnTypeSql),
                _ => DbFunctionDef.CreateScalar(name, returnTypeSql, invocationStyle: invocationStyle)
            };

        return definition with
        {
            AstExecutor = executor
        };
    }

    [ScalarFunction("CURRENT_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 2)]
    private static bool TryEvalSqlServerCurrentTimestampFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgIdentifier("CURRENT_TIMESTAMP", out result);
    }

    [ScalarFunction("GETDATE", "DATETIME", TemporalKind = 2)]
    private static bool TryEvalSqlServerGetDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgCall("GETDATE", out result);
    }

    [ScalarFunction("GETUTCDATE", "DATETIME", TemporalKind = 2)]
    private static bool TryEvalSqlServerGetUtcDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgCall("GETUTCDATE", out result);
    }

    [ScalarFunction("SYSTEMDATE", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 2)]
    private static bool TryEvalSqlServerSystemDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgIdentifier("SYSTEMDATE", out result);
    }

    [ScalarFunction("SYSDATETIME", "DATETIME", TemporalKind = 2, MinVersion = SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion)]
    private static bool TryEvalSqlServerSysDateTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgCall("SYSDATETIME", out result);
    }

    [ScalarFunction("SYSUTCDATETIME", "DATETIME", TemporalKind = 2, MinVersion = SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion)]
    private static bool TryEvalSqlServerSysUtcDateTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgCall("SYSUTCDATETIME", out result);
    }

    [ScalarFunction("SYSDATETIMEOFFSET", "DATETIMEOFFSET", TemporalKind = 3, MinVersion = SqlServerDialect.DateTimeOffsetFunctionsMinVersion)]
    private static bool TryEvalSqlServerSysDateTimeOffsetFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        return context.TryEvaluateZeroArgCall("SYSDATETIMEOFFSET", out result);
    }

    [ScalarFunction("EOMONTH", "DATE", MinVersion = SqlServerDialect.EomonthMinVersion)]
    private static bool TryEvalSqlServerEomonthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return AstQuerySqlServerCompatibilityFunctionEvaluator.TryEvalEomonthFunction(fn, evalArg, out result);
    }

    [ScalarFunction("DATENAME", "VARCHAR")]
    private static bool TryEvalSqlServerDateNameFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var unitText = SqlServerTemporalUnitHelper.GetUnitText(fn.Args[0], evalArg);
        var isIsoWeek = SqlServerTemporalUnitHelper.IsIsoWeek(unitText);
        var unit = isIsoWeek
            ? AstQueryExecutorBase.TemporalUnit.Week
            : SqlServerTemporalUnitHelper.Resolve(unitText);
        if (SqlServerTemporalUnitHelper.IsTimeZoneOffset(unitText))
        {
            var offsetValue = evalArg(1);
            if (!SqlServerTemporalUnitHelper.TryResolveTimeZoneOffsetMinutes(offsetValue, out var offsetMinutes))
            {
                result = null;
                return true;
            }

            result = offsetMinutes.ToString(CultureInfo.InvariantCulture);
            return true;
        }
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && (unit == AstQueryExecutorBase.TemporalUnit.Microsecond
                || unit == AstQueryExecutorBase.TemporalUnit.Nanosecond))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value)
            || unit == AstQueryExecutorBase.TemporalUnit.Unknown
            || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (isIsoWeek)
        {
            result = AstQueryExecutorBase.GetIsoWeekOfYear(dateTime).ToString(CultureInfo.InvariantCulture);
            return true;
        }

        result = unit switch
        {
            AstQueryExecutorBase.TemporalUnit.Year => dateTime.Year.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Month => dateTime.ToString("MMMM", CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Day => dateTime.Day.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Yearday => dateTime.DayOfYear.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Week => SqlServerTemporalUnitHelper.GetWeekOfYear(dateTime).ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Weekday => SqlServerTemporalUnitHelper.GetWeekdayName(dateTime),
            AstQueryExecutorBase.TemporalUnit.Hour => dateTime.Hour.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Minute => dateTime.Minute.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Second => dateTime.Second.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Millisecond => dateTime.Millisecond.ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Microsecond => ((int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L)).ToString(CultureInfo.InvariantCulture),
            AstQueryExecutorBase.TemporalUnit.Nanosecond => ((int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L)).ToString(CultureInfo.InvariantCulture),
            _ => null
        };
        return true;
    }

    [ScalarFunction("DATEPART", "INT")]
    private static bool TryEvalSqlServerDatePartFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDatePartLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("DAY", "INT")]
    private static bool TryEvalSqlServerDayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDatePartLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("MONTH", "INT")]
    private static bool TryEvalSqlServerMonthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDatePartLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("YEAR", "INT")]
    private static bool TryEvalSqlServerYearFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDatePartLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("DATETRUNC", "DATETIME", MinVersion = SqlServerDialect.DateTruncMinVersion)]
    private static bool TryEvalSqlServerDateTruncFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var unitText = SqlServerTemporalUnitHelper.GetUnitText(fn.Args[0], evalArg);
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value)
            || string.IsNullOrWhiteSpace(unitText)
            || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var isIsoWeek = SqlServerTemporalUnitHelper.IsIsoWeek(unitText);
        var unit = isIsoWeek
            ? AstQueryExecutorBase.TemporalUnit.Week
            : SqlServerTemporalUnitHelper.Resolve(unitText);
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && (unit == AstQueryExecutorBase.TemporalUnit.Microsecond
                || unit == AstQueryExecutorBase.TemporalUnit.Nanosecond))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");
        if (unit == AstQueryExecutorBase.TemporalUnit.Nanosecond)
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");
        if (unit == AstQueryExecutorBase.TemporalUnit.Weekday)
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        if (isIsoWeek)
        {
            result = SqlServerTemporalUnitHelper.TruncateToIsoWeekStart(dateTime);
            return true;
        }

        result = unit switch
        {
            AstQueryExecutorBase.TemporalUnit.Year => new DateTime(dateTime.Year, 1, 1, 0, 0, 0, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Month => new DateTime(dateTime.Year, dateTime.Month, 1, 0, 0, 0, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Day => dateTime.Date,
            AstQueryExecutorBase.TemporalUnit.Yearday => dateTime.Date,
            AstQueryExecutorBase.TemporalUnit.Week => SqlServerTemporalUnitHelper.TruncateToWeekStart(dateTime),
            AstQueryExecutorBase.TemporalUnit.Hour => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Minute => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Second => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Millisecond => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond, dateTime.Kind),
            AstQueryExecutorBase.TemporalUnit.Microsecond => new DateTime(dateTime.Ticks - (dateTime.Ticks % 10), dateTime.Kind),
            _ => dateTime
        };
        return true;
    }

    [ScalarFunction("DATEDIFF", "INT")]
    private static bool TryEvalSqlServerDateDiffFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDateDiffLikeFunction(context, fn, evalArg, false, out result);

    [ScalarFunction("DATEDIFF_BIG", "BIGINT", MinVersion = SqlServerDialect.DateDiffBigMinVersion)]
    private static bool TryEvalSqlServerDateDiffBigFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalSqlServerDateDiffLikeFunction(context, fn, evalArg, true, out result);

    [ScalarFunction("DATEADD", "DATETIME")]
    private static bool TryEvalSqlServerDateAddFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var baseValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(baseValue)
            || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var amountValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(amountValue))
        {
            result = null;
            return true;
        }

        var unitText = SqlServerTemporalUnitHelper.GetUnitText(fn.Args[0], evalArg);
        if (SqlServerTemporalUnitHelper.IsIsoWeek(unitText))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        var unit = SqlServerTemporalUnitHelper.Resolve(unitText);
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && unit == AstQueryExecutorBase.TemporalUnit.Microsecond)
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && unit == AstQueryExecutorBase.TemporalUnit.Nanosecond)
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        if (unit == AstQueryExecutorBase.TemporalUnit.Unknown)
        {
            result = dateTime;
            return true;
        }

        var amount = (int)decimal.Truncate(amountValue.ToDec());
        result = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
        return true;
    }

    [ScalarFunction("TODATETIMEOFFSET", "DATETIMEOFFSET", MinVersion = SqlServerDialect.DateTimeOffsetFunctionsMinVersion)]
    private static bool TryEvalSqlServerToDateTimeOffsetGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() expects value and offset.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        var offsetText = evalArg(1)?.ToString() ?? string.Empty;
        if (!SqlTemporalFunctionEvaluator.TryParseOffset(offsetText, out var offset))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        result = new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified), offset);
        return true;
    }

    [ScalarFunction("SWITCHOFFSET", "DATETIMEOFFSET", MinVersion = SqlServerDialect.DateTimeOffsetFunctionsMinVersion)]
    private static bool TryEvalSqlServerSwitchOffsetGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() expects value and offset.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        var offsetText = evalArg(1)?.ToString() ?? string.Empty;
        if (!SqlTemporalFunctionEvaluator.TryParseOffset(offsetText, out var offset))
        {
            result = null;
            return true;
        }

        DateTimeOffset dto;
        if (baseValue is DateTimeOffset directDto)
        {
            dto = directDto;
        }
        else if (!AstQueryExecutorBase.TryParseCachedDateTimeOffset(baseValue!.ToString()!, DateTimeStyles.AllowWhiteSpaces, out dto))
        {
            result = null;
            return true;
        }

        result = dto.ToOffset(offset);
        return true;
    }

    private static void RegisterTemporalFunctions(
        SqlServerDialect dialect,
        int version)
    {
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("PREVIOUS_VALUE_FOR", "BIGINT"),
            "PREVIOUS_VALUE_FOR");
    }

    private static bool TryEvalSqlServerDatePartLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        var isDatePart = name.Equals("DATEPART", StringComparison.OrdinalIgnoreCase);
        var unitText = isDatePart
            ? SqlServerTemporalUnitHelper.GetUnitText(fn.Args[0], evalArg)
            : name;
        var isIsoWeek = SqlServerTemporalUnitHelper.IsIsoWeek(unitText);
        var valueIndex = isDatePart ? 1 : 0;
        if ((isDatePart && fn.Args.Count < 2)
            || (!isDatePart && fn.Args.Count == 0))
        {
            result = null;
            return true;
        }

        var unit = isIsoWeek
            ? AstQueryExecutorBase.TemporalUnit.Week
            : SqlServerTemporalUnitHelper.Resolve(unitText);
        if (SqlServerTemporalUnitHelper.IsTimeZoneOffset(unitText))
        {
            var offsetValue = evalArg(valueIndex);
            if (!SqlServerTemporalUnitHelper.TryResolveTimeZoneOffsetMinutes(offsetValue, out var offsetMinutes))
            {
                result = null;
                return true;
            }

            result = offsetMinutes;
            return true;
        }
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && (unit == AstQueryExecutorBase.TemporalUnit.Microsecond
                || unit == AstQueryExecutorBase.TemporalUnit.Nanosecond))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        var value = evalArg(valueIndex);
        if (AstQueryExecutorBase.IsNullish(value)
            || unit == AstQueryExecutorBase.TemporalUnit.Unknown
            || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (isIsoWeek)
        {
            result = AstQueryExecutorBase.GetIsoWeekOfYear(dateTime);
            return true;
        }

        result = unit switch
        {
            AstQueryExecutorBase.TemporalUnit.Year => dateTime.Year,
            AstQueryExecutorBase.TemporalUnit.Month => dateTime.Month,
            AstQueryExecutorBase.TemporalUnit.Day => dateTime.Day,
            AstQueryExecutorBase.TemporalUnit.Yearday => dateTime.DayOfYear,
            AstQueryExecutorBase.TemporalUnit.Week => SqlServerTemporalUnitHelper.GetWeekOfYear(dateTime),
            AstQueryExecutorBase.TemporalUnit.Weekday => SqlServerTemporalUnitHelper.GetWeekdayIndex(dateTime),
            AstQueryExecutorBase.TemporalUnit.Hour => dateTime.Hour,
            AstQueryExecutorBase.TemporalUnit.Minute => dateTime.Minute,
            AstQueryExecutorBase.TemporalUnit.Second => dateTime.Second,
            AstQueryExecutorBase.TemporalUnit.Millisecond => dateTime.Millisecond,
            AstQueryExecutorBase.TemporalUnit.Microsecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L),
            AstQueryExecutorBase.TemporalUnit.Nanosecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L),
            _ => null
        };
        return true;
    }

    private static bool TryEvalSqlServerDateDiffLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool isBig,
        out object? result)
    {
        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var unitText = SqlServerTemporalUnitHelper.GetUnitText(fn.Args[0], evalArg);
        if (SqlServerTemporalUnitHelper.IsIsoWeek(unitText))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");

        var unit = SqlServerTemporalUnitHelper.Resolve(unitText);
        if (context.Dialect.Version < SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion
            && (unit == AstQueryExecutorBase.TemporalUnit.Microsecond
                || unit == AstQueryExecutorBase.TemporalUnit.Nanosecond))
            throw SqlUnsupported.NotSupported(context.Dialect, $"{fn.Name}({unitText})");
        if (unit == AstQueryExecutorBase.TemporalUnit.Weekday)
            unit = AstQueryExecutorBase.TemporalUnit.Day;

        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(startValue)
            || AstQueryExecutorBase.IsNullish(endValue)
            || unit == AstQueryExecutorBase.TemporalUnit.Unknown
            || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var startDateTime)
            || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var endDateTime))
        {
            result = null;
            return true;
        }

        var difference = GetTemporalDifference(startDateTime, endDateTime, unit);
        result = isBig ? difference : (int)difference;
        return true;

        static long GetTemporalDifference(DateTime start, DateTime end, AstQueryExecutorBase.TemporalUnit unit)
            => unit switch
            {
                AstQueryExecutorBase.TemporalUnit.Year => end.Year - start.Year,
                AstQueryExecutorBase.TemporalUnit.Month => DiffMonths(start, end),
                AstQueryExecutorBase.TemporalUnit.Yearday => (int)(end.Date - start.Date).TotalDays,
                AstQueryExecutorBase.TemporalUnit.Day => (int)(end.Date - start.Date).TotalDays,
                AstQueryExecutorBase.TemporalUnit.Week => SqlServerTemporalUnitHelper.GetWeekDifference(start, end),
                AstQueryExecutorBase.TemporalUnit.Hour => (int)(end - start).TotalHours,
                AstQueryExecutorBase.TemporalUnit.Minute => (int)(end - start).TotalMinutes,
                AstQueryExecutorBase.TemporalUnit.Second => (int)(end - start).TotalSeconds,
                AstQueryExecutorBase.TemporalUnit.Millisecond => (int)(end - start).TotalMilliseconds,
                AstQueryExecutorBase.TemporalUnit.Microsecond => (long)Math.Truncate((end - start).Ticks / 10d),
                AstQueryExecutorBase.TemporalUnit.Nanosecond => (end - start).Ticks * 100L,
                _ => 0
            };

        static int DiffMonths(DateTime start, DateTime end)
            => (end.Year - start.Year) * 12 + end.Month - start.Month;
    }

    private static void RegisterMetadataFunctions(SqlServerDialect dialect, int version)
    {
#pragma warning disable CS8321
        static bool TryEvalSqlServerContextInfoFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = evalArg;
            result = context.Connection.GetContextInfo();
            return true;
        }

        static bool TryEvalSqlServerConnectionPropertyFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return AstQuerySqlServerSessionFunctionEvaluator.TryEvalSqlServerConnectionPropertyFunction(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerDatabaseMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerDatabaseFunctionEvaluator(
                resolveDatabaseProperty: context.TryResolveSqlServerDatabaseProperty,
                resolveDatabasePrincipalId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerDatabasePrincipalId,
                resolveColumnProperty: context.TryResolveSqlServerColumnProperty,
                resolveColumnLength: context.TryResolveSqlServerColumnLength,
                resolveColumnName: context.TryResolveSqlServerColumnName,
                resolveObjectId: context.TryResolveSqlServerObjectId,
                resolveObjectProperty: context.TryResolveSqlServerObjectProperty,
                resolveObjectName: context.TryResolveSqlServerObjectName,
                resolveObjectSchemaName: context.TryResolveSqlServerObjectSchemaName,
                resolveTypeProperty: AstQuerySqlServerResolutionHelper.TryResolveSqlServerTypeProperty,
                getDatabaseName: () => context.Connection.Database);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerSessionMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerSessionFunctionEvaluator(
                getDialect: () => context.Dialect,
                getContextInfo: context.Connection.GetContextInfo,
                hasActiveTransaction: () => context.Connection.HasActiveTransaction || context.HasActiveTransaction,
                tryResolveSqlServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerRoleMembership,
                tryResolveSqlServerServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerServerRoleMembership);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerIdentityMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerIdentityFunctionEvaluator(
                getDialect: () => context.Dialect,
                getLastInsertId: context.Connection.GetLastInsertId,
                resolveSystemTypeId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeId,
                resolveSystemTypeName: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeName);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }
#pragma warning restore CS8321

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("APPLOCK_MODE", "VARCHAR"),
            "APPLOCK_MODE",
            "APPLOCK_TEST",
            "ASSEMBLYPROPERTY",
            "CERTENCODED",
            "CERTPRIVATEKEY",
            "CURSOR_STATUS",
            "FILE_ID",
            "FILE_IDEX",
            "FILE_NAME",
            "FILEGROUP_ID",
            "FILEGROUP_NAME",
            "FILEGROUPPROPERTY",
            "FILEPROPERTY",
            "FULLTEXTCATALOGPROPERTY",
            "FULLTEXTSERVICEPROPERTY",
            "HAS_PERMS_BY_NAME",
            "INDEX_COL",
            "INDEXKEY_PROPERTY",
            "INDEXPROPERTY",
            "MIN_ACTIVE_ROWVERSION",
            "OBJECT_DEFINITION",
            "PWDCOMPARE",
            "PWDENCRYPT",
            "STATS_DATE",
            "TYPEPROPERTY",
            "XACT_STATE");

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("GET_FILESTREAM_TRANSACTION_CONTEXT", "VARBINARY"));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateIdentifier("@@DATEFIRST", "INT"),
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE");

        dialect.AddScalarFunction(DbFunctionDef.CreateIdentifier("@@IDENTITY", "BIGINT"));

        dialect.AddScalarFunction(DbFunctionDef.CreateIdentifier("@@ROWCOUNT", "BIGINT"));
    }

    private static void RegisterScalarFunctions(
        SqlServerDialect dialect,
        int version,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerUtilityFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerSessionContextFunction,
        AstQuerySqlServerUtilityFunctionEvaluator utilityEvaluator)
    {
        static bool TryEvalSqlServerAtn2Function(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (fn.Args.Count < 2)
            {
                result = null;
                return true;
            }

            var yValue = evalArg(0);
            var xValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(yValue)
                || AstQueryExecutorBase.IsNullish(xValue))
            {
                result = null;
                return true;
            }

            if (!AstQueryExecutorBase.TryConvertNumericToDouble(yValue, out var y)
                || !AstQueryExecutorBase.TryConvertNumericToDouble(xValue, out var x))
            {
                result = null;
                return true;
            }

            result = Math.Atan2(y, x);
            return true;
        }

        dialect.AddScalarFunctions("INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerChecksumFunction);

        dialect.AddScalarFunction(CreateScalarFunctionDef("ATN2", "DOUBLE", TryEvalSqlServerAtn2Function));

        dialect.AddScalarFunction("LEN", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions("INT", AstQueryGroupingFunctionEvaluator.TryEvaluate,
            "GROUPING",
            "GROUPING_ID");

        dialect.AddScalarFunction("LEN", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalLenFunction);

    }

    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect);

    [ScalarFunction("APP_NAME", "VARCHAR")]
    [ScalarFunction("GETANSINULL", "VARCHAR")]
    [ScalarFunction("HOST_ID", "VARCHAR")]
    [ScalarFunction("HOST_NAME", "VARCHAR")]
    [ScalarFunction("CHARINDEX", "INT")]
    [ScalarFunction("CHECKSUM", "INT")]
    [ScalarFunction("BINARY_CHECKSUM", "INT")]
    [ScalarFunction("DATALENGTH", "INT")]
    [ScalarFunction("ISDATE", "INT")]
    [ScalarFunction("ISNUMERIC", "INT")]
    [ScalarFunction("FORMATMESSAGE", "VARCHAR")]
    [ScalarFunction("SOUNDEX", "VARCHAR")]
    [ScalarFunction("PATINDEX", "INT")]
    [ScalarFunction("DIFFERENCE", "INT")]
    [ScalarFunction("NEWID", "VARCHAR")]
    [ScalarFunction("NEWSEQUENTIALID", "VARCHAR")]
    [ScalarFunction("STR", "VARCHAR")]
    [ScalarFunction("COMPRESS", "VARBINARY", MinVersion = SqlServerDialect.CompressionFunctionsMinVersion)]
    [ScalarFunction("DECOMPRESS", "VARBINARY", MinVersion = SqlServerDialect.CompressionFunctionsMinVersion)]
    [ScalarFunction("QUOTENAME", "VARCHAR")]
    [ScalarFunction("REPLICATE", "VARCHAR")]
    [ScalarFunction("STUFF", "VARCHAR")]
    [ScalarFunction("PARSENAME", "VARCHAR")]
    [ScalarFunction("SQUARE", "DOUBLE")]
    private static bool TryEvalGeneratedSqlServerUtilityFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        return name switch
        {
            "APP_NAME" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalAppNameFunction(context, fn, evalArg, out result),
            "GETANSINULL" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalGetAnsiNullFunction(context, fn, evalArg, out result),
            "HOST_ID" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostIdFunction(context, fn, evalArg, out result),
            "HOST_NAME" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostNameFunction(context, fn, evalArg, out result),
            "CHARINDEX" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCharIndexFunction(context, fn, evalArg, out result),
            "CHECKSUM" or "BINARY_CHECKSUM" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerChecksumFunction(context, fn, evalArg, out result),
            "DATALENGTH" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDataLengthFunction(context, fn, evalArg, out result),
            "ISDATE" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsDateFunction(context, fn, evalArg, out result),
            "ISNUMERIC" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsNumericFunction(context, fn, evalArg, out result),
            "FORMATMESSAGE" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatMessageFunction(context, fn, evalArg, out result),
            "SOUNDEX" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSoundexFunction(context, fn, evalArg, out result),
            "DIFFERENCE" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDifferenceFunction(context, fn, evalArg, out result),
            "NEWID" or "NEWSEQUENTIALID" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerGuidFunction(context, fn, evalArg, out result),
            "STR" => new AstQuerySqlServerUtilityFunctionEvaluator(
                getDialect: () => context.Dialect,
                tryConvertNumericToDecimal: AstQueryExecutorBase.TryConvertNumericToDecimal,
                tryCoerceDateTime: AstQueryExecutorBase.TryCoerceDateTime,
                tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
                tryParseCachedDateTimeOffset: AstQueryExecutorBase.TryParseCachedDateTimeOffset)
                .TryEvaluate(fn, context, evalArg, out result),
            "COMPRESS" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerCompressFunction(context, fn, evalArg, out result),
            "DECOMPRESS" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerDecompressFunction(context, fn, evalArg, out result),
            "PATINDEX" => AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result),
            "QUOTENAME" or "REPLICATE" or "STUFF" or "PARSENAME" or "SQUARE" => AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result),
            _ => throw new InvalidOperationException()
        };
    }

    [ScalarFunction("IF", "VARCHAR")]
    [ScalarFunction("IIF", "VARCHAR")]
    [ScalarFunction("ISNULL", "VARCHAR")]
    private static bool TryEvalGeneratedSqlServerConditionalNullFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions(context, fn, evalArg, out result);

    [ScalarFunction("ISJSON", "INT", MinVersion = SqlServerDialect.JsonFunctionsMinVersion)]
    [ScalarFunction("STRING_ESCAPE", "VARCHAR", MinVersion = SqlServerDialect.StringEscapeMinVersion)]
    [ScalarFunction("JSON_MODIFY", "VARCHAR", MinVersion = SqlServerDialect.JsonFunctionsMinVersion)]
    [ScalarFunction("JSON_QUERY", "VARCHAR", MinVersion = SqlServerDialect.JsonFunctionsMinVersion)]
    [ScalarFunction("JSON_VALUE", "VARCHAR", MinVersion = SqlServerDialect.JsonFunctionsMinVersion)]
    private static bool TryEvalGeneratedSqlServerJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        return name switch
        {
            "ISJSON" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsJsonFunction(context, fn, evalArg, out result),
            "STRING_ESCAPE" => new AstQuerySqlServerUtilityFunctionEvaluator(
                getDialect: () => context.Dialect,
                tryConvertNumericToDecimal: AstQueryExecutorBase.TryConvertNumericToDecimal,
                tryCoerceDateTime: AstQueryExecutorBase.TryCoerceDateTime,
                tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
                tryParseCachedDateTimeOffset: AstQueryExecutorBase.TryParseCachedDateTimeOffset)
                .TryEvaluate(fn, context, evalArg, out result),
            "JSON_MODIFY" => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerJsonModifyFunction(context, fn, evalArg, out result),
            "JSON_QUERY" or "JSON_VALUE" => AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction(context, fn, evalArg, out result),
            _ => throw new InvalidOperationException()
        };
    }

    private static bool TryEvalSqlServerTryConvertFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.SupportsTryConvertFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "TRY_CONVERT");

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var type = fn.Args[0] is RawSqlExpr typeRaw ? typeRaw.Sql : (evalArg(0)?.ToString() ?? string.Empty);
        type = type.Trim();
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (context.Dialect.IsIntegerCastTypeName(type))
            {
                if (value is long longValue)
                {
                    result = (int)longValue;
                    return true;
                }

                if (value is int intValue)
                {
                    result = intValue;
                    return true;
                }

                if (value is decimal decimalValue)
                {
                    result = (int)decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (int.TryParse(text, out var intParsed))
                {
                    result = intParsed;
                    return true;
                }

                if (long.TryParse(text, out var longParsed))
                {
                    result = (int)longParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (value is decimal decimalValue)
                {
                    result = decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (decimal.TryParse(text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var decimalParsed))
                {
                    result = decimalParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (value is double doubleValue)
                {
                    result = doubleValue;
                    return true;
                }

                if (value is float floatValue)
                {
                    result = (double)floatValue;
                    return true;
                }

                if (value is decimal decimalValue)
                {
                    result = (double)decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (double.TryParse(text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var doubleParsed))
                {
                    result = doubleParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                result = AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime)
                    ? dateTime
                    : null;
                return true;
            }

            result = value!.ToString();
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    [ScalarFunction("NEXT_VALUE_FOR", "BIGINT", MinVersion = SqlServerDialect.SequenceMinVersion)]
    private static bool TryEvalSqlServerSequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;

        if (!fn.Name.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        return SqlSequenceEvaluator.TryEvaluateCall(
            context.Connection,
            fn.Name,
            fn.Args,
            expr => evalArg(0),
            out result);
    }

    [ScalarFunction("CURRENT_USER", "VARCHAR", InvocationStyle = DbInvocationStyle.Identifier)]
    private static bool TryEvalSqlServerCurrentUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCurrentUserFunction(context, fn, evalArg, out result);

    [ScalarFunction("SESSION_USER", "VARCHAR", InvocationStyle = DbInvocationStyle.Identifier)]
    private static bool TryEvalSqlServerSessionUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSessionUserFunction(context, fn, evalArg, out result);

    [ScalarFunction("SYSTEM_USER", "VARCHAR", InvocationStyle = DbInvocationStyle.Identifier)]
    private static bool TryEvalSqlServerSystemUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSystemUserFunction(context, fn, evalArg, out result);

    [ScalarFunction("FORMAT", "VARCHAR", MinVersion = SqlServerDialect.FormatMinVersion)]
    private static bool TryEvalSqlServerFormatGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatFunction(context, fn, evalArg, out result);

    [ScalarFunction("PARSE", "VARCHAR", MinVersion = SqlServerDialect.ParseMinVersion)]
    private static bool TryEvalSqlServerParseGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalParseLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("TRY_PARSE", "VARCHAR", MinVersion = SqlServerDialect.ParseMinVersion)]
    private static bool TryEvalSqlServerTryParseGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalTryParseLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("CAST", "VARCHAR", MinVersion = SqlServerDialect.TryCastMinVersion)]
    private static bool TryEvalSqlServerCastGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalCastLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("TRY_CAST", "VARCHAR", MinVersion = SqlServerDialect.TryCastMinVersion)]
    private static bool TryEvalSqlServerTryCastGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalTryCastLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("TRY_CONVERT", "VARCHAR", MinVersion = SqlServerDialect.TryConvertMinVersion)]
    private static bool TryEvalSqlServerTryConvertGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalTryConvertLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("CONNECTIONPROPERTY", "VARCHAR")]
    private static bool TryEvalSqlServerConnectionPropertyGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerSessionFunctionEvaluator.TryEvalSqlServerConnectionPropertyFunction(fn, evalArg, out result);

    [ScalarFunction("CONTEXT_INFO", "VARBINARY")]
    private static bool TryEvalSqlServerContextInfoGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.Connection.GetContextInfo();
        return true;
    }

    [ScalarFunction("DATABASEPROPERTYEX", "VARCHAR")]
    [ScalarFunction("DATABASE_PRINCIPAL_ID", "INT")]
    [ScalarFunction("COLUMNPROPERTY", "INT")]
    [ScalarFunction("COL_LENGTH", "INT")]
    [ScalarFunction("COL_NAME", "VARCHAR")]
    [ScalarFunction("DB_ID", "INT")]
    [ScalarFunction("DB_NAME", "VARCHAR")]
    [ScalarFunction("OBJECT_ID", "INT")]
    [ScalarFunction("OBJECTPROPERTY", "INT")]
    [ScalarFunction("OBJECTPROPERTYEX", "INT")]
    [ScalarFunction("OBJECT_NAME", "VARCHAR")]
    [ScalarFunction("OBJECT_SCHEMA_NAME", "VARCHAR")]
    [ScalarFunction("ORIGINAL_DB_NAME", "VARCHAR")]
    [ScalarFunction("TYPEPROPERTY", "INT")]
    private static bool TryEvalSqlServerDatabaseMetadataGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var evaluator = new AstQuerySqlServerDatabaseFunctionEvaluator(
            resolveDatabaseProperty: context.TryResolveSqlServerDatabaseProperty,
            resolveDatabasePrincipalId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerDatabasePrincipalId,
            resolveColumnProperty: context.TryResolveSqlServerColumnProperty,
            resolveColumnLength: context.TryResolveSqlServerColumnLength,
            resolveColumnName: context.TryResolveSqlServerColumnName,
            resolveObjectId: context.TryResolveSqlServerObjectId,
            resolveObjectProperty: context.TryResolveSqlServerObjectProperty,
            resolveObjectName: context.TryResolveSqlServerObjectName,
            resolveObjectSchemaName: context.TryResolveSqlServerObjectSchemaName,
            resolveTypeProperty: AstQuerySqlServerResolutionHelper.TryResolveSqlServerTypeProperty,
            getDatabaseName: () => context.Connection.Database);

        return evaluator.TryEvaluate(fn, evalArg, out result);
    }

    [ScalarFunction("CURRENT_REQUEST_ID", "INT")]
    [ScalarFunction("CURRENT_TRANSACTION_ID", "BIGINT")]
    [ScalarFunction("IS_MEMBER", "INT")]
    [ScalarFunction("IS_ROLEMEMBER", "INT")]
    [ScalarFunction("IS_SRVROLEMEMBER", "INT")]
    [ScalarFunction("ORIGINAL_LOGIN", "VARCHAR")]
    [ScalarFunction("SESSION_ID", "INT")]
    [ScalarFunction("SERVERPROPERTY", "VARCHAR")]
    [ScalarFunction("XACT_STATE", "INT")]
    private static bool TryEvalSqlServerSessionMetadataGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var evaluator = new AstQuerySqlServerSessionFunctionEvaluator(
            getDialect: () => context.Dialect,
            getContextInfo: context.Connection.GetContextInfo,
            hasActiveTransaction: () => context.Connection.HasActiveTransaction || context.HasActiveTransaction,
            tryResolveSqlServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerRoleMembership,
            tryResolveSqlServerServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerServerRoleMembership);

        return evaluator.TryEvaluate(fn, evalArg, out result);
    }

    [ScalarFunction("SCHEMA_ID", "INT")]
    [ScalarFunction("SCHEMA_NAME", "VARCHAR")]
    [ScalarFunction("SUSER_ID", "INT")]
    [ScalarFunction("SUSER_NAME", "VARCHAR")]
    [ScalarFunction("SUSER_SID", "VARBINARY")]
    [ScalarFunction("SUSER_SNAME", "VARCHAR")]
    [ScalarFunction("TYPE_ID", "INT")]
    [ScalarFunction("TYPE_NAME", "VARCHAR")]
    [ScalarFunction("USER_ID", "INT")]
    [ScalarFunction("USER_NAME", "VARCHAR")]
    private static bool TryEvalSqlServerIdentityMetadataGeneratedFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var evaluator = new AstQuerySqlServerIdentityFunctionEvaluator(
            getDialect: () => context.Dialect,
            getLastInsertId: context.Connection.GetLastInsertId,
            resolveSystemTypeId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeId,
            resolveSystemTypeName: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeName);

        return evaluator.TryEvaluate(fn, evalArg, out result);
    }

    [ScalarFunction("ERROR_LINE", "VARCHAR")]
    [ScalarFunction("ERROR_MESSAGE", "VARCHAR")]
    [ScalarFunction("ERROR_NUMBER", "VARCHAR")]
    [ScalarFunction("ERROR_PROCEDURE", "VARCHAR")]
    [ScalarFunction("ERROR_SEVERITY", "VARCHAR")]
    [ScalarFunction("ERROR_STATE", "VARCHAR")]
    private static bool TryEvalSqlServerErrorFunctionsGenerated(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions(context, fn, evalArg, out result);

    [ScalarFunction("ROWCOUNT", "INT")]
    private static bool TryEvalSqlServerRowCountFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = (int)context.Connection.GetLastFoundRows();
        return true;
    }

    [ScalarFunction("ROWCOUNT_BIG", "BIGINT")]
    private static bool TryEvalSqlServerRowCountBigFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.Connection.GetLastFoundRows();
        return true;
    }

    [ScalarFunction("SCOPE_IDENTITY", "BIGINT")]
    private static bool TryEvalSqlServerScopeIdentityFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.Connection.GetLastInsertId();
        return true;
    }

    private static void RegisterAggregateFunctions(SqlServerDialect dialect, int version)
    {
        if (version >= SqlServerDialect.StringAggMinVersion)
            dialect.AddScalarFunction(
                DbFunctionDef.CreateScalar(SqlConst.STRING_AGG, "VARCHAR") with
                {
                    IsStringAggregate = true
                });

        if (version >= SqlServerDialect.ApproxCountDistinctMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("APPROX_COUNT_DISTINCT", "BIGINT"));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.CHECKSUM_AGG, "INT"),
            SqlConst.CHECKSUM_AGG);
    }

    private static void RegisterFromPartsFunctions(SqlServerDialect dialect, int version)
    {
        if (version < SqlServerDialect.FromPartsMinVersion)
            return;

        dialect.AddScalarFunction(
            "DATEFROMPARTS",
            "DATE",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "DATETIMEFROMPARTS",
            "DATETIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "DATETIME2FROMPARTS",
            "DATETIME2",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "SMALLDATETIMEFROMPARTS",
            "DATETIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "DATETIMEOFFSETFROMPARTS",
            "DATETIMEOFFSET",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "TIMEFROMPARTS",
            "TIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
    }
}
