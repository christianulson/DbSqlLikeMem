namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2SpecialFunctionEvaluator
{
    private static readonly HashSet<string> OracleApproxFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "APPROX_COUNT_DISTINCT",
        "APPROX_COUNT_DISTINCT_AGG",
        "APPROX_COUNT_DISTINCT_DETAIL",
        "APPROX_MEDIAN",
        "APPROX_PERCENTILE",
        "APPROX_PERCENTILE_AGG",
        "APPROX_PERCENTILE_DETAIL",
        "TO_APPROX_COUNT_DISTINCT",
        "TO_APPROX_PERCENTILE",
    };

    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalOracleApproxFunctions,
            "APPROX_COUNT_DISTINCT",
            "APPROX_COUNT_DISTINCT_AGG",
            "APPROX_COUNT_DISTINCT_DETAIL",
            "APPROX_MEDIAN",
            "APPROX_PERCENTILE",
            "APPROX_PERCENTILE_AGG",
            "APPROX_PERCENTILE_DETAIL",
            "TO_APPROX_COUNT_DISTINCT",
            "TO_APPROX_PERCENTILE");

        Register(handlers, TryEvalOracleAnalyticsFunctions,
            "FEATURE_COMPARE",
            "FEATURE_DETAILS",
            "FEATURE_ID",
            "FEATURE_SET",
            "FEATURE_VALUE",
            "NCGR",
            "POWERMULTISET",
            "POWERMULTISET_BY_CARDINALITY",
            "PREDICTION",
            "PREDICTION_BOUNDS",
            "PREDICTION_COST",
            "PREDICTION_DETAILS",
            "PREDICTION_PROBABILITY",
            "PREDICTION_SET",
            "PRESENTNNV",
            "PRESENTV",
            "RATIO_TO_REPORT");

        Register(handlers, TryEvalOracleScnFunctions, "SCN_TO_TIMESTAMP", "TIMESTAMP_TO_SCN");
        Register(handlers, TryEvalOracleTimeZoneOffsetFunction, "TZ_OFFSET");
        Register(handlers, TryEvalOracleSessionTimeZoneFunction, "SESSIONTIMEZONE");
        Register(handlers, TryEvalOracleUserEnvFunction, "USERENV");
        Register(handlers, TryEvalOracleInvokingUserFunction, "ORA_INVOKING_USER");
        Register(handlers, TryEvalOracleInvokingUserIdFunction, "ORA_INVOKING_USERID");
        Register(handlers, TryEvalOracleDstNoopFunction, "ORA_DST_AFFECTED", "ORA_DST_CONVERT", "ORA_DST_ERROR", "ORA_DM_PARTITION_NAME");
        Register(handlers, TryEvalOracleValidateConversionFunction, "VALIDATE_CONVERSION");
        Register(handlers, TryEvalOracleMonthsBetweenFunction, "MONTHS_BETWEEN");
        Register(handlers, TryEvalOracleIterationNumberFunction, "ITERATION_NUMBER");
        Register(handlers, TryEvalOracleLnnvlFunction, "LNNVL");
        Register(handlers, TryEvalOracleNanvlFunction, "NANVL");
        Register(handlers, TryEvalOracleDepthFunction, "DEPTH");
        Register(handlers, TryEvalOracleDerefFunction, "DEREF");
        Register(handlers, TryEvalOracleDumpFunction, "DUMP");
        Register(handlers, TryEvalOracleExistsNodeFunction, "EXISTSNODE");
        Register(handlers, TryEvalOracleJsonDataGuideFunction, "JSON_DATAGUIDE");
        Register(handlers, TryEvalOracleMakeRefFunction, "MAKE_REF");
        Register(handlers, TryEvalOracleVsizeFunction, "VSIZE");
        Register(handlers, TryEvalOracleWidthBucketFunction, "WIDTH_BUCKET");
        Register(handlers, TryEvalOracleXmlFunctions,
            "EXTRACTVALUE",
            "XMLCAST",
            "XMLCDATA",
            "XMLCOLATTVAL",
            "XMLCOMMENT",
            "XMLCONCAT",
            "XMLDIFF",
            "XMLELEMENT",
            "XMLFOREST",
            "XMLISVALID",
            "XMLPARSE",
            "XMLPATCH",
            "XMLPI",
            "XMLQUERY",
            "XMLROOT",
            "XMLSEQUENCE",
            "XMLSERIALIZE",
            "XMLTABLE",
            "XMLTRANSFORM");
        Register(handlers, TryEvalOracleUserFunction, "USER");

        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, dialect, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalOracleApproxFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!OracleApproxFunctionNames.Contains(fn.Name))
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

    private static bool TryEvalOracleAnalyticsFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("FEATURE_COMPARE" or "FEATURE_DETAILS" or "FEATURE_ID" or "FEATURE_SET" or "FEATURE_VALUE"
            or "NCGR" or "POWERMULTISET" or "POWERMULTISET_BY_CARDINALITY" or "PREDICTION" or "PREDICTION_BOUNDS"
            or "PREDICTION_COST" or "PREDICTION_DETAILS" or "PREDICTION_PROBABILITY" or "PREDICTION_SET"
            or "PRESENTNNV" or "PRESENTV" or "RATIO_TO_REPORT"))
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

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        result = null;
        return true;
    }

    private static bool TryEvalOracleScnFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SCN_TO_TIMESTAMP" or "TIMESTAMP_TO_SCN"))
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

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleTimeZoneOffsetFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TZ_OFFSET", StringComparison.OrdinalIgnoreCase))
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

        if (fn.Args.Count == 0)
        {
            var offset = DateTimeOffset.Now.Offset;
            result = $"{(offset < TimeSpan.Zero ? "-" : "+")}{offset:hh\\:mm}";
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (AstQueryExecutorBase.TryParseOffset(value!.ToString() ?? string.Empty, out var parsed))
        {
            result = $"{(parsed < TimeSpan.Zero ? "-" : "+")}{parsed:hh\\:mm}";
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleSessionTimeZoneFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!fn.Name.Equals("SESSIONTIMEZONE", StringComparison.OrdinalIgnoreCase))
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

        var offset = DateTimeOffset.Now.Offset;
        result = $"{(offset < TimeSpan.Zero ? "-" : "+")}{offset:hh\\:mm}";
        return true;
    }

    private static bool TryEvalOracleUserEnvFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, fn.Name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var param = evalArg(0)?.ToString();
        if (string.Equals(param, "CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase)
            || string.Equals(param, "SESSION_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = "SYS";
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleInvokingUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, fn.Name);
        result = "SYS";
        return true;
    }

    private static bool TryEvalOracleInvokingUserIdFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, fn.Name);
        result = 0;
        return true;
    }

    private static bool TryEvalOracleDstNoopFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, fn.Name);
        result = null;
        return true;
    }

    private static bool TryEvalOracleValidateConversionFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "VALIDATE_CONVERSION")
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

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        var type = evalArg(1)?.ToString() ?? string.Empty;
        if (AstQueryExecutorBase.IsNullish(value) || string.IsNullOrWhiteSpace(type))
        {
            result = 0;
            return true;
        }

        var normalized = type.Trim().ToUpperInvariant();
        var isValid = normalized switch
        {
            "NUMBER" => AstQueryExecutorBase.TryCoerceDecimal(value, out _),
            "DATE" or "TIMESTAMP" => AstQueryExecutorBase.TryCoerceDateTime(value, out _),
            _ => true
        };

        result = isValid ? 1 : 0;
        return true;
    }

    private static bool TryEvalOracleMonthsBetweenFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleIterationNumberFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
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

    private static bool TryEvalOracleLnnvlFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleNanvlFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleDepthFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleDerefFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleDumpFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleExistsNodeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleJsonDataGuideFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleMakeRefFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalOracleVsizeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("VSIZE", StringComparison.OrdinalIgnoreCase))
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

        var text = value?.ToString() ?? string.Empty;
        result = Encoding.UTF8.GetByteCount(text);
        return true;
    }

    private static bool TryEvalOracleWidthBucketFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("WIDTH_BUCKET", StringComparison.OrdinalIgnoreCase))
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

        if (fn.Args.Count < 4)
        {
            result = null;
            return true;
        }

        var expr = evalArg(0);
        var min = evalArg(1);
        var max = evalArg(2);
        var count = evalArg(3);
        if (AstQueryExecutorBase.IsNullish(expr) || AstQueryExecutorBase.IsNullish(min) || AstQueryExecutorBase.IsNullish(max) || AstQueryExecutorBase.IsNullish(count))
        {
            result = null;
            return true;
        }

        var exprValue = Convert.ToDouble(expr, CultureInfo.InvariantCulture);
        var minValue = Convert.ToDouble(min, CultureInfo.InvariantCulture);
        var maxValue = Convert.ToDouble(max, CultureInfo.InvariantCulture);
        var bucketCount = Convert.ToInt32(count.ToDec());
        if (bucketCount <= 0 || maxValue <= minValue)
        {
            result = null;
            return true;
        }

        if (exprValue < minValue)
        {
            result = 0;
            return true;
        }

        if (exprValue >= maxValue)
        {
            result = bucketCount + 1;
            return true;
        }

        var width = (maxValue - minValue) / bucketCount;
        var bucket = (int)Math.Floor((exprValue - minValue) / width) + 1;
        result = bucket;
        return true;
    }

    private static bool TryEvalOracleXmlFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("EXTRACTVALUE" or "XMLCAST" or "XMLCDATA" or "XMLCOLATTVAL" or "XMLCOMMENT" or "XMLCONCAT"
            or "XMLDIFF" or "XMLELEMENT" or "XMLEXISTS" or "XMLFOREST" or "XMLISVALID" or "XMLPARSE" or "XMLPATCH"
            or "XMLPI" or "XMLQUERY" or "XMLROOT" or "XMLSEQUENCE" or "XMLSERIALIZE" or "XMLTABLE" or "XMLTRANSFORM"))
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

    private static bool TryEvalOracleUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!fn.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
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

        result = "SYS";
        return true;
    }
}
