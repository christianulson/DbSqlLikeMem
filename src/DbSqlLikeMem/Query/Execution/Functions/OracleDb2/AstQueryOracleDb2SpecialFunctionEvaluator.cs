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

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalOracleApproxFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleAnalyticsFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleScnFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleTimeZoneOffsetFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleSessionTimeZoneFunction(fn, dialect, out result)
            || TryEvalOracleUserEnvFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleValidateConversionFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleVsizeFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleWidthBucketFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleXmlFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleUserFunction(fn, dialect, out result);
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
        out object? result)
    {
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

    private static bool TryEvalOracleUserEnvFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("USERENV" or "ORA_INVOKING_USER" or "ORA_INVOKING_USERID" or "ORA_DST_AFFECTED" or "ORA_DST_CONVERT" or "ORA_DST_ERROR" or "ORA_DM_PARTITION_NAME"))
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

        switch (name)
        {
            case "ORA_INVOKING_USER":
                result = "SYS";
                return true;
            case "ORA_INVOKING_USERID":
                result = 0;
                return true;
            case "USERENV":
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
            default:
                result = null;
                return true;
        }
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
        out object? result)
    {
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
