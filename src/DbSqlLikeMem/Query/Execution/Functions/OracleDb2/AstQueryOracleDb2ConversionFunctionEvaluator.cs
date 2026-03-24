namespace DbSqlLikeMem;

using System;
using System.Globalization;

internal static class AstQueryOracleDb2ConversionFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("TO_BINARY_DOUBLE" or "TO_BINARY_FLOAT" or "TO_BLOB" or "TO_CHAR" or "TO_CLOB" or "TO_DATE"
            or "TO_DSINTERVAL" or "TO_LOB" or "TO_MULTI_BYTE" or "TO_NCHAR" or "TO_NCLOB" or "TO_NUMBER"
            or "TO_SINGLE_BYTE" or "TO_TIMESTAMP" or "TO_TIMESTAMP_TZ" or "TO_YMINTERVAL"))
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

        if ((name.Equals("TO_BINARY_DOUBLE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_BINARY_FLOAT", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_BLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_CLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_DSINTERVAL", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_LOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_MULTI_BYTE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_NCHAR", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_NCLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_SINGLE_BYTE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_TIMESTAMP_TZ", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_YMINTERVAL", StringComparison.OrdinalIgnoreCase)))
        {
            QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);
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

        switch (name)
        {
            case "TO_BINARY_DOUBLE":
                result = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_BINARY_FLOAT":
                result = Convert.ToSingle(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_NUMBER":
                if (value is string numberText)
                {
                    var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                    if (AstQueryFormatFunctionHelper.TryParseOracleNumber(numberText, mask, out var parsedNumber))
                    {
                        result = parsedNumber;
                        return true;
                    }
                }

                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_CHAR":
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
                        result = AstQueryFormatFunctionHelper.FormatOracleNumber(value!, mask!);
                        return true;
                    }
                }

                result = value!.ToString();
                return true;
            case "TO_DATE":
            case "TO_TIMESTAMP":
            case "TO_TIMESTAMP_TZ":
                if (value is DateTime dt)
                {
                    result = dt;
                    return true;
                }

                var textValue = value?.ToString() ?? string.Empty;
                var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                if (name == "TO_TIMESTAMP_TZ")
                {
                    if (AstQueryFormatFunctionHelper.TryParseOracleDateTimeOffset(textValue, maskValue, out var parsedOffset))
                    {
                        result = parsedOffset;
                        return true;
                    }

                    result = null;
                    return true;
                }

                if (AstQueryFormatFunctionHelper.TryParseOracleDateTime(textValue, maskValue, out var parsed))
                {
                    result = parsed;
                    return true;
                }

                result = null;
                return true;
            case "TO_DSINTERVAL":
            case "TO_YMINTERVAL":
                if (AstQueryExecutorBase.TryCoerceTimeSpan(value, out var parsedSpan))
                {
                    result = parsedSpan;
                    return true;
                }

                result = null;
                return true;
            case "TO_BLOB":
            case "TO_CLOB":
            case "TO_NCLOB":
            case "TO_LOB":
            case "TO_MULTI_BYTE":
            case "TO_SINGLE_BYTE":
            case "TO_NCHAR":
                result = value?.ToString();
                return true;
            default:
                result = value;
                return true;
        }
    }
}
