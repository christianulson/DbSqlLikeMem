namespace DbSqlLikeMem;

internal delegate object? AstQueryEvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryEvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors);

internal delegate object? AstQueryEvalCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate bool AstQueryTryEvalJsonAccessShimFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalJsonExtractionFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalSqlServerJsonModifyFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalOpenJsonFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalJsonUnquoteFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalToNumberFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQueryCastConversionFamilyEvaluator(
    AstQueryTryEvalJsonAccessShimFunction tryEvalJsonAccessShimFunction,
    AstQueryTryEvalJsonExtractionFunction tryEvalJsonExtractionFunction,
    AstQueryTryEvalSqlServerJsonModifyFunction tryEvalSqlServerJsonModifyFunction,
    AstQueryTryEvalOpenJsonFunction tryEvalOpenJsonFunction,
    AstQueryTryEvalJsonUnquoteFunction tryEvalJsonUnquoteFunction,
    AstQueryTryEvalToNumberFunction tryEvalToNumberFunction)
{
    private readonly AstQueryTryEvalJsonAccessShimFunction _tryEvalJsonAccessShimFunction =
        tryEvalJsonAccessShimFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonAccessShimFunction));

    private readonly AstQueryTryEvalJsonExtractionFunction _tryEvalJsonExtractionFunction =
        tryEvalJsonExtractionFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonExtractionFunction));

    private readonly AstQueryTryEvalSqlServerJsonModifyFunction _tryEvalSqlServerJsonModifyFunction =
        tryEvalSqlServerJsonModifyFunction ?? throw new ArgumentNullException(nameof(tryEvalSqlServerJsonModifyFunction));

    private readonly AstQueryTryEvalOpenJsonFunction _tryEvalOpenJsonFunction =
        tryEvalOpenJsonFunction ?? throw new ArgumentNullException(nameof(tryEvalOpenJsonFunction));

    private readonly AstQueryTryEvalJsonUnquoteFunction _tryEvalJsonUnquoteFunction =
        tryEvalJsonUnquoteFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonUnquoteFunction));

    private readonly AstQueryTryEvalToNumberFunction _tryEvalToNumberFunction =
        tryEvalToNumberFunction ?? throw new ArgumentNullException(nameof(tryEvalToNumberFunction));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvaluateCore(fn, context, evalArg, out result);

    private bool TryEvaluateCore(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_tryEvalJsonAccessShimFunction(fn, evalArg, out result))
            return true;

        if (_tryEvalJsonExtractionFunction(context, fn, evalArg, out result))
            return true;

        if (_tryEvalSqlServerJsonModifyFunction(context, fn, evalArg, out result))
            return true;

        if (_tryEvalOpenJsonFunction(context, fn, evalArg, out result))
            return true;

        if (_tryEvalJsonUnquoteFunction(context, fn, evalArg, out result))
            return true;

        if (_tryEvalToNumberFunction(fn, evalArg, out result))
            return true;

        if (TryEvalTryCastFunction(context, fn, evalArg, out result))
            return true;

        if (TryEvalTryConvertFunction(context, fn, evalArg, out result))
            return true;

        if (TryEvalParseFunction(context, fn, evalArg, out result))
            return true;

        if (TryEvalTryParseFunction(context, fn, evalArg, out result))
            return true;

        if (TryEvalCastFunction(context, fn, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalTryCastFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.SupportsTryCastFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "TRY_CAST");

        result = EvalTryCast(context, fn, evalArg);
        return true;
    }

    internal static bool TryEvalTryCastLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalTryCastFunction(context, fn, evalArg, out result);

    private static bool TryEvalTryConvertFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.SupportsTryConvertFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "TRY_CONVERT");

        result = EvalTryCast(context, fn, evalArg);
        return true;
    }

    internal static bool TryEvalTryConvertLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalTryConvertFunction(context, fn, evalArg, out result);

    internal static bool TryEvalParseLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalParseFunction(context, fn, evalArg, out result);

    internal static bool TryEvalTryParseLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalTryParseFunction(context, fn, evalArg, out result);

    public static bool TryEvalCastLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => new AstQueryCastConversionFamilyEvaluator(
            AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonAccessShimFunction,
            AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction,
            AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerJsonModifyFunction,
            TryEvalOpenJsonFunction,
            AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction,
            AstQueryToNumberFunctionEvaluator.TryEvalToNumberFunction)
            .TryEvaluate(fn, context, evalArg, out result);

    private static bool TryEvalOpenJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;
        result = null;
        return false;
    }

    private static bool TryEvalParseFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "PARSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.SupportsParseFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "PARSE");

        result = EvalParseFunction(context, fn, evalArg, swallowErrors: false);
        return true;
    }

    private static bool TryEvalTryParseFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TRY_PARSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.SupportsTryParseFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "TRY_PARSE");

        result = EvalParseFunction(context, fn, evalArg, swallowErrors: true);
        return true;
    }

    private static bool TryEvalCastFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        _ = context;
        result = EvalCast(context, fn, evalArg);
        return true;
    }

    private static object? EvalTryCast(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2)
            return DBNull.Value;

        var v = evalArg(0);
        var type = GetTargetTypeName(fn.Args[1], evalArg);
        if (AstQueryExecutorBase.IsNullish(v))
            return DBNull.Value;

        try
        {
            var invariantText = Convert.ToString(v, CultureInfo.InvariantCulture) ?? string.Empty;

            if ((context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
            {
                if (v is long l) return (int)l;
                if (v is int i) return i;
                if (v is decimal d) return (int)d;
                if (AstQueryExecutorBase.TryConvertNumericToInt64(v!, out var numericLong)) return (int)numericLong;
                if (AstQueryExecutorBase.TryConvertNumericToDecimal(v, out var numericDecimal)) return (int)numericDecimal;
                if (v is IConvertible)
                {
                    try
                    {
                        return Convert.ToInt32(v, CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        // fall back to text parsing below
                    }
                }
                var text = (v!.ToString() ?? invariantText).Trim();
                if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ix)) return ix;
                if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lx)) return (int)lx;
                return DBNull.Value;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (v is decimal dd) return dd;
                if (AstQueryExecutorBase.TryConvertNumericToDecimal(v, out var numericDecimal)) return numericDecimal;
                if (v is IConvertible)
                {
                    try
                    {
                        return Convert.ToDecimal(v, CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        // fall back to text parsing below
                    }
                }
                var text = (v!.ToString() ?? invariantText).Trim();
                if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return dx;
                return null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (v is double dfx) return dfx;
                if (v is float ffx) return (double)ffx;
                if (v is decimal ddx) return (double)ddx;
                if (AstQueryExecutorBase.TryConvertNumericToDouble(v, out var numericDouble)) return numericDouble;
                var text = (v!.ToString() ?? invariantText).Trim();
                if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var fx)) return fx;
                return DBNull.Value;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                return AstQueryExecutionRuntimeHelper.TryCoerceDateTime(v, out var dt) ? dt : DBNull.Value;
            }

            return FormatTextCastValue(context, v!, type, fn.Args[0]);
        }
        catch
        {
            return null;
        }
    }

    private static object? EvalParseFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool swallowErrors)
    {
        if (fn.Args.Count < 2)
            return swallowErrors ? null : throw new InvalidOperationException($"{fn.Name}() requires value and target type.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
            return swallowErrors ? DBNull.Value : null;

        var type = GetTargetTypeName(fn.Args[1], evalArg);
        var cultureName = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;

        try
        {
            var culture = string.IsNullOrWhiteSpace(cultureName)
                ? CultureInfo.InvariantCulture
                : CultureInfo.GetCultureInfo(cultureName!);
            if (TryEvalParseIntegerResult(context, type, value!, culture, swallowErrors, out var integerResult))
                return integerResult;

            if (TryEvalParseDecimalResult(type, value!, culture, swallowErrors, out var decimalResult))
                return decimalResult;

            if (TryEvalParseFloatingResult(type, value!, culture, swallowErrors, out var floatingResult))
                return floatingResult;

            if (TryEvalParseDateResult(type, value!, culture, swallowErrors, out var dateResult))
                return dateResult;

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }
        catch
        {
            if (swallowErrors)
                return DBNull.Value;
            throw;
        }
    }

    private static bool TryEvalParseIntegerResult(
        QueryExecutionContext context,
        string type,
        object value,
        CultureInfo culture,
        bool swallowErrors,
        out object? result)
    {
        result = null;
        if (!context.Dialect.IsIntegerCastTypeName(type))
            return false;

        var text = Convert.ToString(value, culture) ?? string.Empty;
        if (int.TryParse(text, NumberStyles.Integer, culture, out var parsedInt))
        {
            result = parsedInt;
            return true;
        }

        result = swallowErrors ? DBNull.Value : null;
        return true;
    }

    private static bool TryEvalParseDecimalResult(
        string type,
        object value,
        CultureInfo culture,
        bool swallowErrors,
        out object? result)
    {
        result = null;
        if (!IsDecimalCastTypeName(type))
            return false;

        var text = Convert.ToString(value, culture) ?? string.Empty;
        if (decimal.TryParse(text, NumberStyles.Any, culture, out var parsedDecimal))
        {
            result = parsedDecimal;
            return true;
        }

        result = swallowErrors ? DBNull.Value : null;
        return true;
    }

    private static bool TryEvalParseFloatingResult(
        string type,
        object value,
        CultureInfo culture,
        bool swallowErrors,
        out object? result)
    {
        result = null;
        if (!IsFloatingCastTypeName(type))
            return false;

        var text = Convert.ToString(value, culture) ?? string.Empty;
        if (double.TryParse(text, NumberStyles.Any, culture, out var parsedDouble))
        {
            result = parsedDouble;
            return true;
        }

        result = swallowErrors ? DBNull.Value : null;
        return true;
    }

    private static bool TryEvalParseDateResult(
        string type,
        object value,
        CultureInfo culture,
        bool swallowErrors,
        out object? result)
    {
        result = null;
        if (!IsDateCastTypeName(type))
            return false;

        var dateText = Convert.ToString(value, culture);
        if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(dateText, out var parsedDate))
        {
            result = parsedDate;
            return true;
        }

        result = swallowErrors ? DBNull.Value : null;
        return true;
    }

    private static object? EvalCast(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2)
            return null;

        var v = evalArg(0);
        var type = GetTargetTypeName(fn.Args[1], evalArg);
        if (AstQueryExecutorBase.IsNullish(v))
            return null;

        try
        {
            if (v is byte[] bytes && IsBinaryCastTypeName(type))
                return bytes;

            if (IsTextCastTypeName(type))
                return FormatTextCastValue(context, v!, type, fn.Args[0]);

            if (TryEvalCastIntegerResult(context, fn, v!, type, out var integerResult))
                return integerResult;

            if (TryEvalCastDecimalResult(context, fn, v!, type, out var decimalResult))
                return decimalResult;

            if (TryEvalCastFloatingResult(context, fn, v!, type, out var floatingResult))
                return floatingResult;

            if (TryEvalCastDateResult(context, fn, v!, type, out var dateResult))
                return dateResult;

            if (TryEvalCastJsonResult(v!, type, out var jsonResult))
                return jsonResult;

            return FormatTextCastValue(context, v!, type, fn.Args[0]);
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            if (e is InvalidCastException && IsSqlServerDialect(context.Dialect))
                throw;

            AstQueryExecutionRuntimeHelper.LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    private static bool TryEvalCastIntegerResult(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        object value,
        string type,
        out object? result)
    {
        result = null;
        if (!context.Dialect.IsIntegerCastTypeName(type))
            return false;

        var invariantText = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (value is long l) { result = (int)l; return true; }
        if (value is int i) { result = i; return true; }
        if (value is decimal d) { result = (int)d; return true; }
        if (value is IConvertible)
        {
            try
            {
                result = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                // fall back to text parsing below
            }
        }

        var text = (value.ToString() ?? invariantText).Trim();
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ix))
        {
            result = ix;
            return true;
        }

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lx))
        {
            result = (int)lx;
            return true;
        }

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx))
        {
            result = (int)dx;
            return true;
        }

        if (IsSqlServerDialect(context.Dialect))
            throw new InvalidCastException();

        result = 0;
        return true;
    }

    private static bool TryEvalCastDecimalResult(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        object value,
        string type,
        out object? result)
    {
        result = null;
        if (!IsDecimalCastTypeName(type))
            return false;

        var invariantText = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (value is decimal dd)
            result = dd;
        else if (AstQueryExecutorBase.TryConvertNumericToDecimal(value, out var numericDecimal))
            result = numericDecimal;
        else if (value is IConvertible)
        {
            try
            {
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }
            catch
            {
                // fall back to text parsing below
            }
        }

        if (result is not null)
        {
            return true;
        }

        var text = (value.ToString() ?? invariantText).Trim();
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx))
        {
            result = dx;
            return true;
        }

        if (IsSqlServerDialect(context.Dialect))
            throw new InvalidCastException();

        result = 0m;
        return true;
    }

    private static bool TryEvalCastFloatingResult(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        object value,
        string type,
        out object? result)
    {
        result = null;
        if (!IsFloatingCastTypeName(type))
            return false;

        var invariantText = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (value is double dfx) { result = dfx; return true; }
        if (value is float ffx) { result = (double)ffx; return true; }
        if (value is decimal ddx) { result = (double)ddx; return true; }
        if (AstQueryExecutorBase.TryConvertNumericToDouble(value, out var numericDouble)) { result = numericDouble; return true; }

        var text = (value.ToString() ?? invariantText).Trim();
        if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var fx))
        {
            result = fx;
            return true;
        }

        if (IsSqlServerDialect(context.Dialect))
            throw new InvalidCastException();

        result = 0d;
        return true;
    }

    private static bool TryEvalCastDateResult(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        object value,
        string type,
        out object? result)
    {
        result = null;
        if (!IsDateCastTypeName(type))
            return false;

        if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(value, out var dt))
        {
            result = dt;
            return true;
        }

        if (IsSqlServerDialect(context.Dialect))
            throw new InvalidCastException();

        result = null;
        return true;
    }

    private static bool TryEvalCastJsonResult(
        object value,
        string type,
        out object? result)
    {
        result = null;
        if (!string.Equals(type, "JSON", StringComparison.OrdinalIgnoreCase))
            return false;

        static string? ValidateJsonOrNull(string? json)
        {
            if (json is null || string.IsNullOrWhiteSpace(json))
                return null;

            var normalizedJson = json.Trim();
            QueryJsonFunctionHelper.TryGetJsonRootElement(normalizedJson, out _);
            return normalizedJson;
        }

        if (value is string s)
        {
            result = ValidateJsonOrNull(s);
            return true;
        }

        if (value is JsonElement je)
        {
            result = ValidateJsonOrNull(je.GetRawText());
            return true;
        }

        result = ValidateJsonOrNull(JsonSerializer.Serialize(value));
        return true;
    }

    private static bool IsSqlServerDialect(ISqlDialect? dialect)
        => dialect is not null && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase);

    private static bool IsFirebirdDialect(ISqlDialect? dialect)
        => dialect is not null && string.Equals(dialect.Name, "firebird", StringComparison.OrdinalIgnoreCase);

    private static bool IsDb2Dialect(QueryExecutionContext context)
        => string.Equals(context.Connection.ProviderExecutionDialect.Name, "db2", StringComparison.OrdinalIgnoreCase);

    private static bool IsNpgsqlDialect(QueryExecutionContext context)
        => string.Equals(context.Connection.ProviderExecutionDialect.Name, "npgsql", StringComparison.OrdinalIgnoreCase);

    private static bool IsDecimalCastTypeName(string typeName)
        => typeName.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase);

    private static bool IsFloatingCastTypeName(string typeName)
        => typeName.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase);

    private static bool IsDateCastTypeName(string typeName)
        => typeName.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
           || typeName.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase);

    private static string FormatTextCastValue(QueryExecutionContext context, object value, string typeName, SqlExpr? sourceExpression = null)
    {
        if (string.Equals(context.Dialect.Name, "oracle", StringComparison.OrdinalIgnoreCase) && IsNumericValue(value))
            return AstQueryFormatFunctionHelper.FormatOracleNumber(value);

        if (value is decimal decimalValue)
        {
            var scale = TryGetDecimalScaleFromExpression(sourceExpression) ?? GetDecimalScale(decimalValue);
            return decimalValue.ToString($"F{scale}", CultureInfo.InvariantCulture);
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (IsDb2Dialect(context)
            && TryGetFixedLengthTextCastWidth(typeName, out var fixedWidth)
            && text.Length < fixedWidth)
        {
            return text.PadRight(fixedWidth);
        }

        if (IsNpgsqlDialect(context)
            && TryGetFixedLengthTextCastWidth(typeName, out fixedWidth)
            && text.Length < fixedWidth)
        {
            return text.PadRight(fixedWidth);
        }

        if (!IsFirebirdDialect(context.Connection.ProviderExecutionDialect))
            return text;

        if (!IsNumericValue(value))
            return text;

        if (!AstQueryBinaryArithmeticHelper.TryConvertNumericToDecimal(value, out var numeric))
            return text;

        if (numeric != decimal.Truncate(numeric))
            return text;

        return decimal.Truncate(numeric).ToString(CultureInfo.InvariantCulture);
    }

    private static int? TryGetDecimalScaleFromExpression(SqlExpr? expression)
    {
        if (expression is not CallExpr and not FunctionCallExpr)
            return null;

        if (expression is CallExpr callExpr)
            return TryGetDecimalScaleFromCastCall(callExpr.Name, callExpr.Args);

        var fn = (FunctionCallExpr)expression;
        return TryGetDecimalScaleFromCastCall(fn.Name, fn.Args);
    }

    private static bool TryGetFixedLengthTextCastWidth(string typeName, out int width)
    {
        width = 0;

        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        if (typeName.IndexOf("VARYING", StringComparison.OrdinalIgnoreCase) >= 0
            || typeName.IndexOf("FOR BIT DATA", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return false;
        }

        if (!typeName.StartsWith("CHAR", StringComparison.OrdinalIgnoreCase)
            && !typeName.StartsWith("NCHAR", StringComparison.OrdinalIgnoreCase)
            && !typeName.StartsWith("CHARACTER", StringComparison.OrdinalIgnoreCase)
            && !typeName.StartsWith("NCHARACTER", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var openParen = typeName.IndexOf('(');
        if (openParen < 0)
            return false;

        var closeParen = typeName.IndexOf(')', openParen + 1);
        if (closeParen < 0)
            return false;

        var widthText = typeName[(openParen + 1)..closeParen].Trim();
        return int.TryParse(widthText, NumberStyles.Integer, CultureInfo.InvariantCulture, out width)
            && width > 0;
    }

    private static int? TryGetDecimalScaleFromCastCall(string name, IReadOnlyList<SqlExpr> args)
    {
        if ((!string.Equals(name, "CAST", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(name, "CONVERT", StringComparison.OrdinalIgnoreCase))
            || args.Count < 2)
        {
            return null;
        }

        var typeSql = GetTargetTypeName(args[1], _ => null);
        return TryParseDecimalScale(typeSql, out var scale) ? scale : null;
    }

    private static bool TryParseDecimalScale(string typeSql, out int scale)
    {
        scale = 0;
        if (string.IsNullOrWhiteSpace(typeSql))
            return false;

        var match = Regex.Match(
            typeSql,
            @"^(?:DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*(\d+)\s*\)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        if (!match.Success)
            return false;

        if (!int.TryParse(match.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale))
            return false;

        return true;
    }

    private static int GetDecimalScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }

    private static bool IsNumericValue(object value)
        => value is byte
            or sbyte
            or short
            or ushort
            or int
            or uint
            or long
            or ulong
            or float
            or double
            or decimal;

    private static bool IsTextCastTypeName(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        if (typeName.IndexOf("FOR BIT DATA", StringComparison.OrdinalIgnoreCase) >= 0)
            return false;

        return typeName.StartsWith("CHAR", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("VARCHAR", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("NCHAR", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("TEXT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("CLOB", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("LONGTEXT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("MEDIUMTEXT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("TINYTEXT", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsBinaryCastTypeName(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        return typeName.IndexOf("FOR BIT DATA", StringComparison.OrdinalIgnoreCase) >= 0
            || typeName.StartsWith("BINARY", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("VARBINARY", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("BLOB", StringComparison.OrdinalIgnoreCase);
    }

    private static string GetTargetTypeName(SqlExpr expr, Func<int, object?> evalArg)
        => expr switch
        {
            RawSqlExpr raw => raw.Sql.Trim(),
            IdentifierExpr identifier => identifier.Name.Trim(),
            ColumnExpr column => $"{column.Qualifier}.{column.Name}".Trim(),
            FunctionCallExpr call => SqlExprPrinter.Print(call).Trim(),
            CallExpr call => SqlExprPrinter.Print(call).Trim(),
            _ => (evalArg(1)?.ToString() ?? string.Empty).Trim()
        };

    private static bool ContainsParameter(SqlExpr expression, string parameterName)
        => expression switch
        {
            ParameterExpr parameter => parameter.Name.TrimStart('@', ':', '?')
                .Equals(parameterName, StringComparison.OrdinalIgnoreCase),
            BinaryExpr binary => ContainsParameter(binary.Left, parameterName) || ContainsParameter(binary.Right, parameterName),
            UnaryExpr unary => ContainsParameter(unary.Expr, parameterName),
            CaseExpr caseExpr => (caseExpr.BaseExpr is not null && ContainsParameter(caseExpr.BaseExpr, parameterName))
                || caseExpr.Whens.Any(when => ContainsParameter(when.When, parameterName) || ContainsParameter(when.Then, parameterName))
                || (caseExpr.ElseExpr is not null && ContainsParameter(caseExpr.ElseExpr, parameterName)),
            FunctionCallExpr functionCall => functionCall.Args.Any(arg => ContainsParameter(arg, parameterName)),
            CallExpr call => call.Args.Any(arg => ContainsParameter(arg, parameterName)),
            LikeExpr likeExpr => ContainsParameter(likeExpr.Left, parameterName)
                || ContainsParameter(likeExpr.Pattern, parameterName)
                || (likeExpr.Escape is not null && ContainsParameter(likeExpr.Escape, parameterName)),
            InExpr inExpr => ContainsParameter(inExpr.Left, parameterName)
                || inExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            IsNullExpr isNullExpr => ContainsParameter(isNullExpr.Expr, parameterName),
            BetweenExpr betweenExpr => ContainsParameter(betweenExpr.Expr, parameterName)
                || ContainsParameter(betweenExpr.Low, parameterName)
                || ContainsParameter(betweenExpr.High, parameterName),
            RowExpr rowExpr => rowExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            _ => false
        };
}
