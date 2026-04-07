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

            return FormatTextCastValue(context, v!);
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
            var text = Convert.ToString(value, culture) ?? string.Empty;

            if ((context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para PARSE.")).IsIntegerCastTypeName(type))
            {
                if (int.TryParse(text, NumberStyles.Integer, culture, out var parsedInt))
                    return parsedInt;
                return swallowErrors ? DBNull.Value : null;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (decimal.TryParse(text, NumberStyles.Any, culture, out var parsedDecimal))
                    return parsedDecimal;
                return swallowErrors ? DBNull.Value : null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (double.TryParse(text, NumberStyles.Any, culture, out var parsedDouble))
                    return parsedDouble;
                return swallowErrors ? DBNull.Value : null;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase))
            {
                var dateText = Convert.ToString(value, culture);
                if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(dateText, out var parsedDate))
                    return parsedDate;
                return swallowErrors ? DBNull.Value : null;
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }
        catch
        {
            if (swallowErrors)
                return DBNull.Value;
            throw;
        }
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

        var traceGroupedCaseWhen = fn.Args.Count > 0 && ContainsParameter(fn.Args[0], "cutoff");

        if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine(
                $"[CastDebug][entry] name={fn.Name} trace={traceGroupedCaseWhen} arg0={FormatDebugValue(v)} type={type}");
        }

        try
        {
            var invariantText = Convert.ToString(v, CultureInfo.InvariantCulture) ?? string.Empty;

            if (v is byte[] bytes && IsBinaryCastTypeName(type))
                return bytes;

            if (IsTextCastTypeName(type))
                return FormatTextCastValue(context, v!);

            if ((context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
            {
                if (v is long l) return (int)l;
                if (v is int i) return i;
                if (v is decimal d) return (int)d;
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
                if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return (int)dx;
                if (IsSqlServerDialect(context.Dialect))
                    throw new InvalidCastException();
                if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(
                        $"[CastDebug][INT] input={FormatDebugValue(v)} type={type} result=0 (fallback)");
                }
                return 0;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                object? decimalResult;
                if (v is decimal dd)
                {
                    decimalResult = dd;
                }
                else if (AstQueryExecutorBase.TryConvertNumericToDecimal(v, out var numericDecimal))
                {
                    decimalResult = numericDecimal;
                }
                else if (v is IConvertible)
                {
                    try
                    {
                        decimalResult = Convert.ToDecimal(v, CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        // fall back to text parsing below
                        decimalResult = null;
                    }
                }
                else
                {
                    decimalResult = null;
                }

                if (decimalResult is not null)
                {
                    if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine(
                            $"[CastDebug][DECIMAL] input={FormatDebugValue(v)} type={type} result={FormatDebugValue(decimalResult)}");
                    }

                return decimalResult;
            }

                var text = (v!.ToString() ?? invariantText).Trim();
                if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx))
                {
                    if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine(
                            $"[CastDebug][DECIMAL] input={FormatDebugValue(v)} type={type} result={FormatDebugValue(dx)}");
                    }

                return dx;
            }
                if (IsSqlServerDialect(context.Dialect))
                    throw new InvalidCastException();

                if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(
                        $"[CastDebug][DECIMAL] input={FormatDebugValue(v)} type={type} result=0m (fallback)");
                }

                return 0m;
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
                if (IsSqlServerDialect(context.Dialect))
                    throw new InvalidCastException();
                if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(
                        $"[CastDebug][FLOAT] input={FormatDebugValue(v)} type={type} result=0d (fallback)");
                }
                return 0d;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(v, out var dt))
                    return dt;

                if (IsSqlServerDialect(context.Dialect))
                    throw new InvalidCastException();

                if (string.Equals(fn.Name, "CAST", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(fn.Name, "CONVERT", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(
                        $"[CastDebug][DATE] input={FormatDebugValue(v)} type={type} result=NULL (fallback)");
                }
                return null;
            }

            if (type.Equals("JSON", StringComparison.OrdinalIgnoreCase))
            {
                static string? ValidateJsonOrNull(string? json)
                {
                    if (json is null || string.IsNullOrWhiteSpace(json))
                        return null;

                    var normalizedJson = json.Trim();

                    QueryJsonFunctionHelper.TryGetJsonRootElement(normalizedJson, out _);
                    return normalizedJson;
                }

                if (v is string s)
                    return ValidateJsonOrNull(s);

                if (v is JsonElement je)
                    return ValidateJsonOrNull(je.GetRawText());

                var serialized = JsonSerializer.Serialize(v);
                return ValidateJsonOrNull(serialized);
            }

            return FormatTextCastValue(context, v!);
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

    private static bool IsSqlServerDialect(ISqlDialect? dialect)
        => dialect is not null && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase);

    private static bool IsFirebirdDialect(ISqlDialect? dialect)
        => dialect is not null && string.Equals(dialect.Name, "firebird", StringComparison.OrdinalIgnoreCase);

    private static string FormatTextCastValue(QueryExecutionContext context, object value)
    {
        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
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

    private static string FormatDebugValue(object? value)
        => value is null or DBNull
            ? "NULL"
            : $"{value} ({value.GetType().Name})";
}
