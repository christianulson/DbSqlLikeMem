using DbSqlLikeMem;
using DbSqlLikeMem.Models;
using System.Text;

namespace DbSqlLikeMem.Npgsql;

internal static partial class NpgsqlScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        RegisterDateFunctions(dialect);
        RegisterNumericFunctions(dialect);
        RegisterAggregateFunctions(dialect);

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.STRING_AGG, "VARCHAR") with
            {
                IsStringAggregate = true
            });
        RegisterGeneratedScalarFunctions(dialect);

        RegisterJsonFunctions(dialect);
    }

    private static void RegisterDateFunctions(
        ISqlDialect dialect)
    {
        _ = dialect;
    }

    private static bool TryEvalPostgresExtractFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return false;
        }

        var unitText = fn.Args[0] is RawSqlExpr rawUnit
            ? rawUnit.Sql
            : evalArg(0)?.ToString() ?? string.Empty;
        var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = unit switch
            {
                AstQueryExecutorBase.TemporalUnit.Day => dateTime.Day,
                AstQueryExecutorBase.TemporalUnit.Month => dateTime.Month,
                AstQueryExecutorBase.TemporalUnit.Year => dateTime.Year,
                AstQueryExecutorBase.TemporalUnit.Hour => dateTime.Hour,
                AstQueryExecutorBase.TemporalUnit.Minute => dateTime.Minute,
                AstQueryExecutorBase.TemporalUnit.Second => dateTime.Second,
                _ => (int?)null
            } is int partValue ? (double)partValue : null;
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(value, out var numeric))
        {
            result = unit == AstQueryExecutorBase.TemporalUnit.Day
                ? (int)Math.Truncate(numeric)
                : null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalPostgresSequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (SqlSequenceEvaluator.TryEvaluateCall(context.Connection, fn.Name, fn.Args, expr => ResolveSequenceArgValue(fn.Args, expr, evalArg), out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalPostgresRowCountFunction(
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

    [ScalarFunction("ROW_COUNT", "BIGINT")]
    private static bool TryEvalGeneratedPostgresRowCountFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalPostgresRowCountFunction(context, fn, evalArg, out result);

    [ScalarFunction("CURRENT_CATALOG", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_DATABASE", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_ROLE", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_SCHEMA", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_SCHEMAS", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_SETTING", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_USER", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("VERSION", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("CURRENT_QUERY", "VARCHAR", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    private static bool TryEvalGeneratedPostgresSystemFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresSystemFunctionEvaluator.TryEvaluatePostgresSystemFunction(context, fn, evalArg, out result);

    [ScalarFunction("NEXTVAL", "BIGINT")]
    [ScalarFunction("CURRVAL", "BIGINT")]
    [ScalarFunction("SETVAL", "BIGINT")]
    [ScalarFunction("LASTVAL", "BIGINT")]
    private static bool TryEvalGeneratedPostgresSequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalPostgresSequenceFunction(context, fn, evalArg, out result);

    private static object? ResolveSequenceArgValue(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        return expr switch
        {
            LiteralExpr lit => lit.Value,
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => string.IsNullOrWhiteSpace(col.Qualifier) ? col.Name : col.Qualifier + "." + col.Name,
            _ => ResolveSequenceArgValueByReference(args, expr, evalArg)
        };
    }

    private static object? ResolveSequenceArgValueByReference(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        for (var i = 0; i < args.Count; i++)
        {
            if (ReferenceEquals(args[i], expr))
                return evalArg(i);
        }

        return null;
    }

    [ScalarFunction("DATE_TRUNC", "DATETIME")]
    [ScalarFunction("DATE_PART", "DOUBLE")]
    [ScalarFunction("AGE", "INTERVAL", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier)]
    [ScalarFunction("MAKE_DATE", "DATE")]
    [ScalarFunction("MAKE_TIME", "TIME")]
    [ScalarFunction("MAKE_TIMESTAMP", "DATETIME")]
    [ScalarFunction("MAKE_TIMESTAMPTZ", "DATETIMEOFFSET")]
    [ScalarFunction("MAKE_INTERVAL", "INTERVAL")]
    [ScalarFunction("TO_DATE", "DATE")]
    [ScalarFunction("TO_CHAR", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresDateFunctionEvaluator.TryEvaluatePostgresDateFunction(context, fn, evalArg, out result);

    [ScalarFunction("TO_NUMBER", "DECIMAL")]
    private static bool TryEvalGeneratedPostgresToNumberFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresNumberFunctionEvaluator.TryEvalToNumberFunction(fn, evalArg, out result);

    [ScalarFunction("EXTRACT", "DOUBLE")]
    private static bool TryEvalGeneratedPostgresExtractFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalPostgresExtractFunction(context, fn, evalArg, out result);

    [ScalarFunction("CAST", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresCastFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryCastConversionFamilyEvaluator.TryEvalCastLikeFunction(context, fn, evalArg, out result);

    [ScalarFunction("CURRENT_DATE", "DATE", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.Date)]
    [ScalarFunction("CURRENT_TIME", "TIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.Time)]
    [ScalarFunction("CURRENT_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("SYSTEMDATE", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("NOW", "DATETIME", InvocationStyle = DbInvocationStyle.Call, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("LOCALTIME", "TIME", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.Time)]
    [ScalarFunction("LOCALTIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Call | DbInvocationStyle.Identifier, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("CLOCK_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Call, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("STATEMENT_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Call, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    [ScalarFunction("TRANSACTION_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Call, TemporalKind = SqlTemporalFunctionKind.DateTime)]
    private static bool TryEvalGeneratedPostgresTemporalFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction(context, fn, evalArg, out result);

    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect);

    private static void RegisterNumericFunctions(
        ISqlDialect dialect)
    {
        _ = dialect;
    }

    private static void RegisterAggregateFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.ARRAY_AGG, "VARCHAR"));
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.BOOL_AND, "BOOLEAN"),
            SqlConst.BOOL_AND,
            SqlConst.BOOL_OR,
            SqlConst.EVERY);
    }

    private static void RegisterJsonFunctions(
        ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_STRICT, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_UNIQUE, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_UNIQUE_STRICT, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_ARRAYAGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_OBJECT_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_OBJECTAGG, "VARCHAR"));
    }

    [ScalarFunction("STRING_TO_ARRAY", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresStringToArrayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("ARRAY_TO_STRING", "VARCHAR")]
    [ScalarFunction("ARRAY_DIMS", "VARCHAR")]
    [ScalarFunction("ARRAY_TO_JSON", "VARCHAR")]
    [ScalarFunction("ARRAY_LENGTH", "INT")]
    [ScalarFunction("ARRAY_LOWER", "INT")]
    [ScalarFunction("ARRAY_UPPER", "INT")]
    [ScalarFunction("ARRAY_POSITION", "INT")]
    [ScalarFunction("ARRAY_NDIMS", "INT")]
    [ScalarFunction("ARRAY_APPEND", "VARCHAR")]
    [ScalarFunction("ARRAY_PREPEND", "VARCHAR")]
    [ScalarFunction("ARRAY_CAT", "VARCHAR")]
    [ScalarFunction("ARRAY_REMOVE", "VARCHAR")]
    [ScalarFunction("ARRAY_REPLACE", "VARCHAR")]
    [ScalarFunction("ARRAY_POSITIONS", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresArrayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("TO_JSON", "VARCHAR")]
    [ScalarFunction("TO_JSONB", "VARCHAR")]
    [ScalarFunction("ROW_TO_JSON", "VARCHAR")]
    [ScalarFunction("JSON_BUILD_ARRAY", "VARCHAR")]
    [ScalarFunction("JSON_BUILD_OBJECT", "VARCHAR")]
    [ScalarFunction("JSONB_BUILD_ARRAY", "VARCHAR")]
    [ScalarFunction("JSONB_BUILD_OBJECT", "VARCHAR")]
    [ScalarFunction("JSONB_TYPEOF", "VARCHAR")]
    [ScalarFunction("JSONB_EXTRACT_PATH", "VARCHAR")]
    [ScalarFunction("JSONB_EXTRACT_PATH_TEXT", "VARCHAR")]
    [ScalarFunction("JSONB_STRIP_NULLS", "VARCHAR")]
    [ScalarFunction("JSONB_SET", "VARCHAR")]
    [ScalarFunction("JSONB_SET_LAX", "VARCHAR")]
    [ScalarFunction("JSONB_INSERT", "VARCHAR")]
    [ScalarFunction("JSONB_PRETTY", "VARCHAR")]
    [ScalarFunction("JSON_TYPEOF", "VARCHAR")]
    [ScalarFunction("JSON_STRIP_NULLS", "VARCHAR")]
    [ScalarFunction("JSON_EXTRACT_PATH", "VARCHAR")]
    [ScalarFunction("JSON_EXTRACT_PATH_TEXT", "VARCHAR")]
    [ScalarFunction("JSON_ARRAY_LENGTH", "INT")]
    [ScalarFunction("JSONB_ARRAY_LENGTH", "INT")]
    private static bool TryEvalGeneratedPostgresJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JSONB_OBJECT", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresJsonbObjectFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvalJsonbObjectFunction(context, fn, evalArg, out result);

    [ScalarFunction("JSON_ARRAY", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresJsonArrayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryJsonArrayFunctionEvaluator.TryEvalJsonArrayFunction(context, fn, evalArg, out result);

    [ScalarFunction("JSON_SCALAR", "VARCHAR", MinVersion = 17)]
    private static bool TryEvalGeneratedPostgresJsonScalarFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JSON_SERIALIZE", "VARCHAR", MinVersion = 17)]
    private static bool TryEvalGeneratedPostgresJsonSerializeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JSONB_PATH_QUERY_ARRAY", "VARCHAR", MinVersion = 12)]
    private static bool TryEvalGeneratedPostgresJsonbPathQueryArrayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JSONB_PATH_EXISTS", "BOOLEAN", MinVersion = 12)]
    private static bool TryEvalGeneratedPostgresJsonbPathExistsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("BTRIM", "VARCHAR")]
    [ScalarFunction("CHR", "VARCHAR")]
    [ScalarFunction("INITCAP", "VARCHAR")]
    [ScalarFunction("SPLIT_PART", "VARCHAR")]
    [ScalarFunction("QUOTE_LITERAL", "VARCHAR")]
    [ScalarFunction("QUOTE_IDENT", "VARCHAR")]
    [ScalarFunction("TO_HEX", "VARCHAR")]
    [ScalarFunction("STARTS_WITH", "BOOLEAN")]
    private static bool TryEvalGeneratedPostgresTextFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("CHAR_LENGTH", "BIGINT")]
    [ScalarFunction("CHARACTER_LENGTH", "BIGINT")]
    private static bool TryEvalGeneratedPostgresSharedTextFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySharedTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("NORMALIZE", "VARCHAR")]
    [ScalarFunction("TO_ASCII", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresUnicodeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("NUM_NULLS", "INT")]
    [ScalarFunction("NUM_NONNULLS", "INT")]
    [ScalarFunction("MIN_SCALE", "INT")]
    [ScalarFunction("LCM", "BIGINT")]
    [ScalarFunction("PARSE_IDENT", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresUtilityFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluatePostgresScalarUtilityFunction(context, fn, evalArg, out result);

    [ScalarFunction("HOST", "VARCHAR")]
    [ScalarFunction("HOSTMASK", "VARCHAR")]
    [ScalarFunction("NETMASK", "VARCHAR")]
    [ScalarFunction("NETWORK", "VARCHAR")]
    [ScalarFunction("INET_SAME_FAMILY", "BOOLEAN")]
    [ScalarFunction("MASKLEN", "INT")]
    private static bool TryEvalGeneratedPostgresNetworkFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("DECODE", "VARBINARY")]
    private static bool TryEvalGeneratedPostgresBinaryFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions(context, fn, evalArg, out result);

    [ScalarFunction("GEN_RANDOM_UUID", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresUuidFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresScalarFunctionEvaluator.TryEvaluateyPostgresScalarFunction(context, fn, evalArg, out result);

    [ScalarFunction("CBRT", "DOUBLE")]
    private static bool TryEvalGeneratedPostgresNumericFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number == 0d
                ? 0d
                : Math.Sign(number) * Math.Pow(Math.Abs(number), 1d / 3d);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    [ScalarFunction("REGEXP_COUNT", "INT")]
    [ScalarFunction("REGEXP_INSTR", "INT")]
    [ScalarFunction("REGEXP_LIKE", "INT")]
    [ScalarFunction("REGEXP_SUBSTR", "INT")]
    [ScalarFunction("REGEXP_MATCH", "VARCHAR")]
    [ScalarFunction("REGEXP_REPLACE", "VARCHAR")]
    [ScalarFunction("REGEXP_SPLIT_TO_ARRAY", "VARCHAR")]
    private static bool TryEvalGeneratedPostgresRegexFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("FORMAT", "VARCHAR")]
    private static bool TryEvalPostgresFormatFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT() espera ao menos o formato.");

        var format = evalArg(0)?.ToString() ?? string.Empty;
        var args = new object?[Math.Max(0, fn.Args.Count - 1)];
        for (var i = 1; i < fn.Args.Count; i++)
            args[i - 1] = evalArg(i);

        result = FormatPostgreSql(format, args);
        return true;
    }

    [ScalarFunction("LOG", "DOUBLE")]
    [ScalarFunction("LOG10", "DOUBLE")]
    private static bool TryEvalPostgresLogFunction(
        QueryExecutionContext context,
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

        if (fn.Args.Count > 1)
        {
            var baseValue = evalArg(0);
            var numberValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(numberValue))
            {
                result = null;
                return true;
            }

            try
            {
                var baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
                var number = Convert.ToDouble(numberValue, CultureInfo.InvariantCulture);
                if (baseNumber <= 0d || baseNumber == 1d || number <= 0d)
                {
                    result = null;
                    return true;
                }

                result = Math.Log(number, baseNumber);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Log10(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static string FormatPostgreSql(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            builder.Append(token switch
            {
                's' => value?.ToString() ?? string.Empty,
                'I' => QuoteFormatIdentifier(value),
                'L' => QuoteFormatLiteral(value),
                _ => value?.ToString() ?? string.Empty
            });
        }

        return builder.ToString();
    }

    private static string QuoteFormatIdentifier(object? value)
    {
        var text = value?.ToString() ?? string.Empty;
        return $"\"{text.Replace("\"", "\"\"")}\"";
    }

    private static string QuoteFormatLiteral(object? value)
    {
        if (value is null || value is DBNull)
            return SqlConst.NULL;

        var text = value.ToString() ?? string.Empty;
        return $"'{text.Replace("'", "''")}'";
    }

    [ScalarFunction("RANDOM", "DOUBLE")]
    private static bool TryEvalPostgresRandomFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = evalArg;
        if (fn.Args.Count != 0)
        {
            result = null;
            return false;
        }

        result = AstQueryRuntimeHelper.NextRandomDouble();
        return true;
    }
}
