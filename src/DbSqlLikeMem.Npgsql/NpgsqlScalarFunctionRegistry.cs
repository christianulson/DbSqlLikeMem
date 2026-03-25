using System.Globalization;
using System.Text;
using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Npgsql;

internal static class NpgsqlScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        var body = SqlFunctionBodyFactory.Identity();

        bool TryEvalPostgresSystemFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresSystemFunctionEvaluator.TryEvaluate(
                fn,
                context,
                evalArg,
                () => null,
                out result);

        bool TryEvalPostgresDateFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresDateFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresTextFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresTextFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresUnicodeFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresUtilityFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresNetworkFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresArrayFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresJsonFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresRegexFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalPostgresUuidFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresUuidFunctionEvaluator.TryEvaluate(fn, context, out result);

        bool TryEvalGeneralScalarFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        static bool TryEvalNoopSessionContextFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = context;
            _ = evalArg;
            result = null;
            return false;
        }

        static bool TryEvalNoopGeneralJsonFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = context;
            _ = evalArg;
            result = null;
            return false;
        }

        var generalSystemAndJsonFunctionEvaluator = new AstQueryGeneralSystemAndJsonFunctionEvaluator(
            TryEvalNoopSessionContextFunction,
            TryEvalNoopGeneralJsonFunction,
            TryEvalNoopGeneralJsonFunction,
            TryEvalNoopGeneralJsonFunction);

        bool TryEvalGeneralSystemAndJsonFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => generalSystemAndJsonFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        RegisterSystemFunctions(dialect, body, TryEvalPostgresSystemFunction);
        RegisterDateFunctions(dialect, body, TryEvalPostgresDateFunction);
        RegisterNumericFunctions(dialect, body, TryEvalGeneralScalarFunction);
        RegisterTextFunctions(dialect, body, TryEvalPostgresTextFunction, TryEvalPostgresUnicodeFunction, TryEvalGeneralScalarFunction);
        RegisterUtilityFunctions(dialect, body, TryEvalPostgresUtilityFunction);
        RegisterNetworkFunctions(dialect, body, TryEvalPostgresNetworkFunction);
        RegisterBinaryFunctions(dialect, body);
        RegisterMiscFunctions(dialect, body);
        RegisterAggregateFunctions(dialect, body);
        RegisterPostgresCompatibilityFunctions(dialect, body);

        dialect.AddScalarFunction("STRING_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("ROW_COUNT", "BIGINT", body);
        dialect.AddScalarFunctions(
            "BIGINT",
            body,
            SqlScalarFunctionUsageKind.Call,
            null,
            NpgsqlConst.NEXTVAL,
            NpgsqlConst.CURRVAL,
            NpgsqlConst.SETVAL,
            NpgsqlConst.LASTVAL);

        RegisterArrayFunctions(dialect, body, TryEvalPostgresArrayFunction);
        RegisterJsonFunctions(dialect, body, TryEvalPostgresJsonFunction, TryEvalGeneralSystemAndJsonFunction);
        RegisterRegexFunctions(dialect, body, TryEvalPostgresRegexFunction);
        RegisterUuidFunctions(dialect, body, TryEvalPostgresUuidFunction);
        RegisterGeneralFunctions(dialect, body, TryEvalGeneralScalarFunction);
    }

    private static void RegisterSystemFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresSystemFunction)
    {
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("LOCALTIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.CallOrIdentifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("LOCALTIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.CallOrIdentifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunctions(
            "VARCHAR",
            tryEvalPostgresSystemFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "CURRENT_CATALOG",
            "CURRENT_DATABASE",
            "CURRENT_ROLE",
            "CURRENT_SCHEMA",
            "CURRENT_SCHEMAS",
            "CURRENT_SETTING",
            "CURRENT_USER",
            "VERSION");

        dialect.AddScalarFunction("CURRENT_QUERY", "VARCHAR", body, SqlScalarFunctionUsageKind.CallOrIdentifier, null);

        dialect.AddScalarFunctions(
            "DATETIME",
            tryEvalPostgresSystemFunction,
            SqlScalarFunctionUsageKind.Call,
            null,
            "CLOCK_TIMESTAMP",
            "STATEMENT_TIMESTAMP",
            "TRANSACTION_TIMESTAMP");
    }

    private static void RegisterDateFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresDateFunction)
    {
        bool TryEvalPostgresToNumberFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvalToNumberFunction(fn, evalArg, out result);

        dialect.AddScalarFunction("DATE_TRUNC", "DATETIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("DATE_PART", "DOUBLE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("EXTRACT", "DOUBLE", body);
        dialect.AddScalarFunction("AGE", "INTERVAL", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_DATE", "DATE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIME", "TIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIMESTAMP", "DATETIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIMESTAMPTZ", "DATETIMEOFFSET", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_INTERVAL", "INTERVAL", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_DATE", "DATE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_CHAR", "VARCHAR", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_NUMBER", "DECIMAL", TryEvalPostgresToNumberFunction);
    }

    private static void RegisterTextFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresTextFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUnicodeFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralScalarFunction)
    {
        var generalScalarFunction = tryEvalGeneralScalarFunction;
        var postgresqlTextFunction = tryEvalPostgresTextFunction;
        var postgresqlUnicodeFunction = tryEvalPostgresUnicodeFunction;

        dialect.AddScalarFunctions(
            "VARCHAR",
            postgresqlTextFunction,
            "BTRIM",
            "INITCAP",
            "SPLIT_PART",
            "QUOTE_LITERAL",
            "QUOTE_IDENT",
            "TO_HEX",
            "TRANSLATE");
        dialect.AddScalarFunctionIf(dialect.Version >= 11, "STARTS_WITH", "BOOLEAN", postgresqlTextFunction);
        dialect.AddScalarFunctions("BIGINT", generalScalarFunction, "CHAR_LENGTH", "CHARACTER_LENGTH");
        dialect.AddScalarFunctions(
            "VARCHAR",
            generalScalarFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "GREATEST",
            "LEAST");
        dialect.AddScalarFunctions("VARCHAR", generalScalarFunction, "SUBSTR", "RPAD");
        dialect.AddScalarFunctions("INT", generalScalarFunction, "OCTET_LENGTH", "POSITION", "BIT_LENGTH");
        dialect.AddScalarFunctions("VARCHAR", postgresqlUnicodeFunction, "NORMALIZE", "TO_ASCII");
    }

    private static void RegisterUtilityFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUtilityFunction)
    {
        var utilityFunction = tryEvalPostgresUtilityFunction;
        dialect.AddScalarFunctions("INT", utilityFunction, "NUM_NULLS", "NUM_NONNULLS", "MIN_SCALE");
        dialect.AddScalarFunction("LCM", "BIGINT", utilityFunction);
        dialect.AddScalarFunction("PARSE_IDENT", "VARCHAR", utilityFunction);
    }

    private static void RegisterNumericFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralScalarFunction)
    {
        var generalScalarFunction = tryEvalGeneralScalarFunction;
        dialect.AddScalarFunction("CBRT", "DOUBLE", generalScalarFunction);
    }

    private static void RegisterPostgresCompatibilityFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body)
    {
        _ = body;

        dialect.AddScalarFunction("LOG", "DOUBLE", TryEvalPostgresLogFunction);
        dialect.AddScalarFunction("FORMAT", "VARCHAR", TryEvalPostgresFormatFunction);
        dialect.AddScalarFunction("RANDOM", "DOUBLE", TryEvalPostgresRandomFunction);
    }

    private static void RegisterNetworkFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresNetworkFunction)
    {
        dialect.AddScalarFunctions("VARCHAR", tryEvalPostgresNetworkFunction, "HOST", "HOSTMASK", "NETMASK", "NETWORK");
        dialect.AddScalarFunction("INET_SAME_FAMILY", "BOOLEAN", tryEvalPostgresNetworkFunction);
        dialect.AddScalarFunction("MASKLEN", "INT", tryEvalPostgresNetworkFunction);
    }

    private static void RegisterBinaryFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction(
            "DECODE",
            "VARBINARY",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
    }

    private static void RegisterMiscFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        _ = body;
    }

    private static void RegisterAggregateFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("ARRAY_AGG", "VARCHAR", body);
        dialect.AddScalarFunctions("BOOLEAN", body, "BOOL_AND", "BOOL_OR", "EVERY");
    }

    private static void RegisterArrayFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresArrayFunction)
    {
        dialect.AddScalarFunction("STRING_TO_ARRAY", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_TO_STRING", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_DIMS", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_TO_JSON", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_LENGTH", "INT", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_LOWER", "INT", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_UPPER", "INT", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_POSITION", "INT", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_NDIMS", "INT", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_APPEND", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_PREPEND", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_CAT", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_REMOVE", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_REPLACE", "VARCHAR", tryEvalPostgresArrayFunction);
        dialect.AddScalarFunction("ARRAY_POSITIONS", "VARCHAR", tryEvalPostgresArrayFunction);
    }

    private static void RegisterJsonFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresJsonFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralSystemAndJsonFunction)
    {
        var jsonFunction = tryEvalPostgresJsonFunction;
        var generalSystemAndJsonFunction = tryEvalGeneralSystemAndJsonFunction;

        dialect.AddScalarFunction("TO_JSON", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("TO_JSONB", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("ROW_TO_JSON", "VARCHAR", jsonFunction);
        dialect.AddScalarFunctionIf(dialect.Version >= 17, "JSON_SCALAR", "VARCHAR", jsonFunction);
        dialect.AddScalarFunctionIf(dialect.Version >= 17, "JSON_SERIALIZE", "VARCHAR", jsonFunction);
        dialect.AddScalarFunctionIf(dialect.Version >= 12, "JSONB_PATH_QUERY_ARRAY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_ARRAY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_OBJECT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_OBJECT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_TYPEOF", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_EXTRACT_PATH", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_EXTRACT_PATH_TEXT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_STRIP_NULLS", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_SET", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_SET_LAX", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_INSERT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_PRETTY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_TYPEOF", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_STRIP_NULLS", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_EXTRACT_PATH", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_EXTRACT_PATH_TEXT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_ARRAY_LENGTH", "INT", jsonFunction);
        dialect.AddScalarFunction("JSONB_ARRAY_LENGTH", "INT", jsonFunction);
        dialect.AddScalarFunctionIf(dialect.Version >= 12, "JSONB_PATH_EXISTS", "BOOLEAN", jsonFunction);
        dialect.AddScalarFunction("JSON_ARRAY", "VARCHAR", generalSystemAndJsonFunction);

        dialect.AddScalarFunction("JSONB_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSONB_OBJECT_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSONB_OBJECT_AGG_STRICT", "VARCHAR", body);
        dialect.AddScalarFunction("JSONB_OBJECT_AGG_UNIQUE", "VARCHAR", body);
        dialect.AddScalarFunction("JSONB_OBJECT_AGG_UNIQUE_STRICT", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_ARRAYAGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_OBJECT_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_OBJECTAGG", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_OBJECT", "VARCHAR", body);
    }

    private static void RegisterRegexFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresRegexFunction)
    {
        dialect.AddScalarFunctionsIf(dialect.Version >= 15, "INT", tryEvalPostgresRegexFunction, "REGEXP_COUNT", "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_SUBSTR");
        dialect.AddScalarFunctionIf(dialect.Version >= 10, "REGEXP_MATCH", "VARCHAR", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunctionsIf(dialect.Version >= 9, "VARCHAR", tryEvalPostgresRegexFunction, "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY");
    }

    private static void RegisterUuidFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUuidFunction)
    {
        dialect.AddScalarFunction("GEN_RANDOM_UUID", "VARCHAR", tryEvalPostgresUuidFunction);
    }

    private static void RegisterGeneralFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralScalarFunction)
    {
        dialect.AddScalarFunctions("BIGINT", tryEvalGeneralScalarFunction, "CHAR_LENGTH", "CHARACTER_LENGTH");
        dialect.AddScalarFunctions(
            "VARCHAR",
            tryEvalGeneralScalarFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "GREATEST",
            "LEAST");
        dialect.AddScalarFunction("SUBSTR", "VARCHAR", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("RPAD", "VARCHAR", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("OCTET_LENGTH", "INT", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("POSITION", "INT", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("BIT_LENGTH", "INT", tryEvalGeneralScalarFunction);
    }

    private static bool TryEvalPostgresLogFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (!fn.Name.Equals("LOG", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

    private static bool TryEvalPostgresFormatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (!fn.Name.Equals("FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT() espera ao menos o formato.");

        var format = evalArg(0)?.ToString() ?? string.Empty;
        var args = new object?[Math.Max(0, fn.Args.Count - 1)];
        for (var i = 1; i < fn.Args.Count; i++)
            args[i - 1] = evalArg(i);

        result = FormatPostgreSql(format, args);
        return true;
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

    private static bool TryEvalPostgresRandomFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = evalArg;
        if (!fn.Name.Equals("RANDOM", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 0)
        {
            result = null;
            return false;
        }

        result = AstQueryExecutorBase.NextRandomDouble();
        return true;
    }
}
