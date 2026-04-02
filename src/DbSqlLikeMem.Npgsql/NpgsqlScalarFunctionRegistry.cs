using System.Globalization;
using DbSqlLikeMem.Models;
using System.Text;

namespace DbSqlLikeMem.Npgsql;

internal static class NpgsqlScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        RegisterSystemFunctions(dialect, AstQueryPostgresSystemFunctionEvaluator.TryEvaluatePostgresSystemFunction);
        RegisterDateFunctions(dialect, AstQueryPostgresDateFunctionEvaluator.TryEvaluatePostgresDateFunction);
        RegisterNumericFunctions(dialect);
        RegisterTextFunctions(dialect,
            AstQueryPostgresTextFunctionEvaluator.TryEvaluate,
            AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate,
            AstQuerySharedTextFunctionEvaluator.TryEvaluate,
            AstQuerySharedNumericFunctionEvaluator.TryEvaluate);
        RegisterUtilityFunctions(dialect, AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluatePostgresScalarUtilityFunction);
        RegisterNetworkFunctions(dialect, AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate);
        RegisterBinaryFunctions(dialect);
        RegisterMiscFunctions(dialect);
        RegisterAggregateFunctions(dialect);
        RegisterPostgresCompatibilityFunctions(dialect);

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.STRING_AGG, "VARCHAR") with
            {
                IsStringAggregate = true
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ROW_COUNT", "BIGINT", invocationStyle: DbInvocationStyle.Call) with
            {
                AstExecutor = TryEvalPostgresRowCountFunction
            });
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(NpgsqlConst.NEXTVAL, "BIGINT", invocationStyle: DbInvocationStyle.Call) with
            {
                AstExecutor = TryEvalPostgresSequenceFunction
            },
            NpgsqlConst.NEXTVAL,
            NpgsqlConst.CURRVAL,
            NpgsqlConst.SETVAL,
            NpgsqlConst.LASTVAL);

        RegisterArrayFunctions(dialect, AstQueryPostgresArrayFunctionEvaluator.TryEvaluate);
        RegisterJsonFunctions(dialect,
            AstQueryPostgresJsonFunctionEvaluator.TryEvaluate,
            AstQueryJsonArrayFunctionEvaluator.TryEvalJsonArrayFunction);
        RegisterRegexFunctions(dialect, AstQueryPostgresRegexFunctionEvaluator.TryEvaluate);
        RegisterUuidFunctions(dialect, AstQueryPostgresScalarFunctionEvaluator.TryEvaluateyPostgresScalarFunction);
    }

    private static void RegisterSystemFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresSystemFunction)
    {
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("LOCALTIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call | DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("LOCALTIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call | DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunctions(
            "VARCHAR",
            tryEvalPostgresSystemFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            "CURRENT_CATALOG",
            "CURRENT_DATABASE",
            "CURRENT_ROLE",
            "CURRENT_SCHEMA",
            "CURRENT_SCHEMAS",
            "CURRENT_SETTING",
            "CURRENT_USER",
            "VERSION");

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("CURRENT_QUERY", "VARCHAR", invocationStyle: DbInvocationStyle.Call | DbInvocationStyle.Identifier) with
            {
                AstExecutor = tryEvalPostgresSystemFunction
            });

        dialect.AddScalarFunctions(
            "DATETIME",
            tryEvalPostgresSystemFunction,
            DbInvocationStyle.Call,
            "CLOCK_TIMESTAMP",
            "STATEMENT_TIMESTAMP",
            "TRANSACTION_TIMESTAMP");
    }

    private static void RegisterDateFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresDateFunction)
    {
        bool TryEvalPostgresToNumberFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresNumberFunctionEvaluator.TryEvalToNumberFunction(fn, evalArg, out result);

        dialect.AddScalarFunction("DATE_TRUNC", "DATETIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("DATE_PART", "DOUBLE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("EXTRACT", "DOUBLE") with
            {
                AstExecutor = TryEvalPostgresExtractFunction
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("AGE", "INTERVAL", invocationStyle: DbInvocationStyle.Call | DbInvocationStyle.Identifier) with
            {
                AstExecutor = tryEvalPostgresDateFunction
            });
        dialect.AddScalarFunction("MAKE_DATE", "DATE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIME", "TIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIMESTAMP", "DATETIME", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_TIMESTAMPTZ", "DATETIMEOFFSET", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("MAKE_INTERVAL", "INTERVAL", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_DATE", "DATE", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_CHAR", "VARCHAR", tryEvalPostgresDateFunction);
        dialect.AddScalarFunction("TO_NUMBER", "DECIMAL", TryEvalPostgresToNumberFunction);
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

    private static void RegisterTextFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresTextFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUnicodeFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalSharedTextFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalSharedNumericFunction)
    {
        var postgresqlTextFunction = tryEvalPostgresTextFunction;
        var postgresqlUnicodeFunction = tryEvalPostgresUnicodeFunction;

        dialect.AddScalarFunctions(
            "VARCHAR",
            postgresqlTextFunction,
            "BTRIM",
            "CHR",
            "INITCAP",
            "SPLIT_PART",
            "QUOTE_LITERAL",
            "QUOTE_IDENT",
            "TO_HEX");
        if (dialect.Version >= 11)
            dialect.AddScalarFunction("STARTS_WITH", "BOOLEAN", postgresqlTextFunction);
        dialect.AddScalarFunctions("BIGINT", tryEvalSharedTextFunction, "CHAR_LENGTH", "CHARACTER_LENGTH");
        dialect.AddScalarFunctions("VARCHAR", postgresqlUnicodeFunction, "NORMALIZE", "TO_ASCII");
    }

    private static void RegisterUtilityFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUtilityFunction)
    {
        var utilityFunction = tryEvalPostgresUtilityFunction;
        dialect.AddScalarFunctions("INT", utilityFunction, "NUM_NULLS", "NUM_NONNULLS", "MIN_SCALE");
        dialect.AddScalarFunction("LCM", "BIGINT", utilityFunction);
        dialect.AddScalarFunction("PARSE_IDENT", "VARCHAR", utilityFunction);
    }

    private static void RegisterNumericFunctions(
        ISqlDialect dialect)
    {
        static bool TryEvalPostgresNumericFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (!string.Equals(fn.Name, "CBRT", StringComparison.OrdinalIgnoreCase))
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

        dialect.AddScalarFunction("CBRT", "DOUBLE", TryEvalPostgresNumericFunction);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("CAST", "VARCHAR") with
            {
                AstExecutor = AstQueryCastConversionFamilyEvaluator.TryEvalCastLikeFunction
            });
    }

    private static void RegisterPostgresCompatibilityFunctions(
        ISqlDialect dialect)
    {
        dialect.AddScalarFunction("FORMAT", "VARCHAR", TryEvalPostgresFormatFunction);
        dialect.AddScalarFunction("LOG", "DOUBLE", TryEvalPostgresLogFunction);
        dialect.AddScalarFunction("LOG10", "DOUBLE", TryEvalPostgresLogFunction);
        dialect.AddScalarFunction("RANDOM", "DOUBLE", TryEvalPostgresRandomFunction);
    }

    private static void RegisterNetworkFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresNetworkFunction)
    {
        dialect.AddScalarFunctions("VARCHAR", tryEvalPostgresNetworkFunction, "HOST", "HOSTMASK", "NETMASK", "NETWORK");
        dialect.AddScalarFunction("INET_SAME_FAMILY", "BOOLEAN", tryEvalPostgresNetworkFunction);
        dialect.AddScalarFunction("MASKLEN", "INT", tryEvalPostgresNetworkFunction);
    }

    private static void RegisterBinaryFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DECODE", "VARBINARY") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            });
    }

    private static void RegisterMiscFunctions(ISqlDialect dialect)
    {
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

    private static void RegisterArrayFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresArrayFunction)
    {
        dialect.AddScalarFunction("STRING_TO_ARRAY", "VARCHAR", AstQueryPostgresTextFunctionEvaluator.TryEvaluate);
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
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresJsonFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalJsonArrayFunction)
    {
        var jsonFunction = tryEvalPostgresJsonFunction;
        var jsonArrayFunction = tryEvalJsonArrayFunction;

        dialect.AddScalarFunction("TO_JSON", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("TO_JSONB", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("ROW_TO_JSON", "VARCHAR", jsonFunction);
        if (dialect.Version >= 17)
        {
            dialect.AddScalarFunction("JSON_SCALAR", "VARCHAR", jsonFunction);
            dialect.AddScalarFunction("JSON_SERIALIZE", "VARCHAR", jsonFunction);
        }

        if (dialect.Version >= 12)
            dialect.AddScalarFunction("JSONB_PATH_QUERY_ARRAY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_BUILD_ARRAY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSON_BUILD_OBJECT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_ARRAY", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_OBJECT", "VARCHAR", jsonFunction);
        dialect.AddScalarFunction("JSONB_OBJECT", "VARCHAR", AstQueryPostgresJsonFunctionEvaluator.TryEvalJsonbObjectFunction);
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
        if (dialect.Version >= 12)
            dialect.AddScalarFunction("JSONB_PATH_EXISTS", "BOOLEAN", jsonFunction);
        dialect.AddScalarFunction("JSON_ARRAY", "VARCHAR", jsonArrayFunction);

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_STRICT, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_UNIQUE, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSONB_OBJECT_AGG_UNIQUE_STRICT, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_ARRAYAGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_OBJECT_AGG, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_OBJECTAGG, "VARCHAR"));
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("JSON_OBJECT", "VARCHAR") with
            {
                AstExecutor = AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction
            });
    }

    private static void RegisterRegexFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresRegexFunction)
    {
        if (dialect.Version >= 15)
            dialect.AddScalarFunctions("INT", tryEvalPostgresRegexFunction, "REGEXP_COUNT", "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_SUBSTR");
        if (dialect.Version >= 10)
            dialect.AddScalarFunction("REGEXP_MATCH", "VARCHAR", tryEvalPostgresRegexFunction);
        if (dialect.Version >= 9)
            dialect.AddScalarFunctions("VARCHAR", tryEvalPostgresRegexFunction, "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY");
    }

    private static void RegisterUuidFunctions(
        ISqlDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUuidFunction)
    {
        dialect.AddScalarFunction("GEN_RANDOM_UUID", "VARCHAR", tryEvalPostgresUuidFunction);
    }

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
