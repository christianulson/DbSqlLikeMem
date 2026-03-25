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
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresSystemFunctionEvaluator.TryEvaluate(
                fn,
                currentDialect,
                evalArg,
                () => null,
                out result);

        bool TryEvalPostgresDateFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresDateFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresTextFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresTextFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresUnicodeFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresUtilityFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresNetworkFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresArrayFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresJsonFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresRegexFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalPostgresUuidFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryPostgresUuidFunctionEvaluator.TryEvaluate(fn, currentDialect, out result);

        bool TryEvalGeneralScalarFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        //bool TryEvalGeneralSystemAndJsonFunction(
        //    FunctionCallExpr fn,
        //    ISqlDialect currentDialect,
        //    Func<int, object?> evalArg,
        //    out object? result)
        //    => AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        RegisterSystemFunctions(dialect, body, TryEvalPostgresSystemFunction);
        RegisterDateFunctions(dialect, body, TryEvalPostgresDateFunction);
        RegisterNumericFunctions(dialect, body, TryEvalGeneralScalarFunction);
        RegisterTextFunctions(dialect, body, TryEvalPostgresTextFunction, TryEvalPostgresUnicodeFunction);
        RegisterUtilityFunctions(dialect, body, TryEvalPostgresUtilityFunction);
        RegisterNetworkFunctions(dialect, body, TryEvalPostgresNetworkFunction);
        RegisterBinaryFunctions(dialect, body);
        RegisterMiscFunctions(dialect, body);
        RegisterAggregateFunctions(dialect, body);

        dialect.AddScalarFunction("STRING_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("ROW_COUNT", "BIGINT", body);
        dialect.AddScalarFunctions(
            "BIGINT",
            body,
            SqlScalarFunctionUsageKind.Call,
            null,
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);

        RegisterArrayFunctions(dialect, body, TryEvalPostgresArrayFunction);
        RegisterJsonFunctions(dialect, body, TryEvalPostgresJsonFunction);
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
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryExecutorBase.TryEvalToNumberFunction(fn, evalArg, out result);

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
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUnicodeFunction)
    {
        dialect.AddScalarFunctions(
            "VARCHAR",
            tryEvalPostgresTextFunction,
            "BTRIM",
            "INITCAP",
            "SPLIT_PART",
            "QUOTE_LITERAL",
            "QUOTE_IDENT",
            "TO_HEX",
            "TRANSLATE");
        dialect.AddScalarFunction("STARTS_WITH", "BOOLEAN", tryEvalPostgresTextFunction);
        dialect.AddScalarFunctions("BIGINT", tryEvalGeneralScalarFunction, "CHAR_LENGTH", "CHARACTER_LENGTH");
        dialect.AddScalarFunctions(
            "VARCHAR",
            tryEvalGeneralScalarFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "GREATEST",
            "LEAST");
        dialect.AddScalarFunctions("VARCHAR", tryEvalGeneralScalarFunction, "SUBSTR", "RPAD");
        dialect.AddScalarFunctions("INT", tryEvalGeneralScalarFunction, "OCTET_LENGTH", "POSITION", "BIT_LENGTH");
        dialect.AddScalarFunctions("VARCHAR", tryEvalPostgresUnicodeFunction, "NORMALIZE", "TO_ASCII");
    }

    private static void RegisterUtilityFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresUtilityFunction)
    {
        dialect.AddScalarFunctions("INT", tryEvalPostgresUtilityFunction, "NUM_NULLS", "NUM_NONNULLS", "MIN_SCALE");
        dialect.AddScalarFunction("LCM", "BIGINT", tryEvalPostgresUtilityFunction);
        dialect.AddScalarFunction("PARSE_IDENT", "VARCHAR", tryEvalPostgresUtilityFunction);
    }

    private static void RegisterNumericFunctions(
        ISqlDialect dialect,
        Func<SqlExpr, object> body,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralScalarFunction)
    {
        dialect.AddScalarFunction("CBRT", "DOUBLE", tryEvalGeneralScalarFunction);
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
        AstQueryGeneralScalarFunctionHandler tryEvalPostgresJsonFunction)
    {
        dialect.AddScalarFunction("TO_JSON", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("TO_JSONB", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("ROW_TO_JSON", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_SCALAR", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_SERIALIZE", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_PATH_QUERY_ARRAY", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_ARRAY", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_BUILD_OBJECT", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_OBJECT", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_TYPEOF", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_EXTRACT_PATH", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_EXTRACT_PATH_TEXT", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_STRIP_NULLS", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_SET", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_SET_LAX", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_INSERT", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_PRETTY", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_TYPEOF", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_STRIP_NULLS", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_EXTRACT_PATH", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_EXTRACT_PATH_TEXT", "VARCHAR", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_ARRAY_LENGTH", "INT", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_ARRAY_LENGTH", "INT", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSONB_PATH_EXISTS", "BOOLEAN", tryEvalPostgresJsonFunction);
        dialect.AddScalarFunction("JSON_ARRAY", "VARCHAR", tryEvalGeneralSystemAndJsonFunction);

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
        dialect.AddScalarFunction("REGEXP_COUNT", "INT", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_INSTR", "INT", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_LIKE", "BOOLEAN", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_MATCH", "VARCHAR", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_REPLACE", "VARCHAR", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_SPLIT_TO_ARRAY", "VARCHAR", tryEvalPostgresRegexFunction);
        dialect.AddScalarFunction("REGEXP_SUBSTR", "VARCHAR", tryEvalPostgresRegexFunction);
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
}
