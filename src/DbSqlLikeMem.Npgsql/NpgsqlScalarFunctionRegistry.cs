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

        RegisterSystemFunctions(dialect, body);
        RegisterDateFunctions(dialect, body);
        RegisterNumericFunctions(dialect, body);
        RegisterTextFunctions(dialect, body);
        RegisterNetworkFunctions(dialect, body);
        RegisterBinaryFunctions(dialect, body);
        RegisterMiscFunctions(dialect, body);
        RegisterAggregateFunctions(dialect, body);

        dialect.AddScalarFunction("STRING_AGG", "VARCHAR", body);
        dialect.AddScalarFunction("ROW_COUNT", "BIGINT", body);
        dialect.AddScalarFunctions("BIGINT", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);

        RegisterArrayFunctions(dialect, body);
        RegisterJsonFunctions(dialect, body);
        RegisterRegexFunctions(dialect, body);
    }

    private static void RegisterSystemFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("LOCALTIME", "TIME", body, SqlScalarFunctionUsageKind.CallOrIdentifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("LOCALTIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.CallOrIdentifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunctions("VARCHAR", body,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "CURRENT_CATALOG",
            "CURRENT_DATABASE",
            "CURRENT_QUERY",
            "CURRENT_ROLE",
            "CURRENT_SCHEMA",
            "CURRENT_SCHEMAS",
            "CURRENT_SETTING",
            "CURRENT_USER",
            "VERSION");
        dialect.AddScalarFunctions("DATETIME", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            "CLOCK_TIMESTAMP",
            "STATEMENT_TIMESTAMP",
            "TRANSACTION_TIMESTAMP");
    }

    private static void RegisterDateFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("DATE_TRUNC", "DATETIME", body);
        dialect.AddScalarFunction("DATE_PART", "DOUBLE", body);
        dialect.AddScalarFunction("AGE", "INTERVAL", body);
        dialect.AddScalarFunction("EXTRACT", "DOUBLE", body);
        dialect.AddScalarFunction("MAKE_DATE", "DATE", body);
        dialect.AddScalarFunction("MAKE_TIME", "TIME", body);
        dialect.AddScalarFunction("MAKE_TIMESTAMP", "DATETIME", body);
        dialect.AddScalarFunction("MAKE_TIMESTAMPTZ", "DATETIMEOFFSET", body);
        dialect.AddScalarFunction("MAKE_INTERVAL", "INTERVAL", body);
        dialect.AddScalarFunction("TO_DATE", "DATE", body);
        dialect.AddScalarFunction("TO_CHAR", "VARCHAR", body);
        dialect.AddScalarFunction("TO_NUMBER", "DECIMAL", body);
    }

    private static void RegisterTextFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "BTRIM",
            "INITCAP",
            "PARSE_IDENT",
            "NORMALIZE",
            "SPLIT_PART",
            "SUBSTR",
            "RPAD");

        dialect.AddScalarFunctions("BIGINT", body,
            "CHAR_LENGTH",
            "CHARACTER_LENGTH");

        dialect.AddScalarFunction("BIT_LENGTH", "INT", body);

        dialect.AddScalarFunctions("VARCHAR", body,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "GREATEST",
            "LEAST");

        dialect.AddScalarFunction("TO_HEX", "VARCHAR", body);
        dialect.AddScalarFunction("TO_ASCII", "VARCHAR", body);
        dialect.AddScalarFunction("OCTET_LENGTH", "INT", body);
        dialect.AddScalarFunction("POSITION", "INT", body);
        dialect.AddScalarFunction("STARTS_WITH", "BOOLEAN", body);
        dialect.AddScalarFunction("NUM_NULLS", "INT", body);
        dialect.AddScalarFunction("NUM_NONNULLS", "INT", body);
    }

    private static void RegisterNumericFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("DOUBLE", body,
            "CBRT");

        dialect.AddScalarFunctions("BIGINT", body,
            "LCM");

        dialect.AddScalarFunctions("INT", body,
            "MIN_SCALE");
    }

    private static void RegisterNetworkFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "HOST",
            "HOSTMASK",
            "NETMASK",
            "NETWORK");

        dialect.AddScalarFunction("INET_SAME_FAMILY", "BOOLEAN", body);

        dialect.AddScalarFunction("MASKLEN", "INT", body);
    }

    private static void RegisterBinaryFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("DECODE", "VARBINARY", body);
    }

    private static void RegisterMiscFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("GEN_RANDOM_UUID", "VARCHAR", body);
        dialect.AddScalarFunction("QUOTE_IDENT", "VARCHAR", body);
        dialect.AddScalarFunction("QUOTE_LITERAL", "VARCHAR", body);
    }

    private static void RegisterAggregateFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("ARRAY_AGG", "VARCHAR", body);
        dialect.AddScalarFunctions("BOOLEAN", body,
            "BOOL_AND",
            "BOOL_OR",
            "EVERY");
    }

    private static void RegisterArrayFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "STRING_TO_ARRAY",
            "ARRAY_TO_STRING",
            "ARRAY_DIMS",
            "ARRAY_TO_JSON");

        dialect.AddScalarFunctions("INT", body,
            "ARRAY_LENGTH",
            "ARRAY_LOWER",
            "ARRAY_UPPER",
            "ARRAY_POSITION",
            "ARRAY_NDIMS");

        dialect.AddScalarFunctions("VARCHAR", body,
            "ARRAY_APPEND",
            "ARRAY_PREPEND",
            "ARRAY_CAT",
            "ARRAY_REMOVE",
            "ARRAY_REPLACE",
            "ARRAY_POSITIONS");
    }

    private static void RegisterJsonFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "TO_JSON",
            "TO_JSONB",
            "ROW_TO_JSON",
            "JSON_SCALAR",
            "JSON_SERIALIZE",
            "JSONB_PATH_QUERY_ARRAY",
            "JSONB_BUILD_ARRAY",
            "JSONB_BUILD_OBJECT",
            "JSONB_OBJECT",
            "JSONB_TYPEOF",
            "JSONB_ARRAY_LENGTH",
            "JSONB_EXTRACT_PATH",
            "JSONB_EXTRACT_PATH_TEXT",
            "JSONB_STRIP_NULLS",
            "JSONB_SET",
            "JSONB_SET_LAX",
            "JSONB_INSERT",
            "JSONB_PRETTY",
            "JSONB_AGG",
            "JSONB_OBJECT_AGG",
            "JSONB_OBJECT_AGG_STRICT",
            "JSONB_OBJECT_AGG_UNIQUE",
            "JSONB_OBJECT_AGG_UNIQUE_STRICT",
            "JSON_AGG",
            "JSON_ARRAYAGG",
            "JSON_OBJECT_AGG",
            "JSON_OBJECTAGG",
            "JSON_BUILD_ARRAY",
            "JSON_BUILD_OBJECT",
            "JSON_ARRAY",
            "JSON_OBJECT",
            "JSON_TYPEOF",
            "JSON_ARRAY_LENGTH",
            "JSON_STRIP_NULLS",
            "JSON_EXTRACT_PATH",
            "JSON_EXTRACT_PATH_TEXT",
            "JSONB_PATH_EXISTS");
    }

    private static void RegisterRegexFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("REGEXP_COUNT", "INT", body);
        dialect.AddScalarFunction("REGEXP_INSTR", "INT", body);
        dialect.AddScalarFunction("REGEXP_LIKE", "BOOLEAN", body);
        dialect.AddScalarFunction("REGEXP_MATCH", "VARCHAR", body);
        dialect.AddScalarFunction("REGEXP_REPLACE", "VARCHAR", body);
        dialect.AddScalarFunction("REGEXP_SPLIT_TO_ARRAY", "VARCHAR", body);
        dialect.AddScalarFunction("REGEXP_SUBSTR", "VARCHAR", body);
    }
}
