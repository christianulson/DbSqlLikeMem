using DbSqlLikeMem;

namespace DbSqlLikeMem.MariaDb;

internal static class MariaDbScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddScalarFunction("LENGTHB", "BIGINT", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("DECODE_ORACLE", "VARCHAR", QueryMariaDbFunctionHelper.TryEvalFunctions);
        if (version >= MariaDbDialect.Crc32cMinVersion)
            dialect.AddScalarFunction("CRC32C", "BIGINT", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("NATURAL_SORT_KEY", "VARCHAR", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("SFORMAT", "VARCHAR", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("KDF", "VARBINARY", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("TRIM_ORACLE", "VARCHAR", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("WEIGHT_STRING", "VARBINARY", QueryMariaDbFunctionHelper.TryEvalFunctions);

        dialect.AddScalarFunction("JSON_COMPACT", "VARCHAR", QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunctions(
            "VARCHAR",
            QueryMariaDbFunctionHelper.TryEvalFunctions,
            "JSON_PRETTY",
            "JSON_DETAILED",
            "JSON_LOOSE",
            "JSON_NORMALIZE");
        dialect.AddScalarFunctions(
            "INT",
            QueryMariaDbFunctionHelper.TryEvalFunctions,
            "JSON_EQUALS",
            "JSON_EXISTS",
            "JSON_SCHEMA_VALID");
        dialect.AddScalarFunction(
            "JSON_ARRAY_INTERSECT",
            "VARCHAR",
            QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_OBJECT_FILTER_KEYS",
            "VARCHAR",
            QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_OBJECT_TO_ARRAY",
            "VARCHAR",
            QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_KEY_VALUE",
            "VARCHAR",
            QueryMariaDbFunctionHelper.TryEvalFunctions);

        dialect.AddScalarFunctions(
            "VARBINARY",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions,
            "COLUMN_CREATE",
            "COLUMN_ADD",
            "COLUMN_DELETE");
        dialect.AddScalarFunctions(
            "INT",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions,
            "COLUMN_EXISTS",
            "COLUMN_CHECK");
        dialect.AddScalarFunctions(
            "VARCHAR",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions,
            "COLUMN_JSON",
            "COLUMN_LIST",
            "COLUMN_GET",
            "VEC_TOTEXT",
            "WSREP_LAST_SEEN_GTID",
            "WSREP_LAST_WRITTEN_GTID");
        dialect.AddScalarFunctions(
            "VARBINARY",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions,
            "VECTOR",
            "VEC_FROMTEXT");
        dialect.AddScalarFunctions(
            "DOUBLE",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions,
            "VEC_DISTANCE",
            "VEC_DISTANCE_EUCLIDEAN",
            "VEC_DISTANCE_COSINE");
        dialect.AddScalarFunction(
            "WSREP_SYNC_WAIT_UPTO_GTID",
            "INT",
            QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions);
    }
}
