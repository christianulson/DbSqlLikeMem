namespace DbSqlLikeMem.MariaDb;

internal static class MariaDbScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddScalarFunction("LENGTHB", "BIGINT", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("DECODE_ORACLE", "VARCHAR", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunctionIf(
            version >= MariaDbDialect.Crc32cMinVersion,
            "CRC32C",
            "BIGINT",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("NATURAL_SORT_KEY", "VARCHAR", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("SFORMAT", "VARCHAR", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("KDF", "VARBINARY", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("TRIM_ORACLE", "VARCHAR", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction("WEIGHT_STRING", "VARBINARY", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);

        dialect.AddScalarFunction("JSON_COMPACT", "VARCHAR", global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunctions(
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions,
            "JSON_PRETTY",
            "JSON_DETAILED",
            "JSON_LOOSE",
            "JSON_NORMALIZE");
        dialect.AddScalarFunctions(
            "INT",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions,
            "JSON_EQUALS",
            "JSON_EXISTS",
            "JSON_SCHEMA_VALID");
        dialect.AddScalarFunction(
            "JSON_ARRAY_INTERSECT",
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_OBJECT_FILTER_KEYS",
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_OBJECT_TO_ARRAY",
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        dialect.AddScalarFunction(
            "JSON_KEY_VALUE",
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);

        dialect.AddScalarFunctions(
            "VARBINARY",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions,
            "COLUMN_CREATE",
            "COLUMN_ADD",
            "COLUMN_DELETE");
        dialect.AddScalarFunctions(
            "INT",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions,
            "COLUMN_EXISTS",
            "COLUMN_CHECK");
        dialect.AddScalarFunctions(
            "VARCHAR",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions,
            "COLUMN_JSON",
            "COLUMN_LIST",
            "COLUMN_GET",
            "VEC_TOTEXT",
            "WSREP_LAST_SEEN_GTID",
            "WSREP_LAST_WRITTEN_GTID");
        dialect.AddScalarFunctions(
            "VARBINARY",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions,
            "VECTOR",
            "VEC_FROMTEXT");
        dialect.AddScalarFunctions(
            "DOUBLE",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions,
            "VEC_DISTANCE",
            "VEC_DISTANCE_EUCLIDEAN",
            "VEC_DISTANCE_COSINE");
        dialect.AddScalarFunction(
            "WSREP_SYNC_WAIT_UPTO_GTID",
            "INT",
            global::DbSqlLikeMem.QueryMariaDbSpecialFunctionHelper.TryEvalFunctions);
    }
}
