namespace DbSqlLikeMem;

internal static class AutoSqlServerScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("DOUBLE", body,
            "COT",
            "DEGREES",
            "EXP",
            "FLOOR",
            "LOG",
            "LOG10",
            "PI",
            "POWER",
            "RADIANS",
            "RAND",
            "ROUND",
            "SIN",
            "SQUARE",
            "TAN",
            "SQRT");

        dialect.AddScalarFunctions("INT", body,
            "DIFFERENCE",
            "LEN",
            "UNICODE");

        dialect.AddScalarFunctions("VARCHAR", body,
            "LTRIM",
            "PARSENAME",
            "QUOTENAME",
            "REPLICATE",
            "REVERSE",
            "RTRIM",
            "SOUNDEX",
            "SPACE",
            "STUFF");

        dialect.AddScalarFunctions("INT", body,
            "DB_ID",
            "SCHEMA_ID",
            "SCOPE_IDENTITY",
            "SESSION_ID",
            "SUSER_ID",
            "USER_ID",
            "XACT_STATE");

        dialect.AddScalarFunctions("VARCHAR", body,
            "DB_NAME",
            "SCHEMA_NAME",
            "SERVERPROPERTY",
            "SUSER_NAME",
            "SUSER_SNAME",
            "USER_NAME");

        dialect.AddScalarFunctions("VARCHAR", body,
            SqlScalarFunctionUsageKind.Identifier,
            null,
            "CURRENT_USER",
            "SESSION_USER",
            "SYSTEM_USER");

        dialect.AddScalarFunctions("INT", body,
            SqlScalarFunctionUsageKind.Identifier,
            null,
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE",
            "@@ROWCOUNT");

        dialect.AddScalarFunctions("BIGINT", body,
            SqlScalarFunctionUsageKind.Identifier,
            null,
            "@@IDENTITY");

        dialect.AddScalarFunction("DATEFROMPARTS", "DATE", body);
        dialect.AddScalarFunction("DATETIMEFROMPARTS", "DATETIME", body);
        dialect.AddScalarFunction("DATETIME2FROMPARTS", "DATETIME", body);
        dialect.AddScalarFunction("DATETIMEOFFSETFROMPARTS", "DATETIMEOFFSET", body);
        dialect.AddScalarFunction("TIMEFROMPARTS", "TIME", body);
        dialect.AddScalarFunction("SMALLDATETIMEFROMPARTS", "DATETIME", body);

        dialect.AddScalarFunctions("BIGINT", body,
            "FOUND_ROWS",
            "ROW_COUNT",
            "CHANGES",
            "ROWCOUNT",
            "ROWCOUNT_BIG");

        dialect.AddScalarFunctions("BIGINT", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");

        dialect.AddScalarFunction("CHECKSUM_AGG", "INT", body);
    }
}
