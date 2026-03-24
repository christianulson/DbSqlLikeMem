using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerScalarFunctionRegistry
{
    internal static void Register(SqlServerDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);

        RegisterTemporalFunctions(dialect, version);
        RegisterMetadataFunctions(dialect, version);
        RegisterScalarFunctions(dialect, version);
        RegisterAggregateFunctions(dialect, version);
        RegisterFromPartsFunctions(dialect, version);
    }

    private static void RegisterTemporalFunctions(SqlServerDialect dialect, int version)
    {
        _ = version;

        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETDATE", "DATETIME", SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETUTCDATE", "DATETIME", SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion, "DATETIME",
            SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime,
            "SYSDATETIME",
            "SYSUTCDATETIME");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion, "DATETIMEOFFSET",
            SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTimeOffset,
            "SYSDATETIMEOFFSET");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion, "DATETIMEOFFSET",
            SqlFunctionBodyFactory.Identity(),
            "TODATETIMEOFFSET",
            "SWITCHOFFSET");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.EomonthMinVersion, "DATE",
            SqlFunctionBodyFactory.Identity(),
            "EOMONTH");

        dialect.AddScalarFunctions("INT", SqlFunctionBodyFactory.Identity(),
            "DATEDIFF",
            "DATENAME",
            "DATEPART",
            "DAY",
            "MONTH",
            SqlConst.YEAR);

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.DateDiffBigMinVersion, "BIGINT",
            SqlFunctionBodyFactory.Identity(),
            "DATEDIFF_BIG");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.FormatMinVersion, "VARCHAR",
            SqlFunctionBodyFactory.Identity(),
            "FORMAT");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.ParseMinVersion, "VARCHAR",
            SqlFunctionBodyFactory.Identity(),
            "PARSE",
            "TRY_PARSE");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.TryCastMinVersion, "VARCHAR",
            SqlFunctionBodyFactory.Identity(),
            "TRY_CAST");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.TryConvertMinVersion, "VARCHAR",
            SqlFunctionBodyFactory.Identity(),
            "TRY_CONVERT");

        dialect.AddScalarFunctions("DATETIME", SqlFunctionBodyFactory.Identity(),
            "DATEADD");

        dialect.AddScalarFunctions("BIGINT", SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            null,
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");
    }

    private static void RegisterMetadataFunctions(SqlServerDialect dialect, int version)
    {
        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("VARCHAR", body,
            "APP_NAME",
            "APPLOCK_MODE",
            "APPLOCK_TEST",
            "ASSEMBLYPROPERTY",
            "CERTENCODED",
            "CERTPRIVATEKEY",
            "CURRENT_REQUEST_ID",
            "CURRENT_TRANSACTION_ID",
            "CONTEXT_INFO",
            "DATABASE_PRINCIPAL_ID",
            "DATABASEPROPERTYEX",
            "CONNECTIONPROPERTY",
            "COLUMNPROPERTY",
            "COL_LENGTH",
            "COL_NAME",
            "CURSOR_STATUS",
            "DB_ID",
            "DB_NAME",
            "FILE_ID",
            "FILE_IDEX",
            "FILE_NAME",
            "FILEGROUP_ID",
            "FILEGROUP_NAME",
            "FILEGROUPPROPERTY",
            "FILEPROPERTY",
            "FULLTEXTCATALOGPROPERTY",
            "FULLTEXTSERVICEPROPERTY",
            "GET_FILESTREAM_TRANSACTION_CONTEXT",
            "HAS_PERMS_BY_NAME",
            "INDEX_COL",
            "INDEXKEY_PROPERTY",
            "INDEXPROPERTY",
            "MIN_ACTIVE_ROWVERSION",
            "OBJECT_ID",
            "OBJECT_DEFINITION",
            "OBJECTPROPERTY",
            "OBJECTPROPERTYEX",
            "OBJECT_NAME",
            "OBJECT_SCHEMA_NAME",
            "ORIGINAL_DB_NAME",
            "ORIGINAL_LOGIN",
            "PWDCOMPARE",
            "PWDENCRYPT",
            "ERROR_LINE",
            "ERROR_MESSAGE",
            "ERROR_NUMBER",
            "ERROR_PROCEDURE",
            "ERROR_SEVERITY",
            "ERROR_STATE",
            "GETANSINULL",
            "HOST_ID",
            "HOST_NAME",
            "IS_MEMBER",
            "IS_ROLEMEMBER",
            "IS_SRVROLEMEMBER",
            "SCHEMA_ID",
            "SCHEMA_NAME",
            "SERVERPROPERTY",
            "SESSION_ID",
            "SCOPE_IDENTITY",
            "SUSER_ID",
            "SUSER_NAME",
            "SUSER_SID",
            "SUSER_SNAME",
            "STATS_DATE",
            "TYPE_ID",
            "TYPE_NAME",
            "TYPEPROPERTY",
            "USER_ID",
            "USER_NAME",
            "XACT_STATE");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.SessionContextMinVersion, "VARCHAR", body, "SESSION_CONTEXT");

        dialect.AddScalarFunctions("INT", body,
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE");

        dialect.AddScalarFunctions("BIGINT", body,
            "@@IDENTITY");

        dialect.AddScalarFunctions("BIGINT", body,
            SqlScalarFunctionUsageKind.Identifier,
            null,
            "@@ROWCOUNT");

        dialect.AddScalarFunctions("VARCHAR", body,
            "CURRENT_USER",
            "SESSION_USER",
            "SYSTEM_USER");
    }

    private static void RegisterScalarFunctions(SqlServerDialect dialect, int version)
    {
        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("DOUBLE", body,
            "ABS",
            "ACOS",
            "ASIN",
            "ATAN",
            "ATN2",
            "CEILING",
            "COS",
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
            "SIGN",
            "SIN",
            "SQUARE",
            "TAN",
            "SQRT");

        dialect.AddScalarFunctions("INT", body,
            "ASCII",
            "CHARINDEX",
            "CHECKSUM",
            "BINARY_CHECKSUM",
            "DATALENGTH",
            "DIFFERENCE",
            "GROUPING",
            "GROUPING_ID",
            "ISDATE",
            "ISJSON",
            "ISNUMERIC",
            "LEN",
            "PATINDEX",
            "UNICODE");

        dialect.AddScalarFunctions("INT", body,
            "ROWCOUNT");

        dialect.AddScalarFunctions("BIGINT", body,
            "ROWCOUNT_BIG");

        dialect.AddScalarFunctions("VARCHAR", body,
            "CHAR",
            "CONCAT",
            "CONCAT_WS",
            "FORMATMESSAGE",
            "LEFT",
            "LOWER",
            "NCHAR",
            "NEWID",
            "NEWSEQUENTIALID",
            "PARSENAME",
            "QUOTENAME",
            "REPLICATE",
            "REVERSE",
            "REPLACE",
            "RIGHT",
            "ROUND",
            "SIGN",
            "SOUNDEX",
            "SPACE",
            "STR",
            "STUFF",
            "SUBSTRING",
            "TRIM",
            "TRANSLATE",
            "UPPER",
            "LTRIM",
            "RTRIM");

        dialect.AddScalarFunctions("VARCHAR", body,
            "IF",
            "IIF");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.JsonFunctionsMinVersion, "VARCHAR", body,
            "JSON_MODIFY",
            "JSON_QUERY",
            "JSON_VALUE");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.CompressionFunctionsMinVersion, "VARBINARY", body,
            "COMPRESS",
            "DECOMPRESS");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.StringEscapeMinVersion, "VARCHAR", body,
            "STRING_ESCAPE");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.TranslateMinVersion, "VARCHAR", body,
            "TRANSLATE");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion, "DATETIMEOFFSET", body,
            "TODATETIMEOFFSET",
            "SWITCHOFFSET");
    }

    private static void RegisterAggregateFunctions(SqlServerDialect dialect, int version)
    {
        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.StringAggMinVersion, "VARCHAR", body,
            "STRING_AGG");

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.ApproxCountDistinctMinVersion, "BIGINT", body,
            "APPROX_COUNT_DISTINCT");

        dialect.AddScalarFunctions("INT", body,
            "CHECKSUM_AGG");
    }

    private static void RegisterFromPartsFunctions(SqlServerDialect dialect, int version)
    {
        if (version < SqlServerDialect.FromPartsMinVersion)
            return;

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("DATE", body,
            "DATEFROMPARTS");

        dialect.AddScalarFunctions("DATETIME", body,
            "DATETIMEFROMPARTS",
            "DATETIME2FROMPARTS",
            "SMALLDATETIMEFROMPARTS");

        dialect.AddScalarFunctions("DATETIMEOFFSET", body,
            "DATETIMEOFFSETFROMPARTS");

        dialect.AddScalarFunctions("TIME", body,
            "TIMEFROMPARTS");
    }
}
