using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerScalarFunctionRegistry
{
    internal static void Register(SqlServerDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);

        var utilityEvaluator = new AstQuerySqlServerUtilityFunctionEvaluator(
            getDialect: () => dialect,
            tryConvertNumericToDecimal: AstQueryExecutorBase.TryConvertNumericToDecimal,
            tryCoerceDateTime: AstQueryExecutorBase.TryCoerceDateTime,
            tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
            tryParseCachedDateTimeOffset: AstQueryExecutorBase.TryParseCachedDateTimeOffset);

        bool TryEvalSqlServerUtilityFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => utilityEvaluator.TryEvaluate(fn, evalArg, out result);

        RegisterTemporalFunctions(dialect, version, TryEvalSqlServerUtilityFunction);
        RegisterMetadataFunctions(dialect, version);
        RegisterScalarFunctions(dialect, version, TryEvalSqlServerUtilityFunction);
        RegisterAggregateFunctions(dialect, version);
        RegisterFromPartsFunctions(dialect, version);
    }

    private static DbScalarFunctionDef CreateScalarFunctionDef(
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executor,
        SqlScalarFunctionUsageKind usageKind = SqlScalarFunctionUsageKind.Call,
        SqlTemporalFunctionKind? temporalKind = null)
        => new(name, returnTypeSql, [], SqlFunctionBodyFactory.Identity(), usageKind, temporalKind)
        {
            AstExecutor = executor
        };

    private static bool TryEvalSqlServerAppNameFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalAppNameFunction(fn, out result);

    private static bool TryEvalSqlServerCurrentUserFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalCurrentUserFunction(fn, context, out result);

    private static bool TryEvalSqlServerErrorFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalErrorFunctions(fn, out result);

    private static bool TryEvalSqlServerNumericFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalNumericFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerCharIndexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalCharIndexFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerChecksumFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalSqlServerChecksumFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerDataLengthFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalDataLengthFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerFormatMessageFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalSqlServerFormatMessageFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerCompressFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalSqlServerCompressFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerDecompressFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalSqlServerDecompressFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerIsDateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalIsDateFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerIsJsonFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalIsJsonFunction(fn, evalArg, out result);

    private static bool TryEvalSqlServerIsNumericFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryExecutorBase.TryEvalIsNumericFunction(fn, evalArg, out result);

    private static void RegisterTemporalFunctions(
        SqlServerDialect dialect,
        int version,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerUtilityFunction)
    {
        _ = version;

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "CURRENT_TIMESTAMP",
                "DATETIME",
                tryEvalSqlServerUtilityFunction,
                SqlScalarFunctionUsageKind.Identifier,
                SqlTemporalFunctionKind.DateTime));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "GETDATE",
                "DATETIME",
                tryEvalSqlServerUtilityFunction,
                SqlScalarFunctionUsageKind.Call,
                SqlTemporalFunctionKind.DateTime));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "GETUTCDATE",
                "DATETIME",
                tryEvalSqlServerUtilityFunction,
                temporalKind: SqlTemporalFunctionKind.DateTime));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "SYSTEMDATE",
                "DATETIME",
                tryEvalSqlServerUtilityFunction,
                SqlScalarFunctionUsageKind.Identifier,
                SqlTemporalFunctionKind.DateTime));

        dialect.AddScalarFunctionsIf(
            version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion,
            "DATETIME",
            tryEvalSqlServerUtilityFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime,
            "SYSDATETIME",
            "SYSUTCDATETIME");

        dialect.AddScalarFunctionsIf(
            version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion,
            "DATETIMEOFFSET",
            tryEvalSqlServerUtilityFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTimeOffset,
            "SYSDATETIMEOFFSET");

        dialect.AddScalarFunctionsIf(
            version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion,
            "DATETIMEOFFSET",
            tryEvalSqlServerUtilityFunction,
            "TODATETIMEOFFSET",
            "SWITCHOFFSET");

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.EomonthMinVersion,
            "EOMONTH",
            "DATE",
            AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate);

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

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.FormatMinVersion,
            CreateScalarFunctionDef("FORMAT", "VARCHAR", AstQueryExecutorBase.TryEvalSqlServerFormatFunction));

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

        dialect.AddScalarFunctions(
            "BIGINT",
            SqlFunctionBodyFactory.Identity(),
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");
    }

    private static void RegisterMetadataFunctions(SqlServerDialect dialect, int version)
    {
        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunction(CreateScalarFunctionDef("APP_NAME", "VARCHAR", TryEvalSqlServerAppNameFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("GETANSINULL", "VARCHAR", AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalGetAnsiNullFunction));
        dialect.AddScalarFunctions(
            CreateScalarFunctionDef("HOST_ID", "VARCHAR", AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalHostIdFunction),
            "HOST_ID",
            "HOST_NAME");

        dialect.AddScalarFunctions("VARCHAR", body,
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

        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_LINE", "VARCHAR", TryEvalSqlServerErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_MESSAGE", "VARCHAR", TryEvalSqlServerErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_NUMBER", "VARCHAR", TryEvalSqlServerErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_PROCEDURE", "VARCHAR", TryEvalSqlServerErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_SEVERITY", "VARCHAR", TryEvalSqlServerErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_STATE", "VARCHAR", TryEvalSqlServerErrorFunctions));

        dialect.AddScalarFunctionsIf(version >= SqlServerDialect.SessionContextMinVersion, "VARCHAR", body, "SESSION_CONTEXT");

        dialect.AddScalarFunctions(
            "INT",
            body,
            SqlScalarFunctionUsageKind.Identifier,
            null,
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE");

        dialect.AddScalarFunction(
            "@@IDENTITY",
            "BIGINT",
            body,
            SqlScalarFunctionUsageKind.Identifier,
            null);

        dialect.AddScalarFunction(
            "@@ROWCOUNT",
            "BIGINT",
            body,
            SqlScalarFunctionUsageKind.Identifier,
            null);

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "CURRENT_USER",
                "VARCHAR",
                TryEvalSqlServerCurrentUserFunction,
                SqlScalarFunctionUsageKind.Identifier));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "SESSION_USER",
                "VARCHAR",
                AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalSessionUserFunction,
                SqlScalarFunctionUsageKind.Identifier));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "SYSTEM_USER",
                "VARCHAR",
                AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalSystemUserFunction,
                SqlScalarFunctionUsageKind.Identifier));
    }

    private static void RegisterScalarFunctions(
        SqlServerDialect dialect,
        int version,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerUtilityFunction)
    {
        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("DOUBLE", TryEvalSqlServerNumericFunction,
            "ABS",
            "ACOS",
            "ASIN",
            "ATAN",
            "ATN2",
            "CEILING",
            "COS",
            "COT");

        dialect.AddScalarFunction("ASCII", "INT", TryEvalSqlServerNumericFunction);

        dialect.AddScalarFunction("CHARINDEX", "INT", TryEvalSqlServerCharIndexFunction);

        dialect.AddScalarFunctions("INT", TryEvalSqlServerChecksumFunction,
            "CHECKSUM",
            "BINARY_CHECKSUM");

        dialect.AddScalarFunction("DATALENGTH", "INT", TryEvalSqlServerDataLengthFunction);

        dialect.AddScalarFunctions("INT", AstQueryExecutorBase.TryEvalGroupingFunctions,
            "GROUPING",
            "GROUPING_ID");

        dialect.AddScalarFunction("ISDATE", "INT", TryEvalSqlServerIsDateFunction);

        dialect.AddScalarFunction("ISJSON", "INT", TryEvalSqlServerIsJsonFunction);

        dialect.AddScalarFunction("ISNUMERIC", "INT", TryEvalSqlServerIsNumericFunction);

        dialect.AddScalarFunctions("INT", body,
            "ROWCOUNT");

        dialect.AddScalarFunctions("BIGINT", body,
            "ROWCOUNT_BIG");

        dialect.AddScalarFunction("CHAR", "VARCHAR", AstQueryExecutorBase.TryEvalCharFunction);

        dialect.AddScalarFunction("FORMATMESSAGE", "VARCHAR", TryEvalSqlServerFormatMessageFunction);

        dialect.AddScalarFunction("NCHAR", "VARCHAR", AstQueryExecutorBase.TryEvalCharFunction);

        dialect.AddScalarFunctions("VARCHAR", AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate,
            "QUOTENAME",
            "REPLICATE",
            "STUFF",
            "PARSENAME");

        dialect.AddScalarFunction("SQUARE", "DOUBLE", AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction("ISNULL", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);

        dialect.AddScalarFunctions("VARCHAR", tryEvalSqlServerUtilityFunction,
            "NEWID",
            "NEWSEQUENTIALID");

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.StringEscapeMinVersion,
            "STRING_ESCAPE",
            "VARCHAR",
            tryEvalSqlServerUtilityFunction);

        dialect.AddScalarFunction("STR", "VARCHAR", tryEvalSqlServerUtilityFunction);

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.CompressionFunctionsMinVersion,
            CreateScalarFunctionDef("COMPRESS", "VARBINARY", TryEvalSqlServerCompressFunction));

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.CompressionFunctionsMinVersion,
            CreateScalarFunctionDef("DECOMPRESS", "VARBINARY", TryEvalSqlServerDecompressFunction));

        dialect.AddScalarFunctions("VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions,
            "IF",
            "IIF");

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.JsonFunctionsMinVersion,
            "JSON_QUERY",
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonExtractionFunction);

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.JsonFunctionsMinVersion,
            "JSON_VALUE",
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonExtractionFunction);

        dialect.AddScalarFunctionIf(
            version >= SqlServerDialect.TranslateMinVersion,
            "TRANSLATE",
            "VARCHAR",
            AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);

        RegisterGeneralScalarFunctions(dialect, AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);

    }

    private static void RegisterGeneralScalarFunctions(
        SqlServerDialect dialect,
        AstQueryGeneralScalarFunctionHandler tryEvalGeneralScalarFunction)
    {
        dialect.AddScalarFunctions("VARCHAR", tryEvalGeneralScalarFunction,
            "LOWER",
            "UPPER",
            "LTRIM",
            "RTRIM",
            "TRIM",
            "LEFT",
            "RIGHT",
            "REVERSE",
            "SPACE",
            "SOUNDEX");

        dialect.AddScalarFunction("LEN", "INT", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("SUBSTRING", "VARCHAR", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("PATINDEX", "INT", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("DEGREES", "DOUBLE", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("DIFFERENCE", "INT", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("EXP", "DOUBLE", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("FLOOR", "DOUBLE", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("REPLACE", "VARCHAR", tryEvalGeneralScalarFunction);
        dialect.AddScalarFunction("UNICODE", "INT", tryEvalGeneralScalarFunction);

        dialect.AddScalarFunctions("DOUBLE", tryEvalGeneralScalarFunction,
            "LOG",
            "LOG10",
            "PI",
            "POWER",
            "RADIANS",
            "RAND",
            "ROUND",
            "SIN",
            "SQRT",
            "TAN");
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

        dialect.AddScalarFunction(
            "DATEFROMPARTS",
            "DATE",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions(
            "DATETIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate,
            "DATETIMEFROMPARTS",
            "DATETIME2FROMPARTS",
            "SMALLDATETIMEFROMPARTS");

        dialect.AddScalarFunction(
            "DATETIMEOFFSETFROMPARTS",
            "DATETIMEOFFSET",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "TIMEFROMPARTS",
            "TIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
    }
}
