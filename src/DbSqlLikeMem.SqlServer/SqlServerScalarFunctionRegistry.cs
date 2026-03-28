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
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => utilityEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalSqlServerSessionContextFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => utilityEvaluator.TryEvalSessionContextFunction(context, fn, evalArg, out result);

        RegisterTemporalFunctions(dialect, version, TryEvalSqlServerUtilityFunction);
        RegisterMetadataFunctions(dialect, version);
        if (version >= SqlServerDialect.SessionContextMinVersion)
            dialect.AddScalarFunction(
                CreateScalarFunctionDef(
                    "SESSION_CONTEXT",
                    "VARCHAR",
                    TryEvalSqlServerSessionContextFunction));
        RegisterScalarFunctions(dialect, version, TryEvalSqlServerUtilityFunction, TryEvalSqlServerSessionContextFunction);
        RegisterAggregateFunctions(dialect, version);
        RegisterFromPartsFunctions(dialect, version);
    }

    private static DbFunctionDef CreateScalarFunctionDef(
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler executor,
        DbInvocationStyle invocationStyle = DbInvocationStyle.Call,
        SqlTemporalFunctionKind? temporalKind = null)
    {
        var definition = temporalKind is SqlTemporalFunctionKind temporal
            ? DbFunctionDef.CreateTemporal(name, returnTypeSql, temporal, invocationStyle)
            : invocationStyle switch
            {
                DbInvocationStyle.Identifier => DbFunctionDef.CreateIdentifier(name, returnTypeSql),
                _ when invocationStyle == (DbInvocationStyle.Call | DbInvocationStyle.Identifier) => DbFunctionDef.CreateCallOrIdentifier(name, returnTypeSql),
                _ => DbFunctionDef.CreateScalar(name, returnTypeSql, invocationStyle: invocationStyle)
            };

        return definition with
        {
            AstExecutor = executor
        };
    }

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
                DbInvocationStyle.Identifier,
                SqlTemporalFunctionKind.DateTime));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "GETDATE",
                "DATETIME",
                tryEvalSqlServerUtilityFunction,
                DbInvocationStyle.Call,
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
                DbInvocationStyle.Identifier,
                SqlTemporalFunctionKind.DateTime));

        if (version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion)
        {
            dialect.AddScalarFunctions(
                CreateScalarFunctionDef(
                    "SYSDATETIME",
                    "DATETIME",
                    tryEvalSqlServerUtilityFunction,
                    DbInvocationStyle.Call,
                    SqlTemporalFunctionKind.DateTime),
                "SYSDATETIME",
                "SYSUTCDATETIME");

            dialect.AddScalarFunctions(
                CreateScalarFunctionDef(
                    "SYSDATETIMEOFFSET",
                    "DATETIMEOFFSET",
                    tryEvalSqlServerUtilityFunction,
                    DbInvocationStyle.Call,
                    SqlTemporalFunctionKind.DateTimeOffset),
                "SYSDATETIMEOFFSET");
        }

        if (version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarFunctionDef(
                    "TODATETIMEOFFSET",
                    "DATETIMEOFFSET",
                    tryEvalSqlServerUtilityFunction),
                "TODATETIMEOFFSET",
                "SWITCHOFFSET");

        if (version >= SqlServerDialect.EomonthMinVersion)
            dialect.AddScalarFunction(
                "EOMONTH",
                "DATE",
                AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DATEDIFF", "INT"),
            "DATEDIFF",
            "DATENAME",
            "DATEPART",
            "DAY",
            "MONTH",
            SqlConst.YEAR);

        if (version >= SqlServerDialect.DateDiffBigMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("DATEDIFF_BIG", "BIGINT"));

        if (version >= SqlServerDialect.FormatMinVersion)
            dialect.AddScalarFunction(CreateScalarFunctionDef("FORMAT", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatFunction));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("PARSE", "VARCHAR"),
            "PARSE",
            "TRY_PARSE");

        if (version >= SqlServerDialect.TryCastMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR"));

        if (version >= SqlServerDialect.TryConvertMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CONVERT", "VARCHAR"));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DATEADD", "DATETIME"),
            "DATEADD");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("NEXT_VALUE_FOR", "BIGINT"),
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");
    }

    private static void RegisterMetadataFunctions(SqlServerDialect dialect, int version)
    {
        dialect.AddScalarFunction(CreateScalarFunctionDef("APP_NAME", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalAppNameFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("GETANSINULL", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalGetAnsiNullFunction));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef("HOST_ID", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostIdFunction));
        dialect.AddScalarFunction(
            CreateScalarFunctionDef("HOST_NAME", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostNameFunction));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("APPLOCK_MODE", "VARCHAR"),
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

        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_LINE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_MESSAGE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_NUMBER", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_PROCEDURE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_SEVERITY", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ERROR_STATE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalErrorFunctions));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateIdentifier("@@DATEFIRST", "INT"),
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE");

        dialect.AddScalarFunction(DbFunctionDef.CreateIdentifier("@@IDENTITY", "BIGINT"));

        dialect.AddScalarFunction(DbFunctionDef.CreateIdentifier("@@ROWCOUNT", "BIGINT"));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "CURRENT_USER",
                "VARCHAR",
                AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCurrentUserFunction,
                DbInvocationStyle.Identifier));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "SESSION_USER",
                "VARCHAR",
                AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSessionUserFunction,
                DbInvocationStyle.Identifier));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef(
                "SYSTEM_USER",
                "VARCHAR",
                AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSystemUserFunction,
                DbInvocationStyle.Identifier));
    }

    private static void RegisterScalarFunctions(
        SqlServerDialect dialect,
        int version,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerUtilityFunction,
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerSessionContextFunction)
    {
        dialect.AddScalarFunction("CHARINDEX", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCharIndexFunction);

        dialect.AddScalarFunctions("INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerChecksumFunction,
            "CHECKSUM",
            "BINARY_CHECKSUM");

        dialect.AddScalarFunction("DATALENGTH", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDataLengthFunction);

        dialect.AddScalarFunctions("INT", AstQueryGroupingFunctionEvaluator.TryEvaluate,
            "GROUPING",
            "GROUPING_ID");

        dialect.AddScalarFunction("ISDATE", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsDateFunction);

        dialect.AddScalarFunction("ISJSON", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsJsonFunction);

        dialect.AddScalarFunction("ISNUMERIC", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsNumericFunction);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("ROWCOUNT", "INT"),
            "ROWCOUNT");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("ROWCOUNT_BIG", "BIGINT"),
            "ROWCOUNT_BIG");

        dialect.AddScalarFunction("FORMATMESSAGE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatMessageFunction);

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

        if (version >= SqlServerDialect.StringEscapeMinVersion)
            dialect.AddScalarFunction("STRING_ESCAPE", "VARCHAR", tryEvalSqlServerUtilityFunction);

        dialect.AddScalarFunction("STR", "VARCHAR", tryEvalSqlServerUtilityFunction);

        if (version >= SqlServerDialect.CompressionFunctionsMinVersion)
        {
            dialect.AddScalarFunction(CreateScalarFunctionDef("COMPRESS", "VARBINARY", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerCompressFunction));
            dialect.AddScalarFunction(CreateScalarFunctionDef("DECOMPRESS", "VARBINARY", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerDecompressFunction));
        }

        dialect.AddScalarFunctions("VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions,
            "IF",
            "IIF");

        if (version >= SqlServerDialect.JsonFunctionsMinVersion)
        {
            dialect.AddScalarFunction("JSON_QUERY", "VARCHAR", AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction);
            dialect.AddScalarFunction("JSON_VALUE", "VARCHAR", AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction);
        }

        RegisterGeneralScalarFunctions(dialect);

    }

    private static void RegisterGeneralScalarFunctions(
        SqlServerDialect dialect)
    {
        dialect.AddScalarFunction("SOUNDEX", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSoundexFunction);
        dialect.AddScalarFunction("PATINDEX", "INT", AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("DIFFERENCE", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDifferenceFunction);
    }

    private static void RegisterAggregateFunctions(SqlServerDialect dialect, int version)
    {
        if (version >= SqlServerDialect.StringAggMinVersion)
            dialect.AddScalarFunction(
                DbFunctionDef.CreateScalar(SqlConst.STRING_AGG, "VARCHAR") with
                {
                    IsStringAggregate = true
                });

        if (version >= SqlServerDialect.ApproxCountDistinctMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("APPROX_COUNT_DISTINCT", "BIGINT"));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.CHECKSUM_AGG, "INT"),
            SqlConst.CHECKSUM_AGG);
    }

    private static void RegisterFromPartsFunctions(SqlServerDialect dialect, int version)
    {
        if (version < SqlServerDialect.FromPartsMinVersion)
            return;

        dialect.AddScalarFunction(
            "DATEFROMPARTS",
            "DATE",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunctions(
            "DATETIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction,
            "DATETIMEFROMPARTS",
            "DATETIME2FROMPARTS",
            "SMALLDATETIMEFROMPARTS");

        dialect.AddScalarFunction(
            "DATETIMEOFFSETFROMPARTS",
            "DATETIMEOFFSET",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunction(
            "TIMEFROMPARTS",
            "TIME",
            AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
    }
}
