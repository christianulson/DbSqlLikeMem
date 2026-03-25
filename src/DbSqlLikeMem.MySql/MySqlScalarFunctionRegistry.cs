using DbSqlLikeMem;
using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterScalarFunctions(int version)
    {
        SqlSharedScalarFunctionRegistry.Register(this);

        var body = SqlFunctionBodyFactory.Identity();
        var emptyUtilityRow = new AstQueryExecutorBase.EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase));

        bool TryEvalMySqlGeneralScalarFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        bool TryEvalMySqlUtilityFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryMySqlUtilityFunctionEvaluator.TryEvaluate(
                fn,
                context,
                emptyUtilityRow,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                out result);

        bool TryEvalMySqlQueryUtilityFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlUtilityFunctionHelper.TryEvalUtilityFunctions(
                fn,
                context,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                out result);

        bool TryEvalMySqlNumericFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryExecutorBase.TryEvalNumericFunction(fn, evalArg, out result);

        bool TryEvalMySqlDateTimeFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlDateTimeFunctionHelper.TryEvalFunctions(
                fn,
                context,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryCoerceDateTime,
                AstQueryExecutorBase.TryParseExactCachedDateTime,
                out result);

        bool TryEvalMySqlFindInSetFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return QueryTextSearchFunctionHelper.TryEvalFindInSetFunction(fn, evalArg, out result);
        }

        static bool TryEvalNoopSessionContextFunction(
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = evalArg;
            result = null;
            return false;
        }

        static bool TryEvalNoopGeneralSystemAndJsonFunction(
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
            TryEvalNoopGeneralSystemAndJsonFunction,
            TryEvalNoopGeneralSystemAndJsonFunction,
            TryEvalNoopGeneralSystemAndJsonFunction);

        bool TryEvalMySqlGeneralSystemAndJsonFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => generalSystemAndJsonFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result);

        var tryEvalMySqlGeneralScalarFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlGeneralScalarFunction;
        var tryEvalMySqlUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlUtilityFunction;
        var tryEvalMySqlQueryUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlQueryUtilityFunction;
        var tryEvalMySqlDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlDateTimeFunction;
        var tryEvalMySqlNumericFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlNumericFunction;
        var tryEvalMySqlGeneralSystemAndJsonFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlGeneralSystemAndJsonFunction;
        var tryEvalMySqlConversionAndMetadataFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate;
        var tryEvalConvertFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryExecutorBase.TryEvalConvertFunction;
        var tryEvalDateFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralDateFunctionEvaluator.TryEvaluate;
        var tryEvalDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate;
        var tryEvalJsonUtilityFunctions = (AstQueryGeneralScalarFunctionHandler)AstQueryExecutorBase.TryEvalJsonUtilityFunctions;
        var tryEvalJsonExtractionFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryExecutorBase.TryEvalJsonExtractionFunction;
        var tryEvalFindInSetFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlFindInSetFunction;

        this.AddScalarFunction(
            "IFNULL",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            "DATABASE",
            "SCHEMA");
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "SESSION_USER",
            "CURRENT_USER");
        this.AddScalarFunction(
            "CONNECTION_ID",
            "BIGINT",
            tryEvalMySqlConversionAndMetadataFunction);
        this.AddScalarFunction(
            "VERSION",
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            "CHARSET",
            "COLLATION",
            "COERCIBILITY");
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "USER",
            "SYSTEM_USER");
        this.AddScalarFunctions("BIGINT", body, "FOUND_ROWS", "ROW_COUNT");
        this.AddScalarFunction("LAST_INSERT_ID", "BIGINT", body);
        this.AddScalarFunction("GROUP_CONCAT", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);

        this.AddScalarFunction(
            "CURDATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURTIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_DATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURRENT_TIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_TIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "LOCALTIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "LOCALTIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "NOW",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSDATE",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSTEMDATE",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "UTC_DATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "UTC_TIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "UTC_TIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);

        this.AddScalarFunctions(
            "DATE",
            tryEvalDateFunction,
            "DATE",
            "TIME",
            "TIMESTAMP");

        this.AddScalarFunctions(
            "BIGINT",
            tryEvalMySqlGeneralScalarFunction,
            "CHAR_LENGTH",
            "CHARACTER_LENGTH");
        this.AddScalarFunctions(
            "INT",
            tryEvalMySqlGeneralScalarFunction,
            "BIT_LENGTH",
            "BIT_COUNT");
        this.AddScalarFunction(
            "BIN",
            "VARCHAR",
            executionHandler: tryEvalMySqlNumericFunction);
        this.AddScalarFunction(
            "CRC32",
            "BIGINT",
            executionHandler: tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "CONVERT",
            "VARCHAR",
            executionHandler: tryEvalConvertFunction);
        this.AddScalarFunction(
            "CONV",
            "VARCHAR",
            executionHandler: tryEvalMySqlConversionAndMetadataFunction);
        this.AddScalarFunction(
            "DATE_SUB",
            "DATETIME",
            body);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlUtilityFunction,
            "ELT",
            "MAKE_SET",
            "EXPORT_SET");
        this.AddScalarFunction(
            "FIND_IN_SET",
            "INT",
            executionHandler: tryEvalFindInSetFunction);
        this.AddScalarFunction(
            "FROM_BASE64",
            "VARBINARY",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "TO_BASE64",
            "VARCHAR",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "FROM_DAYS",
            "DATE",
            tryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "GET_FORMAT",
            "VARCHAR",
            tryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "INET_ATON",
            "BIGINT",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET_NTOA",
            "VARCHAR",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET6_ATON",
            "VARBINARY",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET6_NTOA",
            "VARCHAR",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4",
            "INT",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4_COMPAT",
            "INT",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4_MAPPED",
            "INT",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV6",
            "INT",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunctions(
            "VARBINARY",
            tryEvalMySqlUtilityFunction,
            "AES_ENCRYPT");
        this.AddScalarFunctionsIf(
            version < 80,
            "VARBINARY",
            tryEvalMySqlUtilityFunction,
            "DES_ENCRYPT",
            "ENCODE");
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlUtilityFunction,
            "AES_DECRYPT");
        this.AddScalarFunctionsIf(
            version < 80,
            "VARCHAR",
            tryEvalMySqlUtilityFunction,
            "DES_DECRYPT",
            "DECODE",
            "ENCRYPT");
        this.AddScalarFunction("DEFAULT", "VARCHAR", tryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(version >= 80, "MEMBER_OF", "BOOLEAN", tryEvalMySqlUtilityFunction);
        this.AddScalarFunctions("VARCHAR", tryEvalMySqlUtilityFunction, "EXTRACTVALUE", "UPDATEXML");
        this.AddScalarFunction("LOG2", "DOUBLE", tryEvalMySqlGeneralScalarFunction);
        this.AddScalarFunction("OCT", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("ORD", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryGeneralScalarFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "GREATEST",
            "LEAST");
        this.AddScalarFunction("POSITION", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("RPAD", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("SLEEP", "INT", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("SUBSTRING_INDEX", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("OCTET_LENGTH", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions("VARBINARY", tryEvalMySqlUtilityFunction, "COMPRESS");
        this.AddScalarFunctions("VARBINARY", tryEvalMySqlUtilityFunction, "UNCOMPRESS");
        this.AddScalarFunction(
            "CONVERT_TZ",
            "DATETIME",
            tryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "TRUNCATE",
            "DECIMAL",
            executionHandler: tryEvalDateTimeFunction);
        this.AddScalarFunction(
            "UNIX_TIMESTAMP",
            "BIGINT",
            executionHandler: tryEvalDateTimeFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlGeneralScalarFunction,
            "SHA",
            "SHA1",
            "SHA2");
        this.AddScalarFunction(
            "UNCOMPRESSED_LENGTH",
            "BIGINT",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "FORMAT",
            "VARCHAR",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "HEX",
            "VARCHAR",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "UNHEX",
            "VARBINARY",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunctionsIf(
            version >= 80,
            "INT",
            tryEvalMySqlUtilityFunction,
            "REGEXP_INSTR",
            "REGEXP_REPLACE",
            "REGEXP_SUBSTR",
            "REGEXP_LIKE");
        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "VARCHAR",
            tryEvalJsonExtractionFunction,
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");

        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "VARCHAR",
            tryEvalJsonUtilityFunctions,
            "JSON_UNQUOTE",
            "JSON_OBJECT",
            "JSON_QUOTE",
            "JSON_PRETTY",
            "JSON_KEYS",
            "JSON_SET",
            "JSON_REMOVE",
            "JSON_CONTAINS",
            "JSON_CONTAINS_PATH",
            "JSON_SEARCH",
            "JSON_INSERT",
            "JSON_REPLACE",
            "JSON_ARRAY_APPEND",
            "JSON_ARRAY_INSERT",
            "JSON_MERGE",
            "JSON_MERGE_PRESERVE",
            "JSON_MERGE_PATCH");

        this.AddScalarFunctionIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "JSON_TYPE",
            "VARCHAR",
            tryEvalJsonUtilityFunctions);

        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "INT",
            tryEvalJsonUtilityFunctions,
            "JSON_VALID",
            "JSON_LENGTH",
            "JSON_CONTAINS",
            "JSON_CONTAINS_PATH",
            "JSON_OVERLAPS");

        this.AddScalarFunctionIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "JSON_ARRAY",
            "VARCHAR",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        this.AddScalarFunctionIf(version >= 56, "JSON_ARRAYAGG", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunctionIf(version >= 56, "JSON_OBJECTAGG", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);

        this.AddScalarFunctionIf(
            version >= 80,
            "JSON_STORAGE_SIZE",
            "BIGINT",
            tryEvalJsonUtilityFunctions);

        this.AddScalarFunctionIf(
            version >= 80,
            "JSON_DEPTH",
            "INT",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        this.AddScalarFunctionIf(
            version >= 80,
            "IS_UUID",
            "INT",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        this.AddScalarFunctionIf(
            version >= 80,
            "UUID_SHORT",
            "BIGINT",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "GROUPING",
            "INT",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "GROUPING_ID",
            "INT",
            executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);
        this.AddScalarFunctions(
            "DATETIME",
            AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate,
            "ADDDATE",
            "ADDTIME",
            "LAST_DAY",
            "EOMONTH",
            "SUBTIME");

        this.AddScalarFunction("DATE_ADD", "DATETIME", body);
        this.AddScalarFunction("TIMESTAMPADD", "DATETIME", body);
        this.AddScalarFunction("TRY_CAST", "VARCHAR", body);
        this.AddScalarFunction("DATEDIFF", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("TIMESTAMPDIFF", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("DAY", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("MONTH", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("YEAR", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("HOUR", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("MINUTE", "INT", executionHandler: tryEvalDateFunction);
        this.AddScalarFunction("SECOND", "INT", executionHandler: tryEvalDateFunction);

        this.AddScalarFunctions(
            "DATE",
            tryEvalDateFunction,
            "MAKEDATE",
            "MAKETIME",
            "MICROSECOND",
            "MONTHNAME",
            "PERIOD_ADD",
            "PERIOD_DIFF",
            "QUARTER",
            "SEC_TO_TIME");

        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalDateTimeFunction,
            "TIME_FORMAT",
            "TIME_TO_SEC",
            "TIMEDIFF",
            "TO_DAYS",
            "TO_SECONDS",
            "TRUNCATE",
            "WEEK",
            "WEEKDAY",
            "WEEKOFYEAR",
            "YEARWEEK");

        this.AddScalarFunction(
            "DATE_FORMAT",
            "VARCHAR",
            executionHandler: tryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "STR_TO_DATE",
            "DATETIME",
            executionHandler: tryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "FROM_UNIXTIME",
            "DATETIME",
            executionHandler: tryEvalMySqlDateTimeFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            "DAYNAME",
            "DAYOFMONTH",
            "DAYOFWEEK",
            "DAYOFYEAR");
        this.AddScalarFunction("ANY_VALUE", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("BIT_AND", "BIGINT", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("BIT_OR", "BIGINT", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("BIT_XOR", "BIGINT", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("STD", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("STDDEV", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("STDDEV_POP", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("STDDEV_SAMP", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("VAR_POP", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("VAR_SAMP", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunction("VARIANCE", "DOUBLE", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunctionIf(
            version >= 56,
            "RANDOM_BYTES",
            "VARBINARY",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 56 && version < 80,
            "JSON_APPEND",
            "VARCHAR",
            tryEvalJsonUtilityFunctions);
        this.AddScalarFunctionIf(
            version >= 80,
            "FORMAT_BYTES",
            "VARCHAR",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "FORMAT_PICO_TIME",
            "VARCHAR",
            tryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "UUID_TO_BIN",
            "VARBINARY",
            tryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "BIN_TO_UUID",
            "VARCHAR",
            tryEvalMySqlQueryUtilityFunction);
    }
}
