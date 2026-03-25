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
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvaluate(fn, currentDialect, evalArg, out result);

        bool TryEvalMySqlUtilityFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryMySqlUtilityFunctionEvaluator.TryEvaluate(
                fn,
                currentDialect,
                emptyUtilityRow,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                out result);

        bool TryEvalMySqlQueryUtilityFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlUtilityFunctionHelper.TryEvalUtilityFunctions(
                fn,
                currentDialect,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                out result);

        bool TryEvalMySqlNumericFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryExecutorBase.TryEvalNumericFunction(fn, evalArg, out result);

        bool TryEvalMySqlDateTimeFunction(
            FunctionCallExpr fn,
            ISqlDialect currentDialect,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlDateTimeFunctionHelper.TryEvalFunctions(
                fn,
                currentDialect,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryCoerceDateTime,
                AstQueryExecutorBase.TryParseExactCachedDateTime,
                out result);

        this.AddScalarFunction(
            "IFNULL",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            "DATABASE",
            "SCHEMA");
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            null,
            "SESSION_USER",
            "CURRENT_USER");
        this.AddScalarFunction(
            "CONNECTION_ID",
            "BIGINT",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction(
            "VERSION",
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            "CHARSET",
            "COLLATION",
            "COERCIBILITY");
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
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
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURTIME",
            "TIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_DATE",
            "DATE",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURRENT_TIME",
            "TIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_TIMESTAMP",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "LOCALTIME",
            "TIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "LOCALTIMESTAMP",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.CallOrIdentifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "NOW",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSDATE",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSTEMDATE",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "UTC_DATE",
            "DATE",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "UTC_TIME",
            "TIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "UTC_TIMESTAMP",
            "DATETIME",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
            SqlScalarFunctionUsageKind.Call,
            SqlTemporalFunctionKind.DateTime);

        this.AddScalarFunctions(
            "DATE",
            AstQueryGeneralDateFunctionEvaluator.TryEvaluate,
            "DATE",
            "TIME",
            "TIMESTAMP");

        this.AddScalarFunctions(
            "BIGINT",
            TryEvalMySqlGeneralScalarFunction,
            "CHAR_LENGTH",
            "CHARACTER_LENGTH");
        this.AddScalarFunctions(
            "INT",
            TryEvalMySqlGeneralScalarFunction,
            "BIT_LENGTH",
            "BIT_COUNT");
        this.AddScalarFunction(
            "BIN",
            "VARCHAR",
            TryEvalMySqlNumericFunction);
        this.AddScalarFunction(
            "CRC32",
            "BIGINT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "CONVERT",
            "VARCHAR",
            AstQueryExecutorBase.TryEvalConvertFunction);
        this.AddScalarFunction(
            "CONV",
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction(
            "DATE_SUB",
            "DATETIME",
            AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "VARCHAR",
            TryEvalMySqlUtilityFunction,
            "ELT",
            "MAKE_SET",
            "EXPORT_SET");
        this.AddScalarFunction(
            "FIND_IN_SET",
            "INT",
            QueryTextSearchFunctionHelper.TryEvalFindInSetFunction);
        this.AddScalarFunction(
            "FROM_BASE64",
            "VARBINARY",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "TO_BASE64",
            "VARCHAR",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "FROM_DAYS",
            "DATE",
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "GET_FORMAT",
            "VARCHAR",
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "INET_ATON",
            "BIGINT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET_NTOA",
            "VARCHAR",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET6_ATON",
            "VARBINARY",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "INET6_NTOA",
            "VARCHAR",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4",
            "INT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4_COMPAT",
            "INT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV4_MAPPED",
            "INT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunction(
            "IS_IPV6",
            "INT",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunctions(
            "VARBINARY",
            TryEvalMySqlUtilityFunction,
            "AES_ENCRYPT",
            "DES_ENCRYPT",
            "ENCODE");
        this.AddScalarFunctions(
            "VARCHAR",
            TryEvalMySqlUtilityFunction,
            "AES_DECRYPT",
            "DES_DECRYPT",
            "DECODE",
            "ENCRYPT");
        this.AddScalarFunction("DEFAULT", "VARCHAR", TryEvalMySqlUtilityFunction);
        this.AddScalarFunction("MEMBER_OF", "BOOLEAN", TryEvalMySqlUtilityFunction);
        this.AddScalarFunctions("VARCHAR", TryEvalMySqlUtilityFunction, "EXTRACTVALUE", "UPDATEXML");
        this.AddScalarFunction("LOG2", "DOUBLE", TryEvalMySqlGeneralScalarFunction);
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
        this.AddScalarFunction("SLEEP", "INT", TryEvalMySqlUtilityFunction);
        this.AddScalarFunction("SUBSTRING_INDEX", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("OCTET_LENGTH", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions("VARBINARY", TryEvalMySqlUtilityFunction, "COMPRESS");
        this.AddScalarFunctions("VARBINARY", TryEvalMySqlUtilityFunction, "UNCOMPRESS");
        this.AddScalarFunction(
            "CONVERT_TZ",
            "DATETIME",
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "TRUNCATE",
            "DECIMAL",
            AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction(
            "UNIX_TIMESTAMP",
            "BIGINT",
            AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "VARCHAR",
            TryEvalMySqlGeneralScalarFunction,
            "SHA",
            "SHA1",
            "SHA2");
        this.AddScalarFunction(
            "UNCOMPRESSED_LENGTH",
            "BIGINT",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "FORMAT",
            "VARCHAR",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "HEX",
            "VARCHAR",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunction(
            "UNHEX",
            "VARBINARY",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunctions(
            "INT",
            TryEvalMySqlUtilityFunction,
            "REGEXP_INSTR",
            "REGEXP_REPLACE",
            "REGEXP_SUBSTR",
            "REGEXP_LIKE");
        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonExtractionFunction,
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");

        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonUtilityFunctions,
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
            "JSON_APPEND",
            "JSON_ARRAY_APPEND",
            "JSON_ARRAY_INSERT",
            "JSON_MERGE",
            "JSON_MERGE_PRESERVE",
            "JSON_MERGE_PATCH");

        this.AddScalarFunctionIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "JSON_TYPE",
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonUtilityFunctions);

        this.AddScalarFunctionsIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "INT",
            AstQueryExecutorBase.TryEvalJsonUtilityFunctions,
            "JSON_VALID",
            "JSON_LENGTH",
            "JSON_CONTAINS",
            "JSON_CONTAINS_PATH",
            "JSON_OVERLAPS");

        this.AddScalarFunctionIf(
            version >= MySqlDialect.JsonExtractMinVersion,
            "JSON_ARRAY",
            "VARCHAR",
            AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvaluate);

        this.AddScalarFunctionIf(version >= 56, "JSON_ARRAYAGG", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);
        this.AddScalarFunctionIf(version >= 56, "JSON_OBJECTAGG", "VARCHAR", body, SqlScalarFunctionUsageKind.Call, null);

        this.AddScalarFunctionIf(
            version >= 80,
            "JSON_STORAGE_SIZE",
            "BIGINT",
            AstQueryExecutorBase.TryEvalJsonUtilityFunctions);

        this.AddScalarFunctionIf(
            version >= 80,
            "JSON_DEPTH",
            "INT",
            AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvaluate);

        this.AddScalarFunctionIf(
            version >= 80,
            "IS_UUID",
            "INT",
            AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvaluate);

        this.AddScalarFunctionIf(
            version >= 80,
            "UUID_SHORT",
            "BIGINT",
            AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "DATETIME",
            AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate,
            "ADDDATE",
            "ADDTIME",
            "LAST_DAY",
            "EOMONTH",
            "SUBTIME");

        this.AddScalarFunction("DATE_ADD", "DATETIME", AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("TIMESTAMPADD", "DATETIME", AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("TRY_CAST", "VARCHAR", body);
        this.AddScalarFunction("DATEDIFF", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("TIMESTAMPDIFF", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("DAY", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("MONTH", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("YEAR", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("HOUR", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("MINUTE", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("SECOND", "INT", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);

        this.AddScalarFunctions(
            "DATE",
            AstQueryGeneralDateFunctionEvaluator.TryEvaluate,
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
            AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate,
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
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "STR_TO_DATE",
            "DATETIME",
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunction(
            "FROM_UNIXTIME",
            "DATETIME",
            TryEvalMySqlDateTimeFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate,
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
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(version >= 56 && version < 80, "JSON_APPEND", "VARCHAR", body);
        this.AddScalarFunctionIf(
            version >= 80,
            "FORMAT_BYTES",
            "VARCHAR",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "FORMAT_PICO_TIME",
            "VARCHAR",
            TryEvalMySqlUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "UUID_TO_BIN",
            "VARBINARY",
            TryEvalMySqlQueryUtilityFunction);
        this.AddScalarFunctionIf(
            version >= 80,
            "BIN_TO_UUID",
            "VARCHAR",
            TryEvalMySqlQueryUtilityFunction);
    }
}
