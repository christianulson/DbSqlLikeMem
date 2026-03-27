using DbSqlLikeMem;
using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterScalarFunctions(int version)
    {
        SqlSharedScalarFunctionRegistry.Register(this);

        var emptyUtilityRow = new AstQueryExecutorBase.EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase));

        static bool TryEvalMySqlGeneralScalarFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction(context, fn, evalArg, out result);

        bool TryEvalMySqlUtilityFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryMySqlUtilityFunctionEvaluator.TryEvaluate(
                context,
                fn,
                emptyUtilityRow,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                out result);

        bool TryEvalMySqlQueryUtilityFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlUtilityFunctionHelper.TryEvalUtilityFunctions(
                context, fn,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                out result);

        bool TryEvalMySqlNumericFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralScalarFunctionEvaluator.TryEvalNumericFunction(context, fn, evalArg, out result);

        bool TryEvalMySqlDateTimeFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => QueryMySqlDateTimeFunctionHelper.TryEvalFunctions(
                context, fn,
                evalArg,
                AstQueryExecutorBase.TryConvertNumericToDouble,
                AstQueryExecutorBase.TryConvertNumericToInt64,
                AstQueryExecutorBase.TryCoerceDateTime,
                AstQueryExecutorBase.TryParseExactCachedDateTime,
                out result);

        bool TryEvalMySqlFindInSetFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return QueryTextSearchFunctionHelper.TryEvalFindInSetFunction(fn, evalArg, out result);
        }

        static bool TryEvalBenchmarkFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("BENCHMARK() espera contagem e expressao.");

            var countValue = evalArg(0);
            if (countValue is null or DBNull)
            {
                result = null;
                return true;
            }

            if (!AstQueryExecutorBase.TryConvertNumericToInt64(countValue, out var count) || count <= 0)
            {
                result = 0;
                return true;
            }

            for (var i = 0L; i < count; i++)
                _ = evalArg(1);

            result = 0;
            return true;
        }

        static bool TryEvalNoopSessionContextFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            _ = fn;
            _ = evalArg;
            result = null;
            return false;
        }

        static bool TryEvalNoopGeneralSystemAndJsonFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
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
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
            => generalSystemAndJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

        var tryEvalMySqlGeneralScalarFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlGeneralScalarFunction;
        var tryEvalMySqlUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlUtilityFunction;
        var tryEvalMySqlQueryUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlQueryUtilityFunction;
        var tryEvalMySqlDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlDateTimeFunction;
        var tryEvalMySqlNumericFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlNumericFunction;
        var tryEvalMySqlGeneralSystemAndJsonFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlGeneralSystemAndJsonFunction;
        var tryEvalMySqlConversionAndMetadataFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate;
        var tryEvalConvertFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate;
        var tryEvalDateFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralDateFunctionEvaluator.TryEvaluate;
        var tryEvalDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate;
        var tryEvalJsonUtilityFunctions = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonUtilityFunctions;
        var tryEvalJsonExtractionFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonExtractionFunction;
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
        this.AddScalarFunction(
            "BENCHMARK",
            "INT",
            executionHandler: TryEvalBenchmarkFunction);
        this.AddScalarFunction(
            "FIELD",
            "INT",
            executionHandler: tryEvalMySqlGeneralScalarFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
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
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            "USER",
            "SYSTEM_USER");
        this.AddScalarFunctions(
            DbFunctionDef.CreateScalar("FOUND_ROWS", "BIGINT"),
            "FOUND_ROWS",
            "ROW_COUNT");
        this.AddScalarFunction(DbFunctionDef.CreateScalar("LAST_INSERT_ID", "BIGINT"));
        var groupConcatFunction = DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
        {
            IsStringAggregate = true
        };
        this.AddScalarFunction(groupConcatFunction);

        this.AddScalarFunction(
            "CURDATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURTIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_DATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "CURRENT_TIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "CURRENT_TIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "LOCALTIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "LOCALTIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "NOW",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSDATE",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "SYSTEMDATE",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.DateTime);
        this.AddScalarFunction(
            "UTC_DATE",
            "DATE",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.Date);
        this.AddScalarFunction(
            "UTC_TIME",
            "TIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
            SqlTemporalFunctionKind.Time);
        this.AddScalarFunction(
            "UTC_TIMESTAMP",
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call,
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
        this.AddScalarFunction(DbFunctionDef.CreateScalar("DATE_SUB", "DATETIME"));
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
        if (version < 80)
            this.AddScalarFunctions(
                "VARBINARY",
                tryEvalMySqlUtilityFunction,
                "DES_ENCRYPT",
                "ENCODE");
        this.AddScalarFunctions(
            "VARCHAR",
            tryEvalMySqlUtilityFunction,
            "AES_DECRYPT");
        if (version < 80)
            this.AddScalarFunctions(
                "VARCHAR",
                tryEvalMySqlUtilityFunction,
                "DES_DECRYPT",
                "DECODE",
                "ENCRYPT");
        this.AddScalarFunction("DEFAULT", "VARCHAR", tryEvalMySqlUtilityFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "MEMBER_OF",
                "BOOLEAN",
                tryEvalMySqlUtilityFunction);
        this.AddScalarFunctions("VARCHAR", tryEvalMySqlUtilityFunction, "EXTRACTVALUE", "UPDATEXML");
        this.AddScalarFunction("LOG2", "DOUBLE", tryEvalMySqlGeneralScalarFunction);
        this.AddScalarFunction("OCT", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
        this.AddScalarFunction("ORD", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
        this.AddScalarFunctions(
            "VARCHAR",
            AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            "GREATEST",
            "LEAST");
        this.AddScalarFunction("POSITION", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
        this.AddScalarFunction("RPAD", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
        this.AddScalarFunction("SLEEP", "INT", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("SUBSTRING_INDEX", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
        this.AddScalarFunction("OCTET_LENGTH", "INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction);
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
        this.AddScalarFunction(
            "WEIGHT_STRING",
            "VARBINARY",
            executionHandler: global::DbSqlLikeMem.QueryMariaDbFunctionHelper.TryEvalFunctions);
        if (version >= 80)
            this.AddScalarFunctions(
                "INT",
                tryEvalMySqlUtilityFunction,
                "REGEXP_INSTR",
                "REGEXP_REPLACE",
                "REGEXP_SUBSTR",
                "REGEXP_LIKE");
        if (version >= MySqlDialect.JsonArrowOperatorsMinVersion)
            this.AddScalarFunctions(
                "VARCHAR",
                tryEvalJsonExtractionFunction,
                "JSON_EXTRACT",
                "JSON_QUERY",
                "JSON_VALUE");

        if (version >= MySqlDialect.JsonArrowOperatorsMinVersion)
            this.AddScalarFunctions(
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

        if (version >= MySqlDialect.JsonArrowOperatorsMinVersion)
            this.AddScalarFunction(
                "JSON_TYPE",
                "VARCHAR",
                tryEvalJsonUtilityFunctions);

        if (version >= MySqlDialect.JsonArrowOperatorsMinVersion)
            this.AddScalarFunctions(
                "INT",
                tryEvalJsonUtilityFunctions,
                "JSON_VALID",
                "JSON_LENGTH",
                "JSON_CONTAINS",
                "JSON_CONTAINS_PATH",
                "JSON_OVERLAPS");

        if (version >= MySqlDialect.JsonArrowOperatorsMinVersion)
            this.AddScalarFunction(
                "JSON_ARRAY",
                "VARCHAR",
                executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        if (version >= 56)
            this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_ARRAYAGG, "VARCHAR"));
        if (version >= 56)
            this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.JSON_OBJECTAGG, "VARCHAR"));

        if (version >= 80)
            this.AddScalarFunction(
                "JSON_STORAGE_SIZE",
                "BIGINT",
                tryEvalJsonUtilityFunctions);

        if (version >= 80)
            this.AddScalarFunction(
                "JSON_DEPTH",
                "INT",
                executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        if (version >= 80)
            this.AddScalarFunction(
                "IS_UUID",
                "INT",
                executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);

        if (version >= 80)
            this.AddScalarFunction(
                "UUID_SHORT",
                "BIGINT",
                executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "GROUPING",
                "INT",
                executionHandler: tryEvalMySqlGeneralSystemAndJsonFunction);
        if (version >= 80)
            this.AddScalarFunction(
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

        this.AddScalarFunction(DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("TIMESTAMPADD", "DATETIME"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR"));
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
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.ANY_VALUE, "VARCHAR"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.BIT_AND, "BIGINT"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.BIT_OR, "BIGINT"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.BIT_XOR, "BIGINT"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("STD", "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("STDDEV", "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("STDDEV_POP", "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar("STDDEV_SAMP", "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.VAR_POP, "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.VAR_SAMP, "DOUBLE"));
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.VARIANCE, "DOUBLE"));
        if (version >= 56)
            this.AddScalarFunction(
                "RANDOM_BYTES",
                "VARBINARY",
                tryEvalMySqlUtilityFunction);
        if (version >= 56 && version < 80)
            this.AddScalarFunction(
                "JSON_APPEND",
                "VARCHAR",
                tryEvalJsonUtilityFunctions);
        if (version >= 80)
            this.AddScalarFunction(
                "FORMAT_BYTES",
                "VARCHAR",
                tryEvalMySqlUtilityFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "FORMAT_PICO_TIME",
                "VARCHAR",
                tryEvalMySqlUtilityFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "UUID_TO_BIN",
                "VARBINARY",
                tryEvalMySqlQueryUtilityFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "BIN_TO_UUID",
                "VARCHAR",
                tryEvalMySqlQueryUtilityFunction);
    }
}
