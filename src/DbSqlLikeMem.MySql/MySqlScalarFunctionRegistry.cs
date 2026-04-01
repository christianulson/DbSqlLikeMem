using DbSqlLikeMem.Models;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterScalarFunctions(int version)
    {
        SqlSharedScalarFunctionRegistry.Register(this);

        var emptyUtilityRow = new AstQueryExecutorBase.EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase));

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

        static bool TryEvalMySqlExtractFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count < 2)
            {
                result = null;
                return false;
            }

            var unitText = fn.Args[0] is RawSqlExpr rawUnit
                ? rawUnit.Sql
                : evalArg(0)?.ToString() ?? string.Empty;
            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            var value = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = unit switch
                {
                    TemporalUnit.Day => dateTime.Day,
                    TemporalUnit.Month => dateTime.Month,
                    TemporalUnit.Year => dateTime.Year,
                    TemporalUnit.Hour => dateTime.Hour,
                    TemporalUnit.Minute => dateTime.Minute,
                    TemporalUnit.Second => dateTime.Second,
                    _ => null
                };
                return true;
            }

            if (AstQueryExecutorBase.TryConvertNumericToDouble(value, out var numeric))
            {
                result = unit switch
                {
                    TemporalUnit.Day => (int)Math.Truncate(numeric),
                    _ => null
                };
                return true;
            }

            result = null;
            return true;
        }

        static bool TryEvalMySqlDatePartFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count < 1)
            {
                result = null;
                return false;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = fn.Name.ToUpperInvariant() switch
            {
                "DAY" => dateTime.Day,
                "MONTH" => dateTime.Month,
                "YEAR" => dateTime.Year,
                "HOUR" => dateTime.Hour,
                "MINUTE" => dateTime.Minute,
                "SECOND" => dateTime.Second,
                _ => null
            };

            return true;
        }

        static bool TryEvalMySqlTemporalDiffFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var name = fn.Name.ToUpperInvariant();
            if (name == "DATEDIFF")
            {
                if (fn.Args.Count < 2)
                {
                    result = null;
                    return false;
                }

                var startValue = evalArg(0);
                var endValue = evalArg(1);
                if (AstQueryExecutorBase.IsNullish(startValue) || AstQueryExecutorBase.IsNullish(endValue)
                    || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var start1)
                    || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var end1))
                {
                    result = null;
                    return true;
                }

                result = (int)(start1.Date - end1.Date).TotalDays;
                return true;
            }

            if (fn.Args.Count < 3)
            {
                result = null;
                return false;
            }

            var unitText = fn.Args[0] is RawSqlExpr rawUnit
                ? rawUnit.Sql
                : evalArg(0)?.ToString() ?? string.Empty;
            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            var leftValue = evalArg(1);
            var rightValue = evalArg(2);
            if (AstQueryExecutorBase.IsNullish(leftValue) || AstQueryExecutorBase.IsNullish(rightValue)
                || !AstQueryExecutorBase.TryCoerceDateTime(leftValue, out var start)
                || !AstQueryExecutorBase.TryCoerceDateTime(rightValue, out var end))
            {
                result = null;
                return true;
            }

            result = name switch
            {
                "TIMESTAMPADD" => AstQueryExecutorBase.ApplyDateDelta(
                    end,
                    unit,
                    Convert.ToInt32(Convert.ToDecimal(leftValue, CultureInfo.InvariantCulture))),
                "TIMESTAMPDIFF" => unit switch
                {
                    TemporalUnit.Year => end.Year - start.Year,
                    TemporalUnit.Month => (end.Year - start.Year) * 12 + end.Month - start.Month,
                    TemporalUnit.Day => (int)(end.Date - start.Date).TotalDays,
                    TemporalUnit.Hour => (int)(end - start).TotalHours,
                    TemporalUnit.Minute => (int)(end - start).TotalMinutes,
                    TemporalUnit.Second => (int)(end - start).TotalSeconds,
                    _ => null
                },
                _ => null
            };
            return true;
        }

        bool TryEvalMySqlFindInSetFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return QueryTextSearchFunctionHelper.TryEvalFindInSetFunction(fn, evalArg, out result);
        }

        static bool TryEvalFoundRowsFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = evalArg;
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            result = context.Connection.GetLastFoundRows();
            return true;
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

        var tryEvalMySqlUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlUtilityFunction;
        var tryEvalMySqlQueryUtilityFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlQueryUtilityFunction;
        var tryEvalMySqlDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlDateTimeFunction;
        var tryEvalMySqlConversionAndMetadataFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate;
        var tryEvalConvertFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate;
        var tryEvalDateFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlDatePartFunction;
        var tryEvalMySqlDateFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlDateFunctionEvaluator.TryEvaluate;
        var tryEvalDateTimeFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlDateTimeFunctionEvaluator.TryEvaluate;
        var tryEvalExtractFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlExtractFunction;
        var tryEvalJsonUtilityFunctions = (AstQueryGeneralScalarFunctionHandler)AstQueryMySqlJsonFunctionEvaluator.TryEvaluate;
        var tryEvalJsonExtractionFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction;
        var tryEvalJsonUnquoteFunction = (AstQueryGeneralScalarFunctionHandler)AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction;
        var tryEvalFindInSetFunction = (AstQueryGeneralScalarFunctionHandler)TryEvalMySqlFindInSetFunction;

        this.AddScalarFunction(
            "IFNULL",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        this.AddScalarFunctions(
            DbFunctionDef.CreateScalar("IF", "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "IF");
        this.AddScalarFunctions(
            DbFunctionDef.CreateScalar("IIF", "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "IIF");
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("SOUNDEX", "VARCHAR") with
            {
                AstExecutor = TryEvalMySqlUtilityFunction
            });
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
            executionHandler: QueryMariaDbFunctionHelper.TryEvalFunctions);
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
            DbFunctionDef.CreateScalar("FOUND_ROWS", "BIGINT") with
            {
                AstExecutor = TryEvalFoundRowsFunction
            },
            "FOUND_ROWS",
            "ROW_COUNT");
        this.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.VALUES, "VARCHAR"));
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("LAST_INSERT_ID", "BIGINT") with
            {
                AstExecutor = TryEvalMySqlUtilityFunction
            });
        var groupConcatFunction = DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
        {
            IsStringAggregate = true
        };
        this.AddScalarFunction(groupConcatFunction);

        this.AddScalarFunction(new DbFunctionDef(SqlConst.SUM, null, DbFunctionCapability.Aggregate)
        {
            PromotesIntegralInputsToDecimal = true
        });
        this.AddScalarFunction(new DbFunctionDef(SqlConst.AVG, null, DbFunctionCapability.Aggregate)
        {
            PromotesIntegralInputsToDecimal = true
        });

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
            "DATETIME",
            tryEvalMySqlConversionAndMetadataFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            SqlTemporalFunctionKind.DateTime);
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
        this.AddScalarFunction(
            "DATETIME",
            "DATETIME",
            tryEvalDateFunction);

        this.AddScalarFunctions(
            "BIGINT",
            AstQuerySharedTextFunctionEvaluator.TryEvaluate,
            "CHAR_LENGTH",
            "CHARACTER_LENGTH");
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
            DbFunctionDef.CreateScalar("DATE_SUB", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
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
        this.AddScalarFunction("NAME_CONST", "VARCHAR", tryEvalMySqlUtilityFunction);
        if (version >= 80)
            this.AddScalarFunction(
                "MEMBER_OF",
                "BOOLEAN",
                tryEvalMySqlUtilityFunction);
        this.AddScalarFunctions("VARCHAR", tryEvalMySqlUtilityFunction, "EXTRACTVALUE", "UPDATEXML");
        this.AddScalarFunction("LOG2", "DOUBLE", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("OCT", "VARCHAR", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("ORD", "INT", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("BIT_COUNT", "INT", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("SLEEP", "INT", tryEvalMySqlUtilityFunction);
        this.AddScalarFunction("SUBSTRING_INDEX", "VARCHAR", tryEvalMySqlUtilityFunction);
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
            tryEvalMySqlUtilityFunction,
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
            "WEIGHT_STRING",
            "VARBINARY",
            executionHandler: QueryMariaDbFunctionHelper.TryEvalFunctions);
        if (version >= 80)
            this.AddScalarFunctions(
                "INT",
                tryEvalMySqlUtilityFunction,
                "REGEXP_INSTR",
                "REGEXP_REPLACE",
                "REGEXP_SUBSTR",
                "REGEXP_LIKE");
        if (version >= MySqlDialect.JsonFunctionsMinVersion)
            this.AddScalarFunctions(
                "VARCHAR",
                tryEvalJsonExtractionFunction,
                "JSON_EXTRACT",
                "JSON_QUERY",
                "JSON_VALUE");

        if (version >= MySqlDialect.JsonArrayFunctionsMinVersion)
        {
            this.AddScalarFunction(
                "JSON_UNQUOTE",
                "VARCHAR",
                tryEvalJsonUnquoteFunction);

            this.AddScalarFunctions(
                "VARCHAR",
                tryEvalJsonUtilityFunctions,
                "JSON_OBJECT",
                "JSON_QUOTE",
                "JSON_PRETTY",
                "JSON_KEYS",
                "JSON_SET",
                "JSON_REMOVE",
                "JSON_SEARCH",
                "JSON_INSERT",
                "JSON_REPLACE",
                "JSON_ARRAY_APPEND",
                "JSON_ARRAY_INSERT",
                "JSON_MERGE",
                "JSON_MERGE_PRESERVE",
                "JSON_MERGE_PATCH");
        }

        if (version >= MySqlDialect.JsonFunctionsMinVersion)
            this.AddScalarFunction(
                "JSON_TYPE",
                "VARCHAR",
                tryEvalJsonUtilityFunctions);

        this.AddScalarFunction(
            "QUOTE",
            "VARCHAR",
            tryEvalMySqlUtilityFunction);

        if (version >= MySqlDialect.JsonOverlapsMinVersion)
            this.AddScalarFunctions(
                "INT",
                tryEvalJsonUtilityFunctions,
                "JSON_VALID",
                "JSON_LENGTH",
                "JSON_CONTAINS",
                "JSON_CONTAINS_PATH",
                "JSON_OVERLAPS");

        if (version >= MySqlDialect.JsonFunctionsMinVersion)
            this.AddScalarFunction(
                "JSON_ARRAY",
                "VARCHAR",
                executionHandler: AstQueryJsonArrayFunctionEvaluator.TryEvalJsonArrayFunction);

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
                executionHandler: AstQueryMySqlSystemAndJsonFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction(
            "STRCMP",
            "INT",
            executionHandler: tryEvalMySqlQueryUtilityFunction);

        if (version >= 80)
            this.AddScalarFunction(
                "IS_UUID",
                "INT",
                executionHandler: tryEvalMySqlUtilityFunction);

        if (version >= 80)
            this.AddScalarFunction(
                "UUID_SHORT",
                "BIGINT",
                executionHandler: AstQueryMySqlSystemAndJsonFunctionEvaluator.TryEvaluate);
        if (version >= 80)
            this.AddScalarFunction(
                "GROUPING",
                "INT",
                executionHandler: AstQueryGroupingFunctionEvaluator.TryEvaluate);
        if (version >= 80)
            this.AddScalarFunction(
                "GROUPING_ID",
                "INT",
                executionHandler: AstQueryGroupingFunctionEvaluator.TryEvaluate);
        this.AddScalarFunctions(
            "DATETIME",
            AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate,
            "ADDDATE",
            "ADDTIME",
            "SUBDATE",
            "SUBTIME");
        this.AddScalarFunction("DATE", "DATE", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("DATETIME", "DATETIME", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("TIME", "TIME", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);
        this.AddScalarFunction("TIMESTAMP", "DATETIME", AstQueryGeneralDateFunctionEvaluator.TryEvaluate);

        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPADD", "DATETIME") with
            {
                AstExecutor = TryEvalMySqlTemporalDiffFunction
            });
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("EXTRACT", "INT") with
            {
                AstExecutor = tryEvalExtractFunction
            });
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR") with
            {
                AstExecutor = AstQueryCastConversionFamilyEvaluator.TryEvalTryCastLikeFunction
            });
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("CAST", "VARCHAR", AstQueryMySqlCastFunctionEvaluator.TryEvaluate));
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATEDIFF", "INT") with
            {
                AstExecutor = TryEvalMySqlTemporalDiffFunction
            });
        this.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPDIFF", "INT") with
            {
                AstExecutor = TryEvalMySqlTemporalDiffFunction
            });
        this.AddScalarFunctions(
            "INT",
            tryEvalDateFunction,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            "DAY",
            "MONTH",
            "YEAR",
            "HOUR",
            "MINUTE",
            "SECOND");

        this.AddScalarFunctions(
            "DATE",
            tryEvalMySqlDateFunction,
            "MAKEDATE",
            "MAKETIME",
            "MICROSECOND",
            "MONTHNAME",
            "PERIOD_ADD",
            "PERIOD_DIFF",
            "LAST_DAY",
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
