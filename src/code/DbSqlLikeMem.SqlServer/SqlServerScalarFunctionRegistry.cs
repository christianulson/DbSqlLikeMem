using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerScalarFunctionRegistry
{
    internal static void Register(SqlServerDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        if (version < SqlServerDialect.TranslateMinVersion)
            dialect.Functions.Remove("TRANSLATE");

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
        RegisterScalarFunctions(dialect, version, TryEvalSqlServerUtilityFunction, TryEvalSqlServerSessionContextFunction, utilityEvaluator);
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
        {
            static bool TryEvalSqlServerEomonthFunction(
                QueryExecutionContext context,
                FunctionCallExpr fn,
                Func<int, object?> evalArg,
                out object? result)
            {
                _ = context;
                return AstQuerySqlServerCompatibilityFunctionEvaluator.TryEvalEomonthFunction(fn, evalArg, out result);
            }

            dialect.AddScalarFunction("EOMONTH", "DATE", TryEvalSqlServerEomonthFunction);
        }

        static bool TryEvalSqlServerDateNameFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (fn.Args.Count < 2)
            {
                result = null;
                return true;
            }

            var unitText = fn.Args[0] is RawSqlExpr rawUnit
                ? rawUnit.Sql
                : evalArg(0)?.ToString() ?? string.Empty;
            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            var value = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value)
                || unit == AstQueryExecutorBase.TemporalUnit.Unknown
                || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = unit switch
            {
                AstQueryExecutorBase.TemporalUnit.Year => dateTime.Year.ToString(CultureInfo.InvariantCulture),
                AstQueryExecutorBase.TemporalUnit.Month => dateTime.ToString("MMMM", CultureInfo.InvariantCulture),
                AstQueryExecutorBase.TemporalUnit.Day => dateTime.Day.ToString(CultureInfo.InvariantCulture),
                AstQueryExecutorBase.TemporalUnit.Hour => dateTime.Hour.ToString(CultureInfo.InvariantCulture),
                AstQueryExecutorBase.TemporalUnit.Minute => dateTime.Minute.ToString(CultureInfo.InvariantCulture),
                AstQueryExecutorBase.TemporalUnit.Second => dateTime.Second.ToString(CultureInfo.InvariantCulture),
                _ => null
            };
            return true;
        }

        static bool TryEvalSqlServerDatePartFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var name = fn.Name.ToUpperInvariant();
            var unitText = name == "DATEPART"
                ? fn.Args[0] is RawSqlExpr rawUnit
                    ? rawUnit.Sql
                    : evalArg(0)?.ToString() ?? string.Empty
                : name;
            var valueIndex = name == "DATEPART" ? 1 : 0;
            if ((name == "DATEPART" && fn.Args.Count < 2)
                || (name != "DATEPART" && fn.Args.Count == 0))
            {
                result = null;
                return true;
            }

            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            var value = evalArg(valueIndex);
            if (AstQueryExecutorBase.IsNullish(value)
                || unit == AstQueryExecutorBase.TemporalUnit.Unknown
                || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = unit switch
            {
                AstQueryExecutorBase.TemporalUnit.Year => dateTime.Year,
                AstQueryExecutorBase.TemporalUnit.Month => dateTime.Month,
                AstQueryExecutorBase.TemporalUnit.Day => dateTime.Day,
                AstQueryExecutorBase.TemporalUnit.Hour => dateTime.Hour,
                AstQueryExecutorBase.TemporalUnit.Minute => dateTime.Minute,
                AstQueryExecutorBase.TemporalUnit.Second => dateTime.Second,
                _ => null
            };
            return true;
        }

        static bool TryEvalSqlServerDateDiffFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (fn.Args.Count < 3)
            {
                result = null;
                return true;
            }

            var unitText = fn.Args[0] is RawSqlExpr rawUnit
                ? rawUnit.Sql
                : evalArg(0)?.ToString() ?? string.Empty;
            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            var startValue = evalArg(1);
            var endValue = evalArg(2);
            if (AstQueryExecutorBase.IsNullish(startValue)
                || AstQueryExecutorBase.IsNullish(endValue)
                || unit == AstQueryExecutorBase.TemporalUnit.Unknown
                || !AstQueryExecutorBase.TryCoerceDateTime(startValue, out var startDateTime)
                || !AstQueryExecutorBase.TryCoerceDateTime(endValue, out var endDateTime))
            {
                result = null;
                return true;
            }

            var difference = GetTemporalDifference(startDateTime, endDateTime, unit);
            result = fn.Name.Equals("DATEDIFF_BIG", StringComparison.OrdinalIgnoreCase)
                ? (long)difference
                : difference;
            return true;
        }

        static int GetTemporalDifference(DateTime start, DateTime end, AstQueryExecutorBase.TemporalUnit unit)
            => unit switch
            {
                AstQueryExecutorBase.TemporalUnit.Year => end.Year - start.Year,
                AstQueryExecutorBase.TemporalUnit.Month => DiffMonths(start, end),
                AstQueryExecutorBase.TemporalUnit.Day => (int)(end.Date - start.Date).TotalDays,
                AstQueryExecutorBase.TemporalUnit.Hour => (int)(end - start).TotalHours,
                AstQueryExecutorBase.TemporalUnit.Minute => (int)(end - start).TotalMinutes,
                AstQueryExecutorBase.TemporalUnit.Second => (int)(end - start).TotalSeconds,
                _ => 0
            };

        static int DiffMonths(DateTime start, DateTime end)
            => (end.Year - start.Year) * 12 + end.Month - start.Month;

        static bool TryEvalSqlServerDateAddFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count < 3)
            {
                result = null;
                return true;
            }

            var baseValue = evalArg(2);
            if (AstQueryExecutorBase.IsNullish(baseValue)
                || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            var amountValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(amountValue))
            {
                result = null;
                return true;
            }

            var unitText = fn.Args[0] is RawSqlExpr rawUnit
                ? rawUnit.Sql
                : evalArg(0)?.ToString() ?? string.Empty;
            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
            if (unit == AstQueryExecutorBase.TemporalUnit.Unknown)
            {
                result = dateTime;
                return true;
            }

            var amount = Convert.ToInt32(amountValue.ToDec(), CultureInfo.InvariantCulture);
            result = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
            return true;
        }

        dialect.AddScalarFunction(CreateScalarFunctionDef("DATEDIFF", "INT", TryEvalSqlServerDateDiffFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DATENAME", "VARCHAR", TryEvalSqlServerDateNameFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DATEPART", "INT", TryEvalSqlServerDatePartFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DAY", "INT", TryEvalSqlServerDatePartFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("MONTH", "INT", TryEvalSqlServerDatePartFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef(SqlConst.YEAR, "INT", TryEvalSqlServerDatePartFunction));

        if (version >= SqlServerDialect.DateDiffBigMinVersion)
            dialect.AddScalarFunction(CreateScalarFunctionDef("DATEDIFF_BIG", "BIGINT", TryEvalSqlServerDateDiffFunction));

        if (version >= SqlServerDialect.FormatMinVersion)
            dialect.AddScalarFunction(CreateScalarFunctionDef("FORMAT", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatFunction));

        if (version >= SqlServerDialect.ParseMinVersion)
        {
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("PARSE", "VARCHAR", AstQueryCastConversionFamilyEvaluator.TryEvalParseLikeFunction),
                "PARSE",
                "TRY_PARSE");

            dialect.AddScalarFunction(
                DbFunctionDef.CreateScalar("TRY_PARSE", "VARCHAR", AstQueryCastConversionFamilyEvaluator.TryEvalTryParseLikeFunction));
        }

        if (version >= SqlServerDialect.TryCastMinVersion)
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar("CAST", "VARCHAR", AstQueryCastConversionFamilyEvaluator.TryEvalCastLikeFunction));

        if (version >= SqlServerDialect.TryCastMinVersion)
            dialect.AddScalarFunction(
                DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR", AstQueryCastConversionFamilyEvaluator.TryEvalTryCastLikeFunction));

        if (version >= SqlServerDialect.TryConvertMinVersion)
            dialect.AddScalarFunction(
                DbFunctionDef.CreateScalar("TRY_CONVERT", "VARCHAR", AstQueryCastConversionFamilyEvaluator.TryEvalTryConvertLikeFunction));

        dialect.AddScalarFunction(CreateScalarFunctionDef("DATEADD", "DATETIME", TryEvalSqlServerDateAddFunction));

        dialect.AddScalarFunction(
            CreateScalarFunctionDef("NEXT_VALUE_FOR", "BIGINT", TryEvalSqlServerSequenceFunction));
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("PREVIOUS_VALUE_FOR", "BIGINT"),
            "PREVIOUS_VALUE_FOR");
    }

    private static void RegisterMetadataFunctions(SqlServerDialect dialect, int version)
    {
        static bool TryEvalSqlServerContextInfoFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = evalArg;
            result = context.Connection.GetContextInfo();
            return true;
        }

        static bool TryEvalSqlServerConnectionPropertyFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return AstQuerySqlServerSessionFunctionEvaluator.TryEvalSqlServerConnectionPropertyFunction(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerDatabaseMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerDatabaseFunctionEvaluator(
                resolveDatabaseProperty: context.TryResolveSqlServerDatabaseProperty,
                resolveDatabasePrincipalId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerDatabasePrincipalId,
                resolveColumnProperty: context.TryResolveSqlServerColumnProperty,
                resolveColumnLength: context.TryResolveSqlServerColumnLength,
                resolveColumnName: context.TryResolveSqlServerColumnName,
                resolveObjectId: context.TryResolveSqlServerObjectId,
                resolveObjectProperty: context.TryResolveSqlServerObjectProperty,
                resolveObjectName: context.TryResolveSqlServerObjectName,
                resolveObjectSchemaName: context.TryResolveSqlServerObjectSchemaName,
                resolveTypeProperty: AstQuerySqlServerResolutionHelper.TryResolveSqlServerTypeProperty,
                getDatabaseName: () => context.Connection.Database);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerSessionMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerSessionFunctionEvaluator(
                getDialect: () => context.Dialect,
                getContextInfo: context.Connection.GetContextInfo,
                hasActiveTransaction: () => context.Connection.HasActiveTransaction || context.HasActiveTransaction,
                tryResolveSqlServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerRoleMembership,
                tryResolveSqlServerServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerServerRoleMembership);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }

        static bool TryEvalSqlServerIdentityMetadataFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            var evaluator = new AstQuerySqlServerIdentityFunctionEvaluator(
                getDialect: () => context.Dialect,
                getLastInsertId: context.Connection.GetLastInsertId,
                resolveSystemTypeId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeId,
                resolveSystemTypeName: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeName);

            return evaluator.TryEvaluate(fn, evalArg, out result);
        }

        dialect.AddScalarFunction("APP_NAME", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalAppNameFunction);
        dialect.AddScalarFunction("GETANSINULL", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalGetAnsiNullFunction);
        dialect.AddScalarFunction("HOST_ID", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostIdFunction);
        dialect.AddScalarFunction("HOST_NAME", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalHostNameFunction);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("APPLOCK_MODE", "VARCHAR"),
            "APPLOCK_MODE",
            "APPLOCK_TEST",
            "ASSEMBLYPROPERTY",
            "CERTENCODED",
            "CERTPRIVATEKEY",
            "CURRENT_REQUEST_ID",
            "CURRENT_TRANSACTION_ID",
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

        dialect.AddScalarFunction(
            "CONNECTIONPROPERTY",
            "VARCHAR",
            TryEvalSqlServerConnectionPropertyFunction);

        dialect.AddScalarFunction(CreateScalarFunctionDef("DATABASEPROPERTYEX", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DATABASE_PRINCIPAL_ID", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("COLUMNPROPERTY", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("COL_LENGTH", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("COL_NAME", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DB_ID", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("DB_NAME", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("OBJECT_ID", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("OBJECTPROPERTY", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("OBJECTPROPERTYEX", "INT", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("OBJECT_NAME", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("OBJECT_SCHEMA_NAME", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ORIGINAL_DB_NAME", "VARCHAR", TryEvalSqlServerDatabaseMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("TYPEPROPERTY", "INT", TryEvalSqlServerDatabaseMetadataFunction));

        dialect.AddScalarFunction(CreateScalarFunctionDef("CURRENT_REQUEST_ID", "INT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("CURRENT_TRANSACTION_ID", "BIGINT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("IS_MEMBER", "INT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("IS_ROLEMEMBER", "INT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("IS_SRVROLEMEMBER", "INT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("ORIGINAL_LOGIN", "VARCHAR", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SESSION_ID", "INT", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SERVERPROPERTY", "VARCHAR", TryEvalSqlServerSessionMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("XACT_STATE", "INT", TryEvalSqlServerSessionMetadataFunction));

        dialect.AddScalarFunction(CreateScalarFunctionDef("SCHEMA_ID", "INT", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SCHEMA_NAME", "VARCHAR", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SUSER_ID", "INT", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SUSER_NAME", "VARCHAR", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SUSER_SID", "VARBINARY", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("SUSER_SNAME", "VARCHAR", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("TYPE_ID", "INT", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("TYPE_NAME", "VARCHAR", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("USER_ID", "INT", TryEvalSqlServerIdentityMetadataFunction));
        dialect.AddScalarFunction(CreateScalarFunctionDef("USER_NAME", "VARCHAR", TryEvalSqlServerIdentityMetadataFunction));

        dialect.AddScalarFunction(CreateScalarFunctionDef("CONTEXT_INFO", "VARBINARY", TryEvalSqlServerContextInfoFunction));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("GET_FILESTREAM_TRANSACTION_CONTEXT", "VARBINARY"));
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
                "SCOPE_IDENTITY",
                "BIGINT",
                TryEvalSqlServerScopeIdentityFunction));

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
        AstQueryGeneralScalarFunctionHandler tryEvalSqlServerSessionContextFunction,
        AstQuerySqlServerUtilityFunctionEvaluator utilityEvaluator)
    {
        static bool TryEvalSqlServerAtn2Function(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            if (fn.Args.Count < 2)
            {
                result = null;
                return true;
            }

            var yValue = evalArg(0);
            var xValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(yValue)
                || AstQueryExecutorBase.IsNullish(xValue))
            {
                result = null;
                return true;
            }

            if (!AstQueryExecutorBase.TryConvertNumericToDouble(yValue, out var y)
                || !AstQueryExecutorBase.TryConvertNumericToDouble(xValue, out var x))
            {
                result = null;
                return true;
            }

            result = Math.Atan2(y, x);
            return true;
        }

        dialect.AddScalarFunction("CHARINDEX", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCharIndexFunction);

        dialect.AddScalarFunctions("INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerChecksumFunction,
            "CHECKSUM",
            "BINARY_CHECKSUM");

        dialect.AddScalarFunction(CreateScalarFunctionDef("ATN2", "DOUBLE", TryEvalSqlServerAtn2Function));

        dialect.AddScalarFunction("DATALENGTH", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDataLengthFunction);
        dialect.AddScalarFunction("LEN", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions("INT", AstQueryGroupingFunctionEvaluator.TryEvaluate,
            "GROUPING",
            "GROUPING_ID");

        dialect.AddScalarFunction("ISDATE", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsDateFunction);

        if (version >= SqlServerDialect.JsonFunctionsMinVersion)
            dialect.AddScalarFunction("ISJSON", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsJsonFunction);

        dialect.AddScalarFunction("ISNUMERIC", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalIsNumericFunction);

        dialect.AddScalarFunction(CreateScalarFunctionDef("ROWCOUNT", "INT", TryEvalSqlServerRowCountFunction));

        dialect.AddScalarFunction(CreateScalarFunctionDef("ROWCOUNT_BIG", "BIGINT", TryEvalSqlServerRowCountBigFunction));

        dialect.AddScalarFunction("FORMATMESSAGE", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerFormatMessageFunction);

        dialect.AddScalarFunctions("VARCHAR", AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate,
            "QUOTENAME",
            "REPLICATE",
            "STUFF",
            "PARSENAME");

        dialect.AddScalarFunction("LEN", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalLenFunction);

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
            dialect.AddScalarFunction("JSON_MODIFY", "VARCHAR",
                static (QueryExecutionContext context, FunctionCallExpr fn, Func<int, object?> evalArg, out object? result)
                    => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerJsonModifyFunction(context, fn, evalArg, out result));
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

    private static bool TryEvalSqlServerTryConvertFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.SupportsTryConvertFunction)
            throw SqlUnsupported.NotSupported(context.Dialect, "TRY_CONVERT");

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var type = fn.Args[0] is RawSqlExpr typeRaw ? typeRaw.Sql : (evalArg(0)?.ToString() ?? string.Empty);
        type = type.Trim();
        var value = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (context.Dialect.IsIntegerCastTypeName(type))
            {
                if (value is long longValue)
                {
                    result = (int)longValue;
                    return true;
                }

                if (value is int intValue)
                {
                    result = intValue;
                    return true;
                }

                if (value is decimal decimalValue)
                {
                    result = (int)decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (int.TryParse(text, out var intParsed))
                {
                    result = intParsed;
                    return true;
                }

                if (long.TryParse(text, out var longParsed))
                {
                    result = (int)longParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (value is decimal decimalValue)
                {
                    result = decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (decimal.TryParse(text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var decimalParsed))
                {
                    result = decimalParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (value is double doubleValue)
                {
                    result = doubleValue;
                    return true;
                }

                if (value is float floatValue)
                {
                    result = (double)floatValue;
                    return true;
                }

                if (value is decimal decimalValue)
                {
                    result = (double)decimalValue;
                    return true;
                }

                var text = value!.ToString();
                if (double.TryParse(text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var doubleParsed))
                {
                    result = doubleParsed;
                    return true;
                }

                result = null;
                return true;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                result = AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime)
                    ? dateTime
                    : null;
                return true;
            }

            result = value!.ToString();
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSqlServerSequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;

        if (!fn.Name.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        return SqlSequenceEvaluator.TryEvaluateCall(
            context.Connection,
            fn.Name,
            fn.Args,
            expr => evalArg(0),
            out result);
    }

    private static bool TryEvalSqlServerRowCountFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = (int)context.Connection.GetLastFoundRows();
        return true;
    }

    private static bool TryEvalSqlServerRowCountBigFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.Connection.GetLastFoundRows();
        return true;
    }

    private static bool TryEvalSqlServerScopeIdentityFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.Connection.GetLastInsertId();
        return true;
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
