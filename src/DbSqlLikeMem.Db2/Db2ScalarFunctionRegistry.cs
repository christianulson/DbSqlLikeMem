using System.Globalization;

using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Db2;

internal static class Db2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        Db2OracleDb2ScalarFunctionRegistry.Register(dialect);

        RegisterConversionFunctions(dialect);
        RegisterTemporalFunctions(dialect);
        RegisterLegacyNumericFunctions(dialect);
        RegisterAnalyticsFunctions(dialect);
        RegisterStringFunctions(dialect, version);
        RegisterRowCountFunctions(dialect);
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunctions("VARCHAR", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate, "BPCHAR", "DBCLOB", "GRAPHIC", "VARGRAPHIC");
        dialect.AddScalarFunctions("DOUBLE", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate, "DOUBLE_PRECISION", "FLOAT4", "FLOAT8");
        dialect.AddScalarFunctions("VARCHAR", AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate, "TO_CLOB", "TO_NCHAR", "TO_NCLOB");
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect)
    {
        static bool TryEvalDb2DaysFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count != 1)
            {
                result = null;
                return false;
            }

            var baseValue = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            result = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
            return true;
        }

        static bool TryEvalDb2TimestampAddAndDiffFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            // SqlExpressionParser stores leading temporal units as RawSqlExpr for TIMESTAMPADD/TIMESTAMPDIFF.
            // The legacy evaluator expects the unit through evalArg(0), so we shim it here for DB2.
            object? unitValue;
            if (fn.Args.Count > 0 && fn.Args[0] is RawSqlExpr raw)
            {
                var text = raw.Sql.Trim();
                unitValue = int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intervalCode)
                    ? intervalCode
                    : text;
            }
            else
            {
                unitValue = evalArg(0);
            }

            object? EvalArgShim(int i) => i == 0 ? unitValue : evalArg(i);
            return AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate(context, fn, EvalArgShim, out result);
        }

        static bool TryEvalDb2DateAddAliasFunction(
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

            var baseValue = evalArg(0);
            var amountValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(amountValue))
            {
                result = null;
                return true;
            }

            if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            var name = fn.Name.ToUpperInvariant();
            var unit = name["ADD_".Length..];
            var amount = Convert.ToInt32(Convert.ToDecimal(amountValue, CultureInfo.InvariantCulture));
            if (name == "ADD_DAYS" || name == "ADD_HOURS" || name == "ADD_MINUTES" || name == "ADD_SECONDS" || name == "ADD_MONTHS" || name == "ADD_YEARS")
            {
                var temporalUnit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unit);
                result = AstQueryExecutorBase.ApplyDateDelta(dateTime, temporalUnit, amount);
                return true;
            }

            result = null;
            return true;
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE", "DATE") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATETIME", "DATETIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIME", "TIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMP", "DATETIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DAYS", "INT") with
            {
                AstExecutor = TryEvalDb2DaysFunction
            });

        dialect.AddScalarFunction(
            "SESSION_USER",
            "VARCHAR",
            static (QueryExecutionContext ctx, FunctionCallExpr fn, Func<int, object?> evalArg, out object? result) =>
            {
                _ = ctx;
                _ = fn;
                _ = evalArg;
                result = "dbo";
                return true;
            },
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            null);

        dialect.AddScalarFunction("CURDATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call | DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("NEXT_DAY", "DATE") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            "TRUNCATE",
            "DECIMAL",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_DAYS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_HOURS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_MINUTES", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_SECONDS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_MONTHS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_YEARS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE_TRUNC", "DATE") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPADD", "DATETIME") with { AstExecutor = TryEvalDb2TimestampAddAndDiffFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPDIFF", "INT") with { AstExecutor = TryEvalDb2TimestampAddAndDiffFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("MIDNIGHT_SECONDS", "INT") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("EOMONTH", "DATE") with
            {
                AstExecutor = TryEvalDb2EomonthFunction
            });
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("NEXT_VALUE_FOR", "BIGINT"),
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");
    }

    private static bool TryEvalDb2EomonthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return AstQuerySqlServerCompatibilityFunctionEvaluator.TryEvalEomonthFunction(fn, evalArg, out result);
    }

    private static void RegisterLegacyNumericFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DIV", "DECIMAL", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate));
    }

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("RATIO_TO_REPORT", "DOUBLE"));
    }

    private static void RegisterStringFunctions(ISqlDialect dialect, int version)
    {
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateCallOrIdentifier(Db2Const.VALUE, "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            Db2Const.VALUE,
            "IFNULL");

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.LISTAGG, "VARCHAR") with
            {
                IsStringAggregate = true
            });

        if (version >= Db2Dialect.JsonFunctionsMinVersion)
        {
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("JSON_QUERY", "VARCHAR") with
                {
                    AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
                },
                "JSON_QUERY",
                "JSON_VALUE");
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("GROUPING", "INT") with
            {
                AstExecutor = AstQueryGroupingFunctionEvaluator.TryEvaluate
            });

    }

    private static void RegisterRowCountFunctions(ISqlDialect dialect)
    {
        static bool TryEvalLastFoundRowsFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = evalArg;
            context.EnsureOracleDb2FunctionSupported(fn);

            if (fn.Args.Count != 0)
            {
                result = null;
                return false;
            }

            result = context.Connection.GetLastFoundRows();
            return true;
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ROW_COUNT", "BIGINT") with
            {
                AstExecutor = TryEvalLastFoundRowsFunction
            });
    }
}
