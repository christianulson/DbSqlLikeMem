using System.Globalization;
using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Auto;

internal static partial class AutoScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETUTCDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATETIME", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        var conditionalNullFunction = DbFunctionDef.CreateScalar("IIF", "VARCHAR") with
        {
            AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
        };
        dialect.AddScalarFunctions(
            conditionalNullFunction,
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");

        var addDateFunction = DbFunctionDef.CreateScalar("ADDDATE", "DATETIME") with
        {
            AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(addDateFunction);
        var addTimeFunction = DbFunctionDef.CreateScalar("ADDTIME", "DATETIME") with
        {
            AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(addTimeFunction);
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"),
            "DATE_ADD",
            "DATEADD",
            "TIMESTAMPADD");

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CONVERT", "VARCHAR"));
        var eomonthFunction = DbFunctionDef.CreateScalar("EOMONTH", "DATE") with
        {
            AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(eomonthFunction);
        var jsonExtractFunction = DbFunctionDef.CreateScalar("JSON_EXTRACT", "VARCHAR") with
        {
            AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
        };
        dialect.AddScalarFunctions(
            jsonExtractFunction,
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("JSON_UNQUOTE", "VARCHAR"));
        RegisterGeneratedScalarFunctions(dialect);
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
            {
                IsStringAggregate = true
            },
            SqlConst.GROUP_CONCAT,
            SqlConst.STRING_AGG,
            SqlConst.LISTAGG);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.NEXTVAL, "BIGINT"),
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);
    }

    [ScalarFunction("JSON_OBJECT", "VARCHAR")]
    private static bool TryEvalJsonObjectFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction(context, fn, evalArg, out result);

    [ScalarFunction("OPENJSON", "VARCHAR")]
    private static bool TryEvalOpenJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value switch
        {
            string text => text,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value!.ToString()
        };
        return true;
    }

    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect);
}
