using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Sqlite;

internal static partial class SqliteScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(
                "LOG2",
                "DOUBLE",
                TryEvalSqliteLog2Function,
                signatures: new DbFunctionSignature([], 1, 1)));

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(
                "TRUNC",
                "DOUBLE",
                TryEvalSqliteTruncFunction,
                signatures: new DbFunctionSignature([], 1, 1)));

        RegisterGeneratedScalarFunctions(dialect);
        var castDefinition = DbFunctionDef.CreateScalar(
            "CAST",
            null,
            DbFunctionCategory.Conversion,
            DbInvocationStyle.Call) with
        {
            AstExecutor = AstQueryCastConversionFamilyEvaluator.TryEvalCastLikeFunction
        };
        dialect.AddScalarFunction(castDefinition);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
            {
                IsStringAggregate = true
            });
    }

    [ScalarFunction("IF", "VARCHAR")]
    [ScalarFunction("IIF", "VARCHAR")]
    [ScalarFunction("IFNULL", "VARCHAR")]
    [ScalarFunction("ISNULL", "VARCHAR")]
    [ScalarFunction("NVL", "VARCHAR")]
    private static bool TryEvalSqliteConditionalNullFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions(context, fn, evalArg, out result);

    [ScalarFunction("GLOB", "INT")]
    [ScalarFunction("PRINTF", "VARCHAR")]
    [ScalarFunction("FORMAT", "VARCHAR")]
    [ScalarFunction("SQLITE3_MPRINTF", "VARCHAR")]
    [ScalarFunction("RANDOMBLOB", "VARBINARY")]
    [ScalarFunction("ZEROBLOB", "VARBINARY")]
    [ScalarFunction("SQLITE3_RESULT_ZEROBLOB", "VARBINARY")]
    [ScalarFunction("TYPEOF", "VARCHAR")]
    [ScalarFunction("UNISTR", "VARCHAR")]
    [ScalarFunction("UNISTR_QUOTE", "VARCHAR")]
    [ScalarFunction("LIKELY", "BOOLEAN")]
    [ScalarFunction("UNLIKELY", "BOOLEAN")]
    [ScalarFunction("LIKELIHOOD", "BOOLEAN")]
    [ScalarFunction("JSON_EACH", "VARCHAR")]
    [ScalarFunction("JSON_TREE", "VARCHAR")]
    [ScalarFunction("JSONB_EACH", "VARCHAR")]
    [ScalarFunction("JSONB_TREE", "VARCHAR")]
    [ScalarFunction("JSONB_EXTRACT", "VARCHAR")]
    private static bool TryEvalSqliteScalarFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqliteScalarFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("NOW", "DATETIME", InvocationStyle = DbInvocationStyle.Call, TemporalKind = 2)]
    [ScalarFunction("CURRENT_DATE", "DATE", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 0)]
    [ScalarFunction("CURRENT_TIME", "TIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 1)]
    [ScalarFunction("CURRENT_TIMESTAMP", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 2)]
    [ScalarFunction("SYSTEMDATE", "DATETIME", InvocationStyle = DbInvocationStyle.Identifier, TemporalKind = 2)]
    private static bool TryEvalSqliteTemporalFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction(context, fn, evalArg, out result);

    [ScalarFunction("DATE", "DATE")]
    [ScalarFunction("TIME", "DATE")]
    [ScalarFunction("DATETIME", "DATE")]
    private static bool TryEvalSqliteDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryGeneralDateFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JULIANDAY", "DOUBLE")]
    private static bool TryEvalSqliteJulianDayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqliteDateFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("UNIXEPOCH", "BIGINT")]
    private static bool TryEvalSqliteUnixEpochFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqliteDateFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("STRFTIME", "VARCHAR")]
    private static bool TryEvalSqliteStrftimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqliteDateFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("CHANGES", "INT")]
    private static bool TryEvalSqliteChangesFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQuerySqliteScalarFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    [ScalarFunction("JSON_EXTRACT", "VARCHAR")]
    [ScalarFunction("JSON_VALUE", "VARCHAR")]
    private static bool TryEvalSqliteJsonExtractFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction(context, fn, evalArg, out result);

    [ScalarFunction("JSON_UNQUOTE", "VARCHAR")]
    private static bool TryEvalSqliteJsonUnquoteFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction(context, fn, evalArg, out result);

    [ScalarFunction("DATE_ADD", "DATETIME")]
    private static bool TryEvalSqliteDateAddFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    private static bool TryEvalSqliteLog2Function(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number <= 0d ? null : Math.Log(number, 2d);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSqliteTruncFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!decimal.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Number, CultureInfo.InvariantCulture, out var number))
        {
            result = null;
            return true;
        }

        result = Math.Truncate(number);
        return true;
    }

    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect);
}
