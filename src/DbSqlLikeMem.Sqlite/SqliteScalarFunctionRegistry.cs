using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        var ifDefinition = DbFunctionDef.CreateScalar("IF", "VARCHAR") with
        {
            AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
        };
        dialect.AddScalarFunctions(
            ifDefinition,
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");
        var dateDefinition = DbFunctionDef.CreateScalar("DATE", "DATE") with
        {
            AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            dateDefinition,
            "DATE",
            "TIME",
            "DATETIME");

        var julianDayDefinition = DbFunctionDef.CreateScalar("JULIANDAY", "DOUBLE") with
        {
            AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            julianDayDefinition,
            "JULIANDAY");

        var strftimeDefinition = DbFunctionDef.CreateScalar("STRFTIME", "VARCHAR") with
        {
            AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            strftimeDefinition,
            "STRFTIME");

        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("CHANGES", "INT"));
        var jsonExtractDefinition = DbFunctionDef.CreateScalar("JSON_EXTRACT", "VARCHAR") with
        {
            AstExecutor = AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonExtractionFunction
        };
        dialect.AddScalarFunctions(
            jsonExtractDefinition,
            "JSON_EXTRACT",
            "JSON_VALUE");
        var jsonUnquoteDefinition = DbFunctionDef.CreateScalar("JSON_UNQUOTE", "VARCHAR") with
        {
            AstExecutor = AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonUtilityFunctions
        };
        dialect.AddScalarFunction(jsonUnquoteDefinition);
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"));
    }
}
