using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("IF", "VARCHAR", [], body)
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");
        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("DATE", "DATE", [], body)
            {
                AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
            },
            "DATE",
            "TIME",
            "DATETIME");

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("JULIANDAY", "DOUBLE", [], body)
            {
                AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
            },
            "JULIANDAY");

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("STRFTIME", "VARCHAR", [], body)
            {
                AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate
            },
            "STRFTIME");

        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GROUP_CONCAT", "VARCHAR", body);
        dialect.AddScalarFunction("CHANGES", "INT", body);
        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("JSON_EXTRACT", "VARCHAR", [], body)
            {
                AstExecutor = AstQueryExecutorBase.TryEvalJsonExtractionFunction
            },
            "JSON_EXTRACT",
            "JSON_VALUE");
        dialect.AddScalarFunction(
            new DbScalarFunctionDef("JSON_UNQUOTE", "VARCHAR", [], body)
            {
                AstExecutor = AstQueryExecutorBase.TryEvalJsonUtilityFunctions
            });
        dialect.AddScalarFunction("DATE_ADD", "DATETIME", body);
    }
}
