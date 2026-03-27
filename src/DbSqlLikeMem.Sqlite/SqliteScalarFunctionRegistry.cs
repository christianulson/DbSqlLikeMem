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

        dialect.AddScalarFunction("GLOB", "INT", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("PRINTF", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("FORMAT", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("SQLITE3_MPRINTF", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("RANDOMBLOB", "VARBINARY", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("ZEROBLOB", "VARBINARY", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("SQLITE3_RESULT_ZEROBLOB", "VARBINARY", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("TYPEOF", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("UNISTR", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("UNISTR_QUOTE", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("LIKELY", "BOOLEAN", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("UNLIKELY", "BOOLEAN", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("LIKELIHOOD", "BOOLEAN", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSON_EACH", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSON_TREE", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSONB_EACH", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSONB_TREE", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSONB_EXTRACT", "VARCHAR", AstQuerySqliteScalarFunctionEvaluator.TryEvaluate);

        var julianDayDefinition = DbFunctionDef.CreateScalar("JULIANDAY", "DOUBLE") with
        {
            AstExecutor = AstQuerySqliteDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            julianDayDefinition,
            "JULIANDAY");

        var unixEpochDefinition = DbFunctionDef.CreateScalar("UNIXEPOCH", "BIGINT") with
        {
            AstExecutor = AstQuerySqliteDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            unixEpochDefinition,
            "UNIXEPOCH");

        var strftimeDefinition = DbFunctionDef.CreateScalar("STRFTIME", "VARCHAR") with
        {
            AstExecutor = AstQuerySqliteDateFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            strftimeDefinition,
            "STRFTIME");

        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
            {
                IsStringAggregate = true
            });
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("CHANGES", "INT"));
        var jsonExtractDefinition = DbFunctionDef.CreateScalar("JSON_EXTRACT", "VARCHAR") with
        {
            AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
        };
        dialect.AddScalarFunctions(
            jsonExtractDefinition,
            "JSON_EXTRACT",
            "JSON_VALUE");
        var jsonUnquoteDefinition = DbFunctionDef.CreateScalar("JSON_UNQUOTE", "VARCHAR") with
        {
            AstExecutor = AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction
        };
        dialect.AddScalarFunction(jsonUnquoteDefinition);
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"));
    }
}
