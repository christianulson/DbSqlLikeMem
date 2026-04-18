namespace DbSqlLikeMem;

internal static class AutoTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var openJsonFunction = DbFunctionDef.CreateTable(
            SqlConst.OPENJSON,
            signatures: new DbFunctionSignature([], 1, 2)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteOpenJsonTableFunction(tableSource, ctes, outerRow)
        };
        var stringSplitFunction = DbFunctionDef.CreateTable(
            SqlConst.STRING_SPLIT,
            signatures: new DbFunctionSignature([], 2, 3)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteStringSplitTableFunction(tableSource, ctes, outerRow)
        };
        var jsonEachFunction = DbFunctionDef.CreateTable(
            "json_each",
            signatures: new DbFunctionSignature([], 1, 1)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteJsonEachTableFunction(tableSource, ctes, outerRow)
        };
        var jsonTreeFunction = DbFunctionDef.CreateTable(
            "json_tree",
            signatures: new DbFunctionSignature([], 1, 1)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteJsonTreeTableFunction(tableSource, ctes, outerRow)
        };
        dialect.AddTableFunctions(openJsonFunction, stringSplitFunction, jsonEachFunction, jsonTreeFunction);

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
