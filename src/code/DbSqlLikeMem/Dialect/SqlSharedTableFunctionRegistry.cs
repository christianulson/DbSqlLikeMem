namespace DbSqlLikeMem;

internal static class SqlSharedTableFunctionRegistry
{
    internal static void RegisterJsonTable(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var jsonTableFunction = DbFunctionDef.CreateTable(
            SqlConst.JSON_TABLE,
            signatures: new DbFunctionSignature([], 2, 2)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteJsonTableFunction(tableSource, ctes, outerRow)
        };

        dialect.AddTableFunction(jsonTableFunction);
    }
}
