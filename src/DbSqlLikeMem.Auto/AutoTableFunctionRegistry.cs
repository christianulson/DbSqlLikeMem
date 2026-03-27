using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Auto;

internal static class AutoTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        DbFunctionDef openJsonDefinition = DbFunctionDef.CreateTable(
            SqlConst.OPENJSON,
            signatures: new DbFunctionSignature([], 1, 2)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteOpenJsonTableFunction(tableSource, ctes, outerRow)
        };

        DbFunctionDef stringSplitDefinition = DbFunctionDef.CreateTable(
            SqlConst.STRING_SPLIT,
            signatures: new DbFunctionSignature([], 2, 3)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteStringSplitTableFunction(tableSource, ctes, outerRow)
        };

        dialect.AddTableFunctions(openJsonDefinition, stringSplitDefinition);

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
