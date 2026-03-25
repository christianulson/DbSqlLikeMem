using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Auto;

internal static class AutoTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddTableFunctions(
            new DbTableFunctionDef(
                SqlConst.OPENJSON,
                1,
                2,
                static (executor, tableSource, ctes, outerRow)
                    => executor.ExecuteOpenJsonTableFunction(tableSource, ctes, outerRow)),
            new DbTableFunctionDef(
                SqlConst.STRING_SPLIT,
                2,
                3,
                static (executor, tableSource, ctes, outerRow)
                    => executor.ExecuteStringSplitTableFunction(tableSource, ctes, outerRow)));

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
