using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
            return;

        var openJsonDefinition = DbFunctionDef.CreateTable(
            SqlServerConst.OPENJSON,
            signatures: new DbFunctionSignature([], 1, 2)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteOpenJsonTableFunction(tableSource, ctes, outerRow)
        };
        dialect.AddTableFunction(openJsonDefinition);

        var stringSplitDefinition = DbFunctionDef.CreateTable(
            SqlServerConst.STRING_SPLIT,
            signatures: new DbFunctionSignature([], 2, version >= SqlServerDialect.StringSplitOrdinalMinVersion ? 3 : 2)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteStringSplitTableFunction(tableSource, ctes, outerRow)
        };
        dialect.AddTableFunction(stringSplitDefinition);
    }
}
