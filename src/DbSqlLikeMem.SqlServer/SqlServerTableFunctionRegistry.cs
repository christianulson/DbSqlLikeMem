using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
            return;

        dialect.AddTableFunction(new DbTableFunctionDef(
            SqlServerConst.OPENJSON,
            1,
            2,
            static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteOpenJsonTableFunction(tableSource, ctes, outerRow)));
        dialect.AddTableFunction(new DbTableFunctionDef(
            SqlServerConst.STRING_SPLIT,
            2,
            version >= SqlServerDialect.StringSplitOrdinalMinVersion ? 3 : 2,
            static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteStringSplitTableFunction(tableSource, ctes, outerRow)));
    }
}
