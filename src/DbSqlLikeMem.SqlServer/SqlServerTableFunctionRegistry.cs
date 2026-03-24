using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
            return;

        dialect.AddTableFunction(new DbTableFunctionDef(SqlConst.OPENJSON, 1, 2));
        dialect.AddTableFunction(new DbTableFunctionDef(
            SqlConst.STRING_SPLIT,
            2,
            version >= SqlServerDialect.StringSplitOrdinalMinVersion ? 3 : 2));
    }
}
