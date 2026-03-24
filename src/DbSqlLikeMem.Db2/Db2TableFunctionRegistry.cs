namespace DbSqlLikeMem.Db2;

internal static class Db2TableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < Db2Dialect.JsonFunctionsMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
