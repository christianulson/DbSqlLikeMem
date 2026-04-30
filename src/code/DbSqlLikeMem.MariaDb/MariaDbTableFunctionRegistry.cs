namespace DbSqlLikeMem.MariaDb;

internal static class MariaDbTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < MariaDbDialect.JsonTableMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
