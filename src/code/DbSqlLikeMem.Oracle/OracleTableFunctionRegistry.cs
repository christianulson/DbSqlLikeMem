namespace DbSqlLikeMem.Oracle;

internal static class OracleTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        if (version < OracleDialect.OracleJsonSqlFunctionMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
