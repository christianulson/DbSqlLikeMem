namespace DbSqlLikeMem.Db2;

internal static class Db2WindowFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlDialectWindowFunctionRegistryExtensions.AddWindowFunction(
            dialect,
            new Models.DbFunctionDef("ROWNUMBER", 0, 0, requiresOver: true, requiresOrderBy: true));
    }
}
