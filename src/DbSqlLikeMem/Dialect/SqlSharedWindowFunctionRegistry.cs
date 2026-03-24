namespace DbSqlLikeMem;

internal static class SqlSharedWindowFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddWindowFunctions(
            new DbWindowFunctionDef("ROW_NUMBER", 0, 0, RequiresOrderBy: true),
            new DbWindowFunctionDef("RANK", 0, 0, RequiresOrderBy: true),
            new DbWindowFunctionDef("DENSE_RANK", 0, 0, RequiresOrderBy: true),
            new DbWindowFunctionDef("NTILE", 1, 1, RequiresOrderBy: true),
            new DbWindowFunctionDef("PERCENT_RANK", 0, 0, RequiresOrderBy: true),
            new DbWindowFunctionDef("CUME_DIST", 0, 0, RequiresOrderBy: true),
            new DbWindowFunctionDef("LAG", 1, 3, RequiresOrderBy: true),
            new DbWindowFunctionDef("LEAD", 1, 3, RequiresOrderBy: true),
            new DbWindowFunctionDef("FIRST_VALUE", 1, 1, RequiresOrderBy: true),
            new DbWindowFunctionDef("LAST_VALUE", 1, 1, RequiresOrderBy: true),
            new DbWindowFunctionDef("NTH_VALUE", 2, 2, RequiresOrderBy: true));
    }
}
