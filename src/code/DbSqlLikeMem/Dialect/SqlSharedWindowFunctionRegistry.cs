namespace DbSqlLikeMem;

internal static class SqlSharedWindowFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddWindowFunctions(
            new DbFunctionDef("COUNT", 1, 1),
            new DbFunctionDef(SqlConst.SUM, 1, 1),
            new DbFunctionDef(SqlConst.AVG, 1, 1),
            new DbFunctionDef(SqlConst.MIN, 1, 1),
            new DbFunctionDef(SqlConst.MAX, 1, 1),
            new DbFunctionDef("ROW_NUMBER", 0, 0, requiresOrderBy: true),
            new DbFunctionDef("RANK", 0, 0, requiresOrderBy: true),
            new DbFunctionDef("DENSE_RANK", 0, 0, requiresOrderBy: true),
            new DbFunctionDef("NTILE", 1, 1, requiresOrderBy: true),
            new DbFunctionDef("PERCENT_RANK", 0, 0, requiresOrderBy: true),
            new DbFunctionDef("CUME_DIST", 0, 0, requiresOrderBy: true),
            new DbFunctionDef("LAG", 1, 3, requiresOrderBy: true),
            new DbFunctionDef("LEAD", 1, 3, requiresOrderBy: true),
            new DbFunctionDef("FIRST_VALUE", 1, 1, requiresOrderBy: true),
            new DbFunctionDef("LAST_VALUE", 1, 1, requiresOrderBy: true),
            new DbFunctionDef("NTH_VALUE", 2, 2, requiresOrderBy: true));
    }
}
