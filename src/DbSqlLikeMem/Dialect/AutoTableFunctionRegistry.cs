namespace DbSqlLikeMem;

internal static class AutoTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddTableFunctions(
            new DbTableFunctionDef(SqlConst.OPENJSON, 1, 2),
            new DbTableFunctionDef(SqlConst.STRING_SPLIT, 2, 3));

        SqlSharedTableFunctionRegistry.RegisterJsonTable(dialect);
    }
}
