namespace DbSqlLikeMem;

internal static class SqlSharedTableFunctionRegistry
{
    internal static void RegisterJsonTable(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddTableFunction(new DbTableFunctionDef(SqlConst.JSON_TABLE, 2, 2));
    }
}
