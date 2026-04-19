using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteTableFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var jsonEachFunction = DbFunctionDef.CreateTable(
            "json_each",
            signatures: new DbFunctionSignature([], 1, 1)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteJsonEachTableFunction(tableSource, ctes, outerRow)
        };
        var jsonTreeFunction = DbFunctionDef.CreateTable(
            "json_tree",
            signatures: new DbFunctionSignature([], 1, 1)) with
        {
            TableExecutor = static (executor, tableSource, ctes, outerRow)
                => executor.ExecuteJsonTreeTableFunction(tableSource, ctes, outerRow)
        };

        dialect.AddTableFunctions(jsonEachFunction, jsonTreeFunction);
    }
}
