namespace DbSqlLikeMem.Models;

internal sealed record DbTableFunctionDef(
    string Name,
    int MinArguments,
    int MaxArguments,
    Func<DbSqlLikeMem.AstQueryTableFunctionExecutor, DbSqlLikeMem.SqlTableSource, IDictionary<string, DbSqlLikeMem.AstQueryExecutorBase.Source>, DbSqlLikeMem.AstQueryExecutorBase.EvalRow?, DbSqlLikeMem.TableResultMock>? AstExecutor = null)
    : ProcessDef(Name)
{
    internal bool AllowsArgumentCount(int count)
        => count >= MinArguments && count <= MaxArguments;
}
