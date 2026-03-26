namespace DbSqlLikeMem.Models;

internal sealed record DbTableFunctionDef(
    string Name,
    int MinArguments,
    int MaxArguments,
    Func<AstQueryTableFunctionExecutor, SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>? AstExecutor = null)
    : ProcessDef(Name)
{
    internal bool AllowsArgumentCount(int count)
        => count >= MinArguments && count <= MaxArguments;
}
