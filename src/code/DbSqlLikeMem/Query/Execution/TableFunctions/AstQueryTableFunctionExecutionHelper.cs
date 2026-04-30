namespace DbSqlLikeMem;

internal static class AstQueryTableFunctionExecutionHelper
{
    internal static bool IsNullish(object? value)
        => value is null or DBNull;

    internal static AstQueryExecutorBase.EvalRow CreateFunctionEvaluationRow(AstQueryExecutorBase.EvalRow? outerRow)
        => outerRow ?? new AstQueryExecutorBase.EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase));
}
