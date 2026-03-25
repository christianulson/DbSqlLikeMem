namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private object? ResolveParam(
        string name)
    {
        if (TryResolveLocalFunctionValue(name, out var localValue))
            return localValue;

        return QueryRowValueHelper.ResolveParam(_context, name);
    }

    private static object? ResolveIdentifier(
        string name,
        EvalRow row)
        => QueryRowValueHelper.ResolveIdentifier(name, row);

    private static object? ResolveColumn(
        string? qualifier,
        string col,
        EvalRow row)
        => QueryRowValueHelper.ResolveColumn(qualifier, col, row);

    private static TableResultMock ApplyDistinct(
        TableResultMock res,
        QueryExecutionContext context)
        => QueryRowValueHelper.ApplyDistinct(res, context);

    private static SqlSelectQuery GetSingleSubqueryOrThrow(
        SubqueryExpr sq,
        string context)
    {
        if (sq.Parsed is null)
            throw new InvalidOperationException(
                $"{context}: SubqueryExpr sem AST parseado (Parsed vazio).");

        return sq.Parsed;
    }
}
