namespace DbSqlLikeMem;

internal sealed class SelectPlan
{
    public required List<TableResultColMock> Columns { get; init; }

    public required List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> Evaluators { get; init; }

    public required List<WindowSlot> WindowSlots { get; init; }

    internal SelectPlan CloneForCache()
        => new()
        {
            Columns = Columns,
            Evaluators = Evaluators,
            WindowSlots = [.. WindowSlots.Select(slot => slot.CloneForCache())]
        };

    internal SelectPlan CloneForExecution()
        => new()
        {
            Columns = Columns,
            Evaluators = Evaluators,
            WindowSlots = [.. WindowSlots.Select(slot => slot.CloneForExecution())]
        };
}

internal sealed class WindowSlot
{
    public required WindowFunctionExpr Expr { get; init; }

    public required Dictionary<AstQueryExecutorBase.EvalRow, object?> Map { get; init; }

    internal WindowSlot CloneForCache()
        => new()
        {
            Expr = Expr,
            Map = new Dictionary<AstQueryExecutorBase.EvalRow, object?>(ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance)
        };

    internal WindowSlot CloneForExecution()
        => new()
        {
            Expr = Expr,
            Map = new Dictionary<AstQueryExecutorBase.EvalRow, object?>(ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance)
        };
}
