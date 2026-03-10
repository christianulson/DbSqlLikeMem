namespace DbSqlLikeMem;

internal sealed class SelectPlan
{
    public required List<TableResultColMock> Columns { get; init; }

    public required List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> Evaluators { get; init; }

    public required List<WindowSlot> WindowSlots { get; init; }
}

internal sealed class WindowSlot
{
    public required WindowFunctionExpr Expr { get; init; }

    public required Dictionary<AstQueryExecutorBase.EvalRow, object?> Map { get; init; }
}
