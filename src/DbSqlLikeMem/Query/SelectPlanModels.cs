namespace DbSqlLikeMem;

internal sealed class SelectPlan
{
    public required List<TableResultColMock> Columns { get; init; }

    public required List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> Evaluators { get; init; }

    public required List<int> WindowSlotIndexes { get; init; }

    public required List<WindowSlot> WindowSlots { get; init; }

    internal SelectPlan CloneForCache()
        => new()
        {
            Columns = Columns,
            Evaluators = Evaluators,
            WindowSlotIndexes = WindowSlotIndexes,
            WindowSlots = CloneWindowSlots(forExecution: false)
        };

    internal SelectPlan CloneForExecution()
    {
        var clonedWindowSlots = CloneWindowSlots(forExecution: true);
        return new()
        {
            Columns = Columns,
            Evaluators = CloneEvaluatorsForExecution(clonedWindowSlots),
            WindowSlotIndexes = WindowSlotIndexes,
            WindowSlots = clonedWindowSlots
        };
    }

    private List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> CloneEvaluatorsForExecution(
        List<WindowSlot> clonedWindowSlots)
    {
        if (Evaluators.Count == 0)
            return [];

        if (WindowSlotIndexes.Count != Evaluators.Count)
            return [.. Evaluators];
        var evaluators = new List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>>(Evaluators.Count);

        for (var i = 0; i < Evaluators.Count; i++)
        {
            var slotIndex = WindowSlotIndexes[i];
            if (slotIndex < 0 || slotIndex >= clonedWindowSlots.Count)
            {
                evaluators.Add(Evaluators[i]);
                continue;
            }

            var slot = clonedWindowSlots[slotIndex];
            evaluators.Add((row, group) => slot.Map.TryGetValue(row, out var value) ? value : null);
        }

        return evaluators;
    }

    private List<WindowSlot> CloneWindowSlots(bool forExecution)
    {
        if (WindowSlots.Count == 0)
            return [];

        var cloned = new List<WindowSlot>(WindowSlots.Count);
        foreach (var slot in WindowSlots)
            cloned.Add(forExecution ? slot.CloneForExecution() : slot.CloneForCache());

        return cloned;
    }
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
