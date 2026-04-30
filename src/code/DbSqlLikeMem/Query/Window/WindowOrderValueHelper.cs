namespace DbSqlLikeMem;

internal static class WindowOrderValueHelper
{
    internal static object?[][] BuildWindowOrderValuesByIndex(
        List<AstQueryExecutorBase.EvalRow> partition,
        IReadOnlyList<WindowOrderItem> orderBy,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalOrderExpression)
    {
        var orderValuesByIndex = new object?[partition.Count][];
        for (var i = 0; i < partition.Count; i++)
        {
            var row = partition[i];
            var values = new object?[orderBy.Count];
            for (var j = 0; j < orderBy.Count; j++)
                values[j] = evalOrderExpression(orderBy[j].Expr, row);

            orderValuesByIndex[i] = values;
        }

        return orderValuesByIndex;
    }

    internal static Dictionary<AstQueryExecutorBase.EvalRow, object?[]> BuildWindowOrderValuesByRow(
        List<AstQueryExecutorBase.EvalRow> partition,
        IReadOnlyList<WindowOrderItem> orderBy,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalOrderExpression)
    {
        var orderValuesByRow = new Dictionary<AstQueryExecutorBase.EvalRow, object?[]>(
            Math.Max(1, partition.Count),
            ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance);

        foreach (var row in partition)
        {
            var values = new object?[orderBy.Count];
            for (var i = 0; i < orderBy.Count; i++)
                values[i] = evalOrderExpression(orderBy[i].Expr, row);

            orderValuesByRow[row] = values;
        }

        return orderValuesByRow;
    }

    internal static bool RowsFrameContainsRow(RowsFrameRange frameRange, int rowIndex)
        => rowIndex >= frameRange.StartIndex && rowIndex <= frameRange.EndIndex;

    internal static bool WindowOrderValuesEqual(
        this WindowPartitionExecutionContext context,
        object?[] left,
        object?[] right)
    {
        if (left.Length != right.Length)
            return false;

        for (var i = 0; i < left.Length; i++)
        {
            if (context.QueryExecutionContext.CompareSql(left[i], right[i]) != 0)
                return false;
        }

        return true;
    }
}
