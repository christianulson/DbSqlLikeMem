namespace DbSqlLikeMem;

internal static class WindowPartitionHelper
{
    internal static Dictionary<string, List<AstQueryExecutorBase.EvalRow>> BuildPartitions(
        WindowFunctionExpr windowFunction,
        List<AstQueryExecutorBase.EvalRow> rows,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        var partitions = new Dictionary<string, List<AstQueryExecutorBase.EvalRow>>(StringComparer.Ordinal);

        foreach (var row in rows)
        {
            var key = BuildPartitionKey(windowFunction.Spec.PartitionBy, row, evalPartitionExpression, normalizePartitionKeyValue);
            if (!partitions.TryGetValue(key, out var partition))
            {
                partition = [];
                partitions[key] = partition;
            }

            partition.Add(row);
        }

        return partitions;
    }

    internal static void SortPartition(
        List<AstQueryExecutorBase.EvalRow> partition,
        IReadOnlyList<WindowOrderItem> orderBy,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalOrderExpression,
        Func<object?, object?, int> compareSql)
    {
        if (orderBy.Count == 0)
            return;

        partition.Sort((leftRow, rightRow) =>
        {
            foreach (var orderItem in orderBy)
            {
                var leftValue = evalOrderExpression(orderItem.Expr, leftRow);
                var rightValue = evalOrderExpression(orderItem.Expr, rightRow);
                var comparison = compareSql(leftValue, rightValue);
                if (comparison != 0)
                    return orderItem.Desc ? -comparison : comparison;
            }

            return 0;
        });
    }

    private static string BuildPartitionKey(
        IReadOnlyList<SqlExpr> partitionBy,
        AstQueryExecutorBase.EvalRow row,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        if (partitionBy.Count == 0)
            return "__all__";

        var parts = new string[partitionBy.Count];
        for (var i = 0; i < partitionBy.Count; i++)
            parts[i] = normalizePartitionKeyValue(evalPartitionExpression(partitionBy[i], row));

        return string.Join("\u001F", parts);
    }
}
