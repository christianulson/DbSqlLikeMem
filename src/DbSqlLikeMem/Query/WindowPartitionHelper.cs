namespace DbSqlLikeMem;

internal static class WindowPartitionHelper
{
    internal static Dictionary<string, List<AstQueryExecutorBase.EvalRow>> BuildPartitions(
        WindowFunctionExpr windowFunction,
        List<AstQueryExecutorBase.EvalRow> rows,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        var partitions = new Dictionary<string, List<AstQueryExecutorBase.EvalRow>>(Math.Max(1, rows.Count), StringComparer.Ordinal);

        foreach (var row in rows)
        {
            var key = BuildPartitionKey(windowFunction.Spec.PartitionBy, row, evalPartitionExpression, normalizePartitionKeyValue);
            if (!partitions.TryGetValue(key, out var partition))
            {
                partition = new List<AstQueryExecutorBase.EvalRow>();
                partitions[key] = partition;
            }

            partition.Add(row);
        }

        return partitions;
    }

    internal static Dictionary<AstQueryExecutorBase.EvalRow, object?[]>? SortPartition(
        List<AstQueryExecutorBase.EvalRow> partition,
        IReadOnlyList<WindowOrderItem> orderBy,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalOrderExpression,
        Func<object?, object?, int> compareSql)
    {
        if (orderBy.Count == 0)
            return null;

        var orderValuesByRow = WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            partition,
            orderBy,
            evalOrderExpression);

        if (orderBy.Count == 1)
        {
            var orderItem = orderBy[0];
            partition.Sort((leftRow, rightRow) =>
            {
                var leftValue = orderValuesByRow[leftRow][0];
                var rightValue = orderValuesByRow[rightRow][0];
                var comparison = compareSql(leftValue, rightValue);
                if (comparison != 0)
                    return orderItem.Desc ? -comparison : comparison;

                return 0;
            });

            return orderValuesByRow;
        }

        partition.Sort((leftRow, rightRow) =>
        {
            var leftValues = orderValuesByRow[leftRow];
            var rightValues = orderValuesByRow[rightRow];

            for (var i = 0; i < orderBy.Count; i++)
            {
                var comparison = compareSql(leftValues[i], rightValues[i]);
                if (comparison != 0)
                    return orderBy[i].Desc ? -comparison : comparison;
            }

            return 0;
        });

        return orderValuesByRow;
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
