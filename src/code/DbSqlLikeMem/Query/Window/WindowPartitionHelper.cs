namespace DbSqlLikeMem;

internal static class WindowPartitionHelper
{
    internal static Dictionary<WindowPartitionKey, List<AstQueryExecutorBase.EvalRow>> BuildPartitions(
        WindowFunctionExpr windowFunction,
        List<AstQueryExecutorBase.EvalRow> rows,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        var partitions = new Dictionary<WindowPartitionKey, List<AstQueryExecutorBase.EvalRow>>(
            Math.Max(1, rows.Count),
            WindowPartitionKey.Comparer);

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
        Func<object?, object?, int> compareSql,
        bool includeOrderValues = true)
    {
        if (orderBy.Count == 0 || partition.Count < 2)
            return null;

        var orderIndexes = new int[partition.Count];
        for (var i = 0; i < partition.Count; i++)
            orderIndexes[i] = i;

        if (orderBy.Count == 1)
        {
            var orderItem = orderBy[0];
            var cachedValues = new object?[partition.Count];
            var hasCachedValues = new bool[partition.Count];

            object? GetOrderValue(int index)
            {
                if (hasCachedValues[index])
                    return cachedValues[index];

                var value = evalOrderExpression(orderItem.Expr, partition[index]);
                cachedValues[index] = value;
                hasCachedValues[index] = true;
                return value;
            }

            Array.Sort(orderIndexes, (leftIndex, rightIndex) =>
            {
                var comparison = compareSql(GetOrderValue(leftIndex), GetOrderValue(rightIndex));
                if (comparison != 0)
                    return orderItem.Desc ? -comparison : comparison;

                return leftIndex.CompareTo(rightIndex);
            });

            ReorderPartition(partition, orderIndexes);
            return null;
        }

        // EN: Ensure deterministic ordering for peers (equal ORDER BY values) so window functions like NTILE/LAG/LEAD
        // preserve the original row sequence when the ORDER BY does not provide a tie-breaker.
        // PT: Garante ordenacao deterministica para peers (valores iguais no ORDER BY) para que funcoes de janela como
        // NTILE/LAG/LEAD preservem a sequencia original das linhas quando o ORDER BY nao define desempate.
        var orderValuesByIndex = WindowOrderValueHelper.BuildWindowOrderValuesByIndex(
            partition,
            orderBy,
            evalOrderExpression);

        Array.Sort(orderIndexes, (leftIndex, rightIndex) =>
        {
            var leftValues = orderValuesByIndex[leftIndex];
            var rightValues = orderValuesByIndex[rightIndex];

            for (var i = 0; i < orderBy.Count; i++)
            {
                var comparison = compareSql(leftValues[i], rightValues[i]);
                if (comparison != 0)
                    return orderBy[i].Desc ? -comparison : comparison;
            }

            return leftIndex.CompareTo(rightIndex);
        });

        ReorderPartition(partition, orderIndexes);
        return includeOrderValues ? BuildOrderValuesByRow(partition, orderValuesByIndex, orderIndexes) : null;
    }

    private static Dictionary<AstQueryExecutorBase.EvalRow, object?[]> BuildOrderValuesByRow(
        List<AstQueryExecutorBase.EvalRow> partition,
        object?[][] orderValuesByIndex,
        int[] orderIndexes)
    {
        var orderValuesByRow = new Dictionary<AstQueryExecutorBase.EvalRow, object?[]>(
            Math.Max(1, partition.Count),
            ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance);

        for (var i = 0; i < partition.Count; i++)
            orderValuesByRow[partition[i]] = orderValuesByIndex[orderIndexes[i]];

        return orderValuesByRow;
    }

    private static void ReorderPartition(List<AstQueryExecutorBase.EvalRow> partition, int[] orderIndexes)
    {
        if (orderIndexes.Length <= 1)
            return;

        var orderedRows = new AstQueryExecutorBase.EvalRow[orderIndexes.Length];
        for (var i = 0; i < orderIndexes.Length; i++)
            orderedRows[i] = partition[orderIndexes[i]];

        partition.Clear();
        partition.AddRange(orderedRows);
    }

    private static WindowPartitionKey BuildPartitionKey(
        IReadOnlyList<SqlExpr> partitionBy,
        AstQueryExecutorBase.EvalRow row,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        if (partitionBy.Count == 0)
            return new WindowPartitionKey(SingleValue: null, Values: Array.Empty<string>());

        if (partitionBy.Count == 1)
            return new WindowPartitionKey(
                SingleValue: normalizePartitionKeyValue(evalPartitionExpression(partitionBy[0], row)),
                Values: null);

        var parts = new string[partitionBy.Count];
        for (var i = 0; i < partitionBy.Count; i++)
            parts[i] = normalizePartitionKeyValue(evalPartitionExpression(partitionBy[i], row));

        return new WindowPartitionKey(SingleValue: null, Values: parts);
    }

    internal readonly record struct WindowPartitionKey(string? SingleValue, string[]? Values)
    {
        internal static IEqualityComparer<WindowPartitionKey> Comparer { get; } = new WindowPartitionKeyComparer();

        private sealed class WindowPartitionKeyComparer : IEqualityComparer<WindowPartitionKey>
        {
            public bool Equals(WindowPartitionKey x, WindowPartitionKey y)
            {
                if (x.Values is null || y.Values is null)
                    return x.Values is null
                        && y.Values is null
                        && string.Equals(x.SingleValue, y.SingleValue, StringComparison.Ordinal);

                if (x.Values.Length != y.Values.Length)
                    return false;

                for (var i = 0; i < x.Values.Length; i++)
                {
                    if (!string.Equals(x.Values[i], y.Values[i], StringComparison.Ordinal))
                        return false;
                }

                return true;
            }

            public int GetHashCode(WindowPartitionKey obj)
            {
                if (obj.Values is null)
                    return obj.SingleValue is null ? 0 : StringComparer.Ordinal.GetHashCode(obj.SingleValue);

                var hash = 17;
                for (var i = 0; i < obj.Values.Length; i++)
                {
                    var value = obj.Values[i];
                    hash = (hash * 31) + (value is null ? 0 : StringComparer.Ordinal.GetHashCode(value));
                }

                return hash;
            }
        }
    }
}
