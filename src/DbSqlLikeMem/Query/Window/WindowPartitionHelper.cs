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
        Func<object?, object?, int> compareSql)
    {
        if (orderBy.Count == 0 || partition.Count < 2)
            return null;

        // EN: Ensure deterministic ordering for peers (equal ORDER BY values) so window functions like NTILE/LAG/LEAD
        // preserve the original row sequence when the ORDER BY does not provide a tie-breaker.
        // PT: Garante ordenacao deterministica para peers (valores iguais no ORDER BY) para que funcoes de janela como
        // NTILE/LAG/LEAD preservem a sequencia original das linhas quando o ORDER BY nao define desempate.
        var stableIndexByRow = new Dictionary<AstQueryExecutorBase.EvalRow, int>(partition.Count);
        for (var i = 0; i < partition.Count; i++)
            stableIndexByRow[partition[i]] = i;

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

                return stableIndexByRow.TryGetValue(leftRow, out var li) && stableIndexByRow.TryGetValue(rightRow, out var ri)
                    ? li.CompareTo(ri)
                    : 0;
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

            return stableIndexByRow.TryGetValue(leftRow, out var li) && stableIndexByRow.TryGetValue(rightRow, out var ri)
                ? li.CompareTo(ri)
                : 0;
        });

        return orderValuesByRow;
    }

    private static WindowPartitionKey BuildPartitionKey(
        IReadOnlyList<SqlExpr> partitionBy,
        AstQueryExecutorBase.EvalRow row,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalPartitionExpression,
        Func<object?, string> normalizePartitionKeyValue)
    {
        if (partitionBy.Count == 0)
            return new WindowPartitionKey(Array.Empty<string>());

        var parts = new string[partitionBy.Count];
        for (var i = 0; i < partitionBy.Count; i++)
            parts[i] = normalizePartitionKeyValue(evalPartitionExpression(partitionBy[i], row));

        return new WindowPartitionKey(parts);
    }

    internal readonly record struct WindowPartitionKey(string[] Values)
    {
        internal static IEqualityComparer<WindowPartitionKey> Comparer { get; } = new WindowPartitionKeyComparer();

        private sealed class WindowPartitionKeyComparer : IEqualityComparer<WindowPartitionKey>
        {
            public bool Equals(WindowPartitionKey x, WindowPartitionKey y)
            {
                if (ReferenceEquals(x.Values, y.Values))
                    return true;

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
