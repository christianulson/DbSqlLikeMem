namespace DbSqlLikeMem;

internal static class QueryOrderByHelper
{
    internal static bool TryApplyOrder(
        TableResultMock result,
        IReadOnlyList<SqlOrderByItem> orderBy,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalExpression,
        Func<object?, object?, int> compareSql)
    {
        Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>? joinFieldsByRow = null;
        var keySelectors = BuildKeySelectors(
            result,
            orderBy,
            parseExpression,
            evalExpression,
            () => joinFieldsByRow ??= BuildJoinFieldsByRow(result));
        if (keySelectors.Count == 0)
            return false;

        var sortedRows = result.ToList();
        sortedRows.Sort((left, right) => CompareRows(left, right, keySelectors, compareSql));

        if (result.JoinFields.Count > 0 && joinFieldsByRow is null)
            joinFieldsByRow = BuildJoinFieldsByRow(result);

        result.Clear();
        foreach (var row in sortedRows)
            result.Add(row);

        if (joinFieldsByRow is not null)
            ReorderJoinFields(result, sortedRows, joinFieldsByRow);
        return true;
    }

    private static List<OrderByKeySelector> BuildKeySelectors(
        TableResultMock result,
        IReadOnlyList<SqlOrderByItem> orderBy,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalExpression,
        Func<Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>> getJoinFieldsByRow)
    {
        var keySelectors = new List<OrderByKeySelector>(orderBy.Count);
        Dictionary<string, int>? aliasToIndex = null;

        foreach (var item in orderBy)
        {
            var raw = (item.Raw ?? string.Empty).Trim();
            if (raw.Length == 0)
                continue;

            if (TryCreateOrdinalSelector(result, item, raw, out var ordinalSelector))
            {
                keySelectors.Add(ordinalSelector);
                continue;
            }

            if (TryCreateColumnSelector(result, item, raw, out var columnSelector))
            {
                keySelectors.Add(columnSelector);
                continue;
            }

            aliasToIndex ??= BuildAliasToIndex(result.Columns);
            var parsedExpression = parseExpression(raw);
            keySelectors.Add(new OrderByKeySelector(
                row =>
                {
                    var joinFieldsByRow = getJoinFieldsByRow();
                    joinFieldsByRow.TryGetValue(row, out var joinFields);
                    var projectedRow = AstQueryExecutorBase.EvalRow.FromProjected(
                        result,
                        row,
                        aliasToIndex,
                        joinFields);
                    return evalExpression(parsedExpression, projectedRow);
                },
                item.Desc,
                item.NullsFirst));
        }

        return keySelectors;
    }

    private static bool TryCreateOrdinalSelector(
        TableResultMock result,
        SqlOrderByItem item,
        string raw,
        out OrderByKeySelector selector)
    {
        selector = default;
        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ordinal))
            return false;

        if (ordinal < 1)
            throw new InvalidOperationException("invalid: ORDER BY ordinal must be >= 1");

        var columnIndex = ordinal - 1;
        if (columnIndex >= result.Columns.Count)
            throw new InvalidOperationException($"invalid: ORDER BY ordinal {ordinal} out of range");

        selector = new OrderByKeySelector(
            row => row.TryGetValue(columnIndex, out var value) ? value : null,
            item.Desc,
            item.NullsFirst);
        return true;
    }

    private static bool TryCreateColumnSelector(
        TableResultMock result,
        SqlOrderByItem item,
        string raw,
        out OrderByKeySelector selector)
    {
        selector = default;
        var column = result.Columns.FirstOrDefault(current =>
            current.ColumnAlias.Equals(raw, StringComparison.OrdinalIgnoreCase)
            || current.ColumnName.Equals(raw, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            return false;

        var columnIndex = column.ColumIndex;
        selector = new OrderByKeySelector(
            row => row.TryGetValue(columnIndex, out var value) ? value : null,
            item.Desc,
            item.NullsFirst);
        return true;
    }

    private static Dictionary<string, int> BuildAliasToIndex(IList<TableResultColMock> columns)
    {
        var aliasToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            AddAlias(aliasToIndex, column.ColumnAlias, i);
            AddAlias(aliasToIndex, column.ColumnName, i);

            var tail = column.ColumnName;
            var dot = tail.LastIndexOf('.');
            if (dot >= 0 && dot + 1 < tail.Length)
                tail = tail[(dot + 1)..];

            AddAlias(aliasToIndex, tail, i);
        }

        return aliasToIndex;
    }

    private static void AddAlias(Dictionary<string, int> aliasToIndex, string alias, int index)
    {
        if (!string.IsNullOrWhiteSpace(alias) && !aliasToIndex.ContainsKey(alias))
            aliasToIndex[alias] = index;
    }

    private static Dictionary<Dictionary<int, object?>, Dictionary<string, object?>> BuildJoinFieldsByRow(TableResultMock result)
    {
        var joinFieldsByRow = new Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>(
            Math.Max(1, result.Count),
            ReferenceEqualityComparer<Dictionary<int, object?>>.Instance);

        for (var i = 0; i < result.Count && i < result.JoinFields.Count; i++)
            joinFieldsByRow[result[i]] = result.JoinFields[i];

        return joinFieldsByRow;
    }

    private static int CompareRows(
        Dictionary<int, object?> left,
        Dictionary<int, object?> right,
        List<OrderByKeySelector> keySelectors,
        Func<object?, object?, int> compareSql)
    {
        foreach (var selector in keySelectors)
        {
            var leftValue = selector.Get(left);
            var rightValue = selector.Get(right);
            var comparison = CompareValues(leftValue, rightValue, selector.Desc, selector.NullsFirst, compareSql);
            if (comparison != 0)
                return comparison;
        }

        return 0;
    }

    private static int CompareValues(
        object? leftValue,
        object? rightValue,
        bool descending,
        bool? nullsFirst,
        Func<object?, object?, int> compareSql)
    {
        var leftIsNull = IsNullish(leftValue);
        var rightIsNull = IsNullish(rightValue);
        if (leftIsNull || rightIsNull)
        {
            if (leftIsNull && rightIsNull)
                return 0;

            if (nullsFirst.HasValue)
                return leftIsNull ? (nullsFirst.Value ? -1 : 1) : (nullsFirst.Value ? 1 : -1);

            return leftIsNull ? (descending ? 1 : -1) : (descending ? -1 : 1);
        }

        var comparison = compareSql(leftValue, rightValue);
        return descending ? -comparison : comparison;
    }

    private static void ReorderJoinFields(
        TableResultMock result,
        List<Dictionary<int, object?>> sortedRows,
        Dictionary<Dictionary<int, object?>, Dictionary<string, object?>> joinFieldsByRow)
    {
        if (result.JoinFields.Count == 0)
            return;

        result.JoinFields = [.. sortedRows
            .Select(row => joinFieldsByRow.TryGetValue(row, out var joinFields)
                ? joinFields
                : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase))];
    }

    private static bool IsNullish(object? value)
        => value is null or DBNull;

    private readonly record struct OrderByKeySelector(
        Func<Dictionary<int, object?>, object?> Get,
        bool Desc,
        bool? NullsFirst);
}
