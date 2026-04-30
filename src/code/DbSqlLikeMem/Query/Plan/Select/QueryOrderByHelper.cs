namespace DbSqlLikeMem;

internal static class QueryOrderByHelper
{
    internal static bool TryApplyOrder(
        this QueryExecutionContext context,
        TableResultMock result,
        IReadOnlyList<SqlOrderByItem> orderBy,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalExpression)
    {
        Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>? joinFieldsByRow = null;
        var keySelectors = context.BuildKeySelectors(
            result,
            orderBy,
            parseExpression,
            evalExpression,
            () => joinFieldsByRow ??= BuildJoinFieldsByRow(result));
        if (keySelectors.Count == 0)
            return false;

        var sortedRows = new List<Dictionary<int, object?>>(result.Count);
        for (var i = 0; i < result.Count; i++)
            sortedRows.Add(result[i]);
        sortedRows.Sort((left, right) => context.CompareRows(left, right, keySelectors));

        if (result.JoinFields.Count > 0 && joinFieldsByRow is null)
            joinFieldsByRow = BuildJoinFieldsByRow(result);

        result.Clear();
        for (var i = 0; i < sortedRows.Count; i++)
            result.Add(sortedRows[i]);

        if (joinFieldsByRow is not null)
            ReorderJoinFields(result, sortedRows, joinFieldsByRow);
        return true;
    }

    internal static TableResultMock ApplyDistinctOn(
        this QueryExecutionContext context,
        TableResultMock result,
        IReadOnlyList<string> distinctOn,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalExpression)
    {
        if (distinctOn.Count == 0 || result.Count <= 1)
            return result;

        Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>? joinFieldsByRow = null;
        var keySelectors = context.BuildKeySelectors(
            result,
            [.. distinctOn.Select(raw => new SqlOrderByItem(raw, false, null))],
            parseExpression,
            evalExpression,
            () => joinFieldsByRow ??= BuildJoinFieldsByRow(result));
        if (keySelectors.Count == 0)
            return result;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outputRows = new List<Dictionary<int, object?>>(result.Count);

        foreach (var row in result)
        {
            var key = BuildDistinctOnRowKey(context, row, keySelectors);
            if (seen.Add(key))
                outputRows.Add(row);
        }

        result.Clear();
        foreach (var row in outputRows)
            result.Add(row);

        if (joinFieldsByRow is not null)
            ReorderJoinFields(result, outputRows, joinFieldsByRow);

        return result;
    }

    private static List<OrderByKeySelector> BuildKeySelectors(
        this QueryExecutionContext context,
        TableResultMock result,
        IReadOnlyList<SqlOrderByItem> orderBy,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, object?> evalExpression,
        Func<Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>> getJoinFieldsByRow)
    {
        var keySelectors = new List<OrderByKeySelector>(orderBy.Count);
        var aliasToIndex = BuildAliasToIndex(result.Columns);

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

            if (TryCreateColumnSelector(result, item, raw, aliasToIndex, out var columnSelector))
            {
                keySelectors.Add(columnSelector);
                continue;
            }

            var parsedExpression = parseExpression(raw);
            keySelectors.Add(new OrderByKeySelector(
                row =>
                {
                    using var positionalScope = context.BeginPositionalParameterScope();
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
        Dictionary<string, int> aliasToIndex,
        out OrderByKeySelector selector)
    {
        selector = default;
        TableResultColMock? column = null;
        for (var i = 0; i < result.Columns.Count; i++)
        {
            var current = result.Columns[i];
            if (current.ColumnAlias.Equals(raw, StringComparison.OrdinalIgnoreCase)
                || current.ColumnName.Equals(raw, StringComparison.OrdinalIgnoreCase))
            {
                column = current;
                break;
            }
        }
        if (column is null && aliasToIndex.TryGetValue(raw, out var resolvedIndex))
        {
            selector = new OrderByKeySelector(
                row => row.TryGetValue(resolvedIndex, out var value) ? value : null,
                item.Desc,
                item.NullsFirst);
            return true;
        }

        if (column is null)
            return false;

        var columnIndex = column.ColumIndex;
        selector = new OrderByKeySelector(
            row => row.TryGetValue(columnIndex, out var value) ? value : null,
            item.Desc,
            item.NullsFirst);
        return true;
    }

    private static string BuildDistinctOnRowKey(
        QueryExecutionContext context,
        Dictionary<int, object?> row,
        List<OrderByKeySelector> keySelectors)
    {
        var builder = new StringBuilder(Math.Max(16, keySelectors.Count * 12));
        foreach (var selector in keySelectors)
        {
            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(context.NormalizeDistinctKey(selector.Get(row)));
        }

        return builder.ToString();
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
        this QueryExecutionContext context,
        Dictionary<int, object?> left,
        Dictionary<int, object?> right,
        List<OrderByKeySelector> keySelectors)
    {
        foreach (var selector in keySelectors)
        {
            var leftValue = selector.Get(left);
            var rightValue = selector.Get(right);
            var comparison = context.CompareValues(leftValue, rightValue, selector.Desc, selector.NullsFirst);
            if (comparison != 0)
                return comparison;
        }

        return 0;
    }

    private static int CompareValues(
        this QueryExecutionContext context,
        object? leftValue,
        object? rightValue,
        bool descending,
        bool? nullsFirst)
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

        var comparison = context.CompareSql(leftValue, rightValue);
        return descending ? -comparison : comparison;
    }

    private static void ReorderJoinFields(
        TableResultMock result,
        List<Dictionary<int, object?>> sortedRows,
        Dictionary<Dictionary<int, object?>, Dictionary<string, object?>> joinFieldsByRow)
    {
        if (result.JoinFields.Count == 0)
            return;

        var reorderedJoinFields = new List<Dictionary<string, object?>>(sortedRows.Count);
        for (var i = 0; i < sortedRows.Count; i++)
        {
            var row = sortedRows[i];
            if (joinFieldsByRow.TryGetValue(row, out var joinFields))
            {
                reorderedJoinFields.Add(joinFields);
                continue;
            }

            reorderedJoinFields.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));
        }

        result.JoinFields = reorderedJoinFields;
    }

    private static bool IsNullish(object? value)
        => value is null or DBNull;

    private readonly record struct OrderByKeySelector(
        Func<Dictionary<int, object?>, object?> Get,
        bool Desc,
        bool? NullsFirst);
}
