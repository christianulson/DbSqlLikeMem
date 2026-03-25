namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private string? EvalStringAggregate(
        IReadOnlyList<object?> values,
        object? separatorObj,
        string defaultSeparator)
    {
        if (values.Count == 0)
            return null;

        var separator = separatorObj?.ToString() ?? defaultSeparator;
        StringBuilder? builder = null;
        for (var i = 0; i < values.Count; i++)
        {
            if (!TryGetStringAggregateText(values[i], out var text))
                continue;

            if (builder is null)
            {
                builder = new StringBuilder(text.Length + separator.Length);
                builder.Append(text);
                continue;
            }

            builder.Append(separator);
            builder.Append(text);
        }

        return builder?.ToString();
    }

    private object? EvalStringAggregateForCallExpr(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var separator = GetAggregateSeparator(fn, group, ctes);
        return EvalSimpleStringAggregate(fn, group, ctes, separator, GetStringAggregateDefaultSeparator(name));
    }

    private object? GetAggregateSeparator(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => fn.Args.Count > 1 && group.Rows.Count > 0
            ? Eval(fn.Args[1], group.Rows[0], null, ctes)
            : null;

    private object? GetAggregateSeparator(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => fn.Args.Count > 1 && group.Rows.Count > 0
            ? Eval(fn.Args[1], group.Rows[0], null, ctes)
            : null;

    private static string GetStringAggregateDefaultSeparator(string name)
        => name == "LISTAGG" ? string.Empty : ",";

    private static int EstimateStringAggregateCapacity(int rowCount, int separatorLength)
    {
        if (rowCount <= 1)
            return 16;

        var estimated = rowCount * Math.Max(8, separatorLength + 6);
        return Math.Min(estimated, 64 * 1024);
    }

    private static int GetKnownRowCount(IEnumerable<EvalRow> rows, int defaultValue = 0)
    {
        if (rows is ICollection<EvalRow> collection)
            return collection.Count;

        if (rows is IReadOnlyCollection<EvalRow> readOnlyCollection)
            return readOnlyCollection.Count;

        return defaultValue;
    }

    private string? EvalSimpleStringAggregate(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        object? separatorObj,
        string defaultSeparator)
    {
        if (fn.Args.Count == 0)
            return null;

        var hasDirectValueSelector = TryCreateStringAggregateValueSelector(fn.Args[0], out var valueSelector);
        if (group.Rows.Count == 1)
        {
            var singleValue = hasDirectValueSelector
                ? valueSelector!(group.Rows[0])
                : Eval(fn.Args[0], group.Rows[0], null, ctes);
            if (!TryGetStringAggregateText(singleValue, out var singleText))
                return null;

            return singleText;
        }

        var separator = separatorObj?.ToString() ?? defaultSeparator;
        StringBuilder? builder = null;
        var hasValue = false;
        var estimatedCapacity = EstimateStringAggregateCapacity(group.Rows.Count, separator.Length);

        if (!hasDirectValueSelector)
        {
            for (var i = 0; i < group.Rows.Count; i++)
            {
                var value = Eval(fn.Args[0], group.Rows[i], null, ctes);
                if (IsNullish(value))
                    continue;

                var text = value?.ToString() ?? string.Empty;
                if (!hasValue)
                {
                    builder = new StringBuilder(Math.Max(estimatedCapacity, text.Length));
                    builder.Append(text);
                    hasValue = true;
                    continue;
                }

                builder!.Append(separator);
                builder.Append(text);
            }
        }
        else
        {
            for (var i = 0; i < group.Rows.Count; i++)
            {
                var value = valueSelector!(group.Rows[i]);
                if (IsNullish(value))
                    continue;

                var text = value?.ToString() ?? string.Empty;
                if (!hasValue)
                {
                    builder = new StringBuilder(Math.Max(estimatedCapacity, text.Length));
                    builder.Append(text);
                    hasValue = true;
                    continue;
                }

                builder!.Append(separator);
                builder.Append(text);
            }
        }

        return hasValue ? builder!.ToString() : null;
    }

    private static bool TryCreateStringAggregateValueSelector(
        SqlExpr expr,
        out Func<EvalRow, object?> selector)
    {
        switch (expr)
        {
            case ColumnExpr column:
                selector = row => ResolveColumn(column.Qualifier, column.Name, row);
                return true;
            case IdentifierExpr identifier:
                selector = row => ResolveIdentifier(identifier.Name, row);
                return true;
            case LiteralExpr literal:
                selector = _ => literal.Value;
                return true;
            default:
                selector = null!;
                return false;
        }
    }

    private bool TryEvalAggregateCount(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        out object? value)
    {
        if (name != "COUNT" && name != "COUNT_BIG")
        {
            value = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            value = (long)group.Rows.Count;
            return true;
        }

        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
        {
            value = (long)group.Rows.Count;
            return true;
        }

        long c = 0;
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v))
                c++;
        }

        value = c;
        return true;
    }
}
