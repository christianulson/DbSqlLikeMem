namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private long EvalCountDistinct(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        bool useOrdinalTextComparison)
    {
        // COUNT(DISTINCT *) não faz sentido no MySQL; se acontecer, trata como COUNT(*)
        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
            return group.Rows.Count;

        var set = CreateDistinctStringSet(useOrdinalTextComparison, group.Rows.Count);
        foreach (var row in group.Rows)
        {
            if (TryBuildCountDistinctKey(fn, row, ctes, useOrdinalTextComparison, out var key))
                set.Add(key);
        }

        return set.Count;
    }

    private bool TryBuildCountDistinctKey(
        CallExpr fn,
        EvalRow row,
        IDictionary<string, Source> ctes,
        bool useOrdinalTextComparison,
        out string key)
    {
        key = string.Empty;

        if (fn.Args.Count == 1)
        {
            var singleValue = Eval(fn.Args[0], row, null, ctes);
            if (!TryGetStringAggregateKeyAndText(singleValue, useOrdinalTextComparison, out _, out var singleKey))
                return false;

            key = singleKey;
            return true;
        }

        var builder = new StringBuilder();
        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = Eval(fn.Args[i], row, null, ctes);
            if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out _, out var normalized))
                return false;

            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(normalized);
        }

        key = builder.ToString();
        return true;
    }

    private static HashSet<string> CreateDistinctStringSet(bool useOrdinalTextComparison, int estimatedCount)
    {
        var comparer = useOrdinalTextComparison ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;
        return new HashSet<string>(comparer);
    }

    private static bool TryGetStringAggregateText(object? value, out string text)
    {
        text = string.Empty;

        if (IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                return true;
        }
    }

    private static bool TryGetStringAggregateKeyAndText(
        object? value,
        bool useOrdinalTextComparison,
        out string text,
        out string distinctKey)
    {
        text = string.Empty;
        distinctKey = string.Empty;

        if (IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                distinctKey = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                distinctKey = text;
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                distinctKey = text;
                return true;
        }
    }
}
