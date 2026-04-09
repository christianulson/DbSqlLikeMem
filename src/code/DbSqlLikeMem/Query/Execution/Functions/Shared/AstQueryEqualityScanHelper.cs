using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryEqualityScanHelper
{
    internal static bool TryCountRows(
        this QueryExecutionContext context,
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        out long count)
    {
        count = 0;

        if (equalities.Count == 0)
            return false;

        if (TryCountRowsWithSingleEquality(context, src, equalities, out count))
            return true;

        return TryCountRowsWithMultipleEqualities(context, src, equalities, out count);
    }

    private static bool TryCountRowsWithSingleEquality(
        QueryExecutionContext context,
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        out long count)
    {
        count = 0;
        if (equalities.Count != 1)
            return false;

        foreach (var kv in equalities)
        {
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                return false;
            }

            var qualifiedName = qualifiedColumnName ?? string.Empty;
            foreach (var rawRow in src.Rows())
            {
                if (rawRow.TryGetValue(qualifiedName, out var actualValue)
                    && actualValue.EqualsSql(kv.Value, context))
                {
                    count++;
                }
            }
        }

        return true;
    }

    private static bool TryCountRowsWithMultipleEqualities(
        QueryExecutionContext context,
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        out long count)
    {
        count = 0;
        if (equalities.Count <= 1)
            return false;

        if (!TryResolveQualifiedEqualities(src, equalities, out var resolvedEqualities))
            return false;

        foreach (var rawRow in src.Rows())
        {
            var matches = true;
            for (var i = 0; i < resolvedEqualities.Count; i++)
            {
                var equality = resolvedEqualities[i];
                if (!rawRow.TryGetValue(equality.QualifiedColumnName, out var actualValue)
                    || !actualValue.EqualsSql(equality.Value, context))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                count++;
        }

        return true;
    }

    private static bool TryResolveQualifiedEqualities(
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        out List<(string QualifiedColumnName, object? Value)> resolvedEqualities)
    {
        resolvedEqualities = new List<(string QualifiedColumnName, object? Value)>(equalities.Count);
        foreach (var kv in equalities)
        {
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                resolvedEqualities = [];
                return false;
            }

            resolvedEqualities.Add((qualifiedColumnName!, kv.Value));
        }

        return true;
    }
}
