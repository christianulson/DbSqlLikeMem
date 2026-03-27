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

        if (equalities.Count == 1)
        {
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

        var resolvedEqualities = new (string QualifiedColumnName, object? Value)[equalities.Count];
        var resolvedEqualitiesCount = 0;
        foreach (var kv in equalities)
        {
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                return false;
            }

            resolvedEqualities[resolvedEqualitiesCount++] = (qualifiedColumnName!, kv.Value);
        }

        foreach (var rawRow in src.Rows())
        {
            var matches = true;
            for (var i = 0; i < resolvedEqualitiesCount; i++)
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
}
