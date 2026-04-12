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

        if (TryCountRowsWithExactPrimaryKeyEquality(src, equalities, out count))
            return true;

        if (TryCountRowsWithSingleEquality(context, src, equalities, out count))
            return true;

        return TryCountRowsWithMultipleEqualities(context, src, equalities, out count);
    }

    private static bool TryCountRowsWithExactPrimaryKeyEquality(
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        out long count)
    {
        count = 0;
        if (src.Physical is not TableMock table
            || table.PkIndexArray.Length == 0
            || !string.IsNullOrWhiteSpace(table.PartitionClauseSql)
            || equalities.Count != table.PkIndexArray.Length)
        {
            return false;
        }

        if (table.PkIndexArray.Length == 1)
        {
            var pkColumnName = table.GetColumnByIndex(table.PkIndexArray[0]).Name;
            if (!TryGetEqualityValue(src, equalities, pkColumnName, out var pkValue))
                return false;

            if (!table.TryFindRowByPkValues(pkValue, out _))
                return false;

            count = 1;
            return true;
        }

        var pkValues = new object?[table.PkIndexArray.Length];
        for (var i = 0; i < table.PkIndexArray.Length; i++)
        {
            var pkColumnName = table.GetColumnByIndex(table.PkIndexArray[i]).Name;
            if (!TryGetEqualityValue(src, equalities, pkColumnName, out pkValues[i]))
                return false;
        }

        if (!table.TryFindRowByPkValues(pkValues, out _))
            return false;

        count = 1;
        return true;
    }

    private static bool TryGetEqualityValue(
        Source src,
        IReadOnlyDictionary<string, object?> equalities,
        string expectedColumnName,
        out object? value)
    {
        if (!src.TryGetQualifiedColumnName(expectedColumnName, out var expectedQualifiedColumnName)
            || string.IsNullOrWhiteSpace(expectedQualifiedColumnName))
        {
            value = null;
            return false;
        }

        foreach (var kv in equalities)
        {
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                continue;
            }

            if (string.Equals(qualifiedColumnName, expectedQualifiedColumnName, StringComparison.OrdinalIgnoreCase))
            {
                value = kv.Value;
                return true;
            }
        }

        value = null;
        return false;
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

            if (TryCountRowsWithSingleEqualityFromSingleColumnIndex(src, qualifiedColumnName!, kv.Value, out count))
                return true;

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

    private static bool TryCountRowsWithSingleEqualityFromSingleColumnIndex(
        Source src,
        string qualifiedColumnName,
        object? value,
        out long count)
    {
        count = 0;
        if (src.Physical is not TableMock table
            || !string.IsNullOrWhiteSpace(table.PartitionClauseSql)
            || table.IndexesRaw.Count == 0)
        {
            return false;
        }

        foreach (var index in table.IndexesRaw.Values)
        {
            if (index.KeyCols.Count != 1)
                continue;

            if (!src.TryGetQualifiedColumnName(index.KeyCols[0], out var indexQualifiedColumnName)
                || string.IsNullOrWhiteSpace(indexQualifiedColumnName))
            {
                continue;
            }

            if (!string.Equals(indexQualifiedColumnName, qualifiedColumnName, StringComparison.OrdinalIgnoreCase))
                continue;

            var keyValues = new Dictionary<string, object?>(1, StringComparer.OrdinalIgnoreCase)
            {
                [index.KeyCols[0]] = value
            };

            var key = index.BuildIndexKeyFromValues(keyValues);
            count = index.LookupMutable(key)?.Count ?? 0;
            return true;
        }

        return false;
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
