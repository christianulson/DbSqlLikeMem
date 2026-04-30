using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryIndexHelper(
    Func<SqlExpr, Source, Dictionary<string, object?>?> collectColumnEqualities,
    Action incrementIndexLookupMetric,
    Action<string> incrementIndexHintMetric,
    Action<ITableMock, MySqlIndexHintPlan?> recordPrimaryKeyHintMetric)
{
    private readonly Func<SqlExpr, Source, Dictionary<string, object?>?> _collectColumnEqualities = collectColumnEqualities;
    private readonly Action _incrementIndexLookupMetric = incrementIndexLookupMetric;
    private readonly Action<string> _incrementIndexHintMetric = incrementIndexHintMetric;
    private readonly Action<ITableMock, MySqlIndexHintPlan?> _recordPrimaryKeyHintMetric = recordPrimaryKeyHintMetric;

    internal IEnumerable<Dictionary<string, object?>>? TryRowsFromIndex(
        Source src,
        SqlTableSource? tableSource,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (src.Physical is null)
            return null;

        var hintPlan = BuildMySqlIndexHintPlan(tableSource?.MySqlIndexHints, src.Physical, hasOrderBy, hasGroupBy);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");

        if (where is null)
            return null;

        var equalsByColumn = _collectColumnEqualities(where, src);
        if (equalsByColumn is null || equalsByColumn.Count == 0)
            return null;

        if (TryRowsFromPrimaryKey(src, hintPlan, equalsByColumn, out var pkRows))
            return pkRows;

        IndexDef? best = null;
        foreach (var ix in src.Physical is TableMock tableMockPhysical ? tableMockPhysical.IndexesRaw.Values : src.Physical.Indexes.Values)
        {
            var keyCols = ix.KeyCols;
            var keyColCount = keyCols.Count;
            if (keyColCount == 0)
                continue;

            if (hintPlan is not null && !hintPlan.AllowedIndexNames.Contains(ix.Name.NormalizeName()))
                continue;

            var coversAll = true;
            for (var i = 0; i < keyColCount; i++)
            {
                if (!equalsByColumn.ContainsKey(keyCols[i].NormalizeName()))
                {
                    coversAll = false;
                    break;
                }
            }

            if (!coversAll)
                continue;

            if (best is null || keyColCount > best.KeyCols.Count)
                best = ix;
        }

        if (best is null)
            return null;

        IndexKey key;
        if (src.Physical is TableMock)
        {
            key = best.BuildIndexKeyFromValues(equalsByColumn);
        }
        else
        {
            var keyCols = best.KeyCols;
            var keyValues = new object?[keyCols.Count];
            for (var i = 0; i < keyCols.Count; i++)
            {
                var norm = keyCols[i].NormalizeName();
                keyValues[i] = equalsByColumn[norm]?.ToString() ?? "<null>";
            }

            key = new IndexKey(keyValues);
        }

        var positions = LookupIndexWithMetrics(src.Physical, best, key);
        if (positions is null || positions.Count == 0)
            return null;

        return src.RowsByIndexes(positions.Keys);
    }

    internal bool TryCountRowsFromIndex(
        Source src,
        SqlTableSource? tableSource,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy,
        out long count)
    {
        count = 0;

        if (src.Physical is null)
            return false;

        var hintPlan = BuildMySqlIndexHintPlan(tableSource?.MySqlIndexHints, src.Physical, hasOrderBy, hasGroupBy);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");

        if (where is null)
            return false;

        var equalsByColumn = _collectColumnEqualities(where, src);
        if (equalsByColumn is null || equalsByColumn.Count == 0)
            return false;

        if (TryCountRowsFromPrimaryKey(src, hintPlan, equalsByColumn, out count))
            return true;

        IndexDef? best = null;
        foreach (var ix in src.Physical is TableMock tableMockPhysical ? tableMockPhysical.IndexesRaw.Values : src.Physical.Indexes.Values)
        {
            var keyCols = ix.KeyCols;
            var keyColCount = keyCols.Count;
            if (keyColCount == 0)
                continue;

            if (hintPlan is not null && !hintPlan.AllowedIndexNames.Contains(ix.Name.NormalizeName()))
                continue;

            var coversAll = true;
            for (var i = 0; i < keyColCount; i++)
            {
                if (!equalsByColumn.ContainsKey(keyCols[i].NormalizeName()))
                {
                    coversAll = false;
                    break;
                }
            }

            if (!coversAll)
                continue;

            if (best is null || keyColCount > best.KeyCols.Count)
                best = ix;
        }

        if (best is null)
            return false;

        IndexKey key;
        if (src.Physical is TableMock)
        {
            key = best.BuildIndexKeyFromValues(equalsByColumn);
        }
        else
        {
            var keyCols = best.KeyCols;
            var keyValues = new object?[keyCols.Count];
            for (var i = 0; i < keyCols.Count; i++)
            {
                var norm = keyCols[i].NormalizeName();
                keyValues[i] = equalsByColumn[norm]?.ToString() ?? "<null>";
            }

            key = new IndexKey(keyValues);
        }

        var positions = LookupIndexWithMetrics(src.Physical, best, key);
        if (positions is null || positions.Count == 0)
        {
            count = 0;
            return true;
        }

        count = src.CountRowsByIndexes(positions.Keys);
        return true;
    }

    internal static MySqlIndexHintPlan? BuildMySqlIndexHintPlan(
        IReadOnlyList<SqlMySqlIndexHint>? hints,
        ITableMock table,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (hints is null || hints.Count == 0)
            return null;

        var allIndexes = table is TableMock tableMock ? tableMock.IndexesRaw.Values : table.Indexes.Values;
        var existingIndexNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var index in allIndexes)
            existingIndexNames.Add(index.Name.NormalizeName());

        var primaryEquivalentIndexNames = ResolvePrimaryEquivalentIndexNames(table, allIndexes);

        var missingForced = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var allowedNames = new HashSet<string>(existingIndexNames, StringComparer.OrdinalIgnoreCase);
        var hasRowAccessHints = false;

        foreach (var hint in hints)
        {
            var appliesToForce = hint.Kind == SqlMySqlIndexHintKind.Force
                && (hint.Scope == SqlMySqlIndexHintScope.Any
                    || hint.Scope == SqlMySqlIndexHintScope.Join
                    || (hint.Scope == SqlMySqlIndexHintScope.OrderBy && hasOrderBy)
                    || (hint.Scope == SqlMySqlIndexHintScope.GroupBy && hasGroupBy));

            if (appliesToForce)
            {
                var normalizedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in ExpandHintIndexNames(hint.IndexNames, primaryEquivalentIndexNames))
                    normalizedNames.Add(item);

                foreach (var item in normalizedNames)
                {
                    if (!existingIndexNames.Contains(item))
                        missingForced.Add(item);
                }
            }

            if (hint.Scope is SqlMySqlIndexHintScope.Any or SqlMySqlIndexHintScope.Join)
            {
                hasRowAccessHints = true;

                var normalizedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in ExpandHintIndexNames(hint.IndexNames, primaryEquivalentIndexNames))
                    normalizedNames.Add(item);

                if (hint.Kind is SqlMySqlIndexHintKind.Use or SqlMySqlIndexHintKind.Force)
                {
                    allowedNames.IntersectWith(normalizedNames);
                }
                else if (hint.Kind == SqlMySqlIndexHintKind.Ignore)
                {
                    allowedNames.ExceptWith(normalizedNames);
                }
            }
        }

        return new MySqlIndexHintPlan(
            allowedNames,
            missingForced,
            hasRowAccessHints,
            primaryEquivalentIndexNames);
    }

    internal void RecordPrimaryKeyHintMetric(ITableMock table, MySqlIndexHintPlan? hintPlan)
        => _recordPrimaryKeyHintMetric(table, hintPlan);

    private bool TryRowsFromPrimaryKey(
        Source src,
        MySqlIndexHintPlan? hintPlan,
        IReadOnlyDictionary<string, object?> equalsByColumn,
        out IEnumerable<Dictionary<string, object?>>? rows)
    {
        rows = null;

        if (src.Physical is not TableMock tableMock)
            return false;

        var pkIndexes = tableMock.PkIndexArray;
        if (pkIndexes.Length == 0)
            return false;

        bool TryGetPkValue(int pkIdx, out object? value)
        {
            var pkColumnName = tableMock.GetColumnByIndex(pkIdx).Name;
            var normalizedColumn = pkColumnName.NormalizeName();
            if (!equalsByColumn.TryGetValue(normalizedColumn, out var resolvedValue))
            {
                value = null;
                return false;
            }

            value = resolvedValue;
            return true;
        }

        _recordPrimaryKeyHintMetric(tableMock, hintPlan);
        _incrementIndexLookupMetric();

        switch (pkIndexes.Length)
        {
            case 1:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue0))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue0, out var rowIndex))
                {
                    rows = [];
                    return true;
                }

                rows = src.RowsByIndexes(rowIndex);
                return true;
            case 2:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue1)
                    || !TryGetPkValue(pkIndexes[1], out var pkValue2))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue1, pkValue2, out var rowIndex2))
                {
                    rows = [];
                    return true;
                }

                rows = src.RowsByIndexes(rowIndex2);
                return true;
            case 3:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue3)
                    || !TryGetPkValue(pkIndexes[1], out var pkValue4)
                    || !TryGetPkValue(pkIndexes[2], out var pkValue5))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue3, pkValue4, pkValue5, out var rowIndex3))
                {
                    rows = [];
                    return true;
                }

                rows = src.RowsByIndexes(rowIndex3);
                return true;
            default:
                var pkValues = new object?[pkIndexes.Length];
                for (var i = 0; i < pkIndexes.Length; i++)
                {
                    if (!TryGetPkValue(pkIndexes[i], out var pkValue))
                    {
                        return false;
                    }

                    pkValues[i] = pkValue;
                }

                if (!tableMock.TryFindRowByPkValues(pkValues, out var rowIndex4))
                {
                    rows = [];
                    return true;
                }

                rows = src.RowsByIndexes(rowIndex4);
                return true;
        }
    }

    private bool TryCountRowsFromPrimaryKey(
        Source src,
        MySqlIndexHintPlan? hintPlan,
        IReadOnlyDictionary<string, object?> equalsByColumn,
        out long count)
    {
        count = 0;

        if (src.Physical is not TableMock tableMock)
            return false;

        var pkIndexes = tableMock.PkIndexArray;
        if (pkIndexes.Length == 0)
            return false;

        bool TryGetPkValue(int pkIdx, out object? value)
        {
            var pkColumnName = tableMock.GetColumnByIndex(pkIdx).Name;
            var normalizedColumn = pkColumnName.NormalizeName();
            if (!equalsByColumn.TryGetValue(normalizedColumn, out var resolvedValue))
            {
                value = null;
                return false;
            }

            value = resolvedValue;
            return true;
        }

        _recordPrimaryKeyHintMetric(tableMock, hintPlan);
        _incrementIndexLookupMetric();

        switch (pkIndexes.Length)
        {
            case 1:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue0))
                    return false;

                if (!tableMock.TryFindRowByPkValues(pkValue0, out var rowIndex))
                {
                    count = 0;
                    return true;
                }

                count = src.CountRowsByIndex(rowIndex);
                return true;
            case 2:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue1)
                    || !TryGetPkValue(pkIndexes[1], out var pkValue2))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue1, pkValue2, out var rowIndex2))
                {
                    count = 0;
                    return true;
                }

                count = src.CountRowsByIndex(rowIndex2);
                return true;
            case 3:
                if (!TryGetPkValue(pkIndexes[0], out var pkValue3)
                    || !TryGetPkValue(pkIndexes[1], out var pkValue4)
                    || !TryGetPkValue(pkIndexes[2], out var pkValue5))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue3, pkValue4, pkValue5, out var rowIndex3))
                {
                    count = 0;
                    return true;
                }

                count = src.CountRowsByIndex(rowIndex3);
                return true;
            default:
                var pkValues = new object?[pkIndexes.Length];
                for (var i = 0; i < pkIndexes.Length; i++)
                {
                    if (!TryGetPkValue(pkIndexes[i], out var pkValue))
                        return false;

                    pkValues[i] = pkValue;
                }

                if (!tableMock.TryFindRowByPkValues(pkValues, out var rowIndex4))
                {
                    count = 0;
                    return true;
                }

                count = src.CountRowsByIndex(rowIndex4);
                return true;
        }
    }

    private IReadOnlyDictionary<int, Dictionary<string, object?>>? LookupIndexWithMetrics(
        ITableMock table,
        IndexDef indexDef,
        IndexKey key)
    {
        _incrementIndexLookupMetric();
        _incrementIndexHintMetric(indexDef.Name.NormalizeName());
        return indexDef.LookupMutable(key);
    }

    private static IEnumerable<string> ExpandHintIndexNames(
        IReadOnlyList<string> hintedNames,
        IReadOnlyHashSet<string> primaryEquivalentIndexNames)
    {
        foreach (var hintedName in hintedNames)
        {
            var normalized = hintedName.NormalizeName();
            if (!normalized.Equals("primary", StringComparison.OrdinalIgnoreCase))
            {
                yield return normalized;
                continue;
            }

            if (primaryEquivalentIndexNames.Count == 0)
            {
                yield return "primary";
                continue;
            }

            foreach (var item in primaryEquivalentIndexNames)
                yield return item;
        }
    }

    private static IReadOnlyHashSet<string> ResolvePrimaryEquivalentIndexNames(
        ITableMock table,
        IEnumerable<IndexDef> allIndexes)
    {
        var primaryKeyIndexes = table.PrimaryKeyIndexes;
        var pkColumnNames = new List<string>(primaryKeyIndexes.Count);
        if (table is TableMock tableMock)
        {
            foreach (var pkIdx in tableMock.PkIndexArray)
            {
                pkColumnNames.Add(tableMock.GetColumnByIndex(pkIdx).Name.NormalizeName());
            }
        }
        else
        {
            foreach (var pkIdx in primaryKeyIndexes)
            {
                foreach (var column in table.Columns)
                {
                    if (column.Value.Index != pkIdx)
                        continue;

                    if (!string.IsNullOrWhiteSpace(column.Key))
                        pkColumnNames.Add(column.Key.NormalizeName());
                    break;
                }
            }
        }

        if (pkColumnNames.Count == 0)
            return new ReadOnlyHashSet<string>();

        var primaryEquivalent = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var index in allIndexes)
        {
            if (index.KeyCols.Count != pkColumnNames.Count)
                continue;

            var matches = true;
            var pkOrdinal = 0;
            foreach (var col in index.KeyCols)
            {
                if (!string.Equals(col.NormalizeName(), pkColumnNames[pkOrdinal], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }

                pkOrdinal++;
            }

            if (matches)
                primaryEquivalent.Add(index.Name.NormalizeName());
        }

        return new ReadOnlyHashSet<string>(primaryEquivalent);
    }
}
