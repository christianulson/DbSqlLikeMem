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
        foreach (var ix in src.Physical.Indexes.Values)
        {
            if (ix.KeyCols.Count == 0)
                continue;

            if (hintPlan is not null && !hintPlan.AllowedIndexNames.Contains(ix.Name.NormalizeName()))
                continue;

            var coversAll = ix.KeyCols.All(col => equalsByColumn.ContainsKey(col.NormalizeName()));
            if (!coversAll)
                continue;

            if (best is null || ix.KeyCols.Count > best.KeyCols.Count)
                best = ix;
        }

        if (best is null)
            return null;

        var key = src.Physical is TableMock
            ? best.BuildIndexKeyFromValues(equalsByColumn)
            : new IndexKey(best.KeyCols.Select(col =>
            {
                var norm = col.NormalizeName();
                var value = equalsByColumn[norm];
                return value?.ToString() ?? "<null>";
            }));

        var positions = LookupIndexWithMetrics(src.Physical, best, key);
        if (positions is null)
            return [];

        return src.RowsByIndexes(positions.Keys);
    }

    internal static MySqlIndexHintPlan? BuildMySqlIndexHintPlan(
        IReadOnlyList<SqlMySqlIndexHint>? hints,
        ITableMock table,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (hints is null || hints.Count == 0)
            return null;

        var allIndexes = table.Indexes.Values;
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

        var primaryKeyIndexes = tableMock.PrimaryKeyIndexes;
        if (primaryKeyIndexes.Count == 0)
            return false;

        var pkValues = new Dictionary<int, object?>(primaryKeyIndexes.Count);
        foreach (var pkIdx in primaryKeyIndexes)
        {
            if (!tableMock.ColumnsByIndex.TryGetValue(pkIdx, out var pkColumnName))
                return false;

            var normalizedColumn = pkColumnName.NormalizeName();
            if (!equalsByColumn.TryGetValue(normalizedColumn, out var value))
                return false;

            pkValues[pkIdx] = value;
        }

        _recordPrimaryKeyHintMetric(tableMock, hintPlan);
        _incrementIndexLookupMetric();
        if (!tableMock.TryFindRowByPk(pkValues, out var rowIndex))
        {
            rows = [];
            return true;
        }

        rows = src.RowsByIndexes(rowIndex);
        return true;
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
                if (!tableMock.ColumnsByIndex.TryGetValue(pkIdx, out var pkColumnName))
                    continue;

                pkColumnNames.Add(pkColumnName.NormalizeName());
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
