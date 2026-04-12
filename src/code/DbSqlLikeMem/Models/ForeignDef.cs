namespace DbSqlLikeMem.Models;

public sealed record Foreign(
    string name,
    string RefTableName,
    HashSet<(string col, string refCol)> references);


public sealed class ForeignDef
{
    private ForeignLookupPlan? _childLookupPlan;
    private int _childLookupPlanVersion = -1;
    private ForeignLookupPlan? _refLookupPlan;
    private int _refLookupPlanVersion = -1;

    internal ForeignDef(
        ITableMock table,
        string name,
        ITableMock refTable,
        HashSet<(ColumnDef col, ColumnDef refCol)> references
        )
    {
        Table = table;
        Name = name;
        RefTable = refTable;
        References = references;
    }

    /// <summary>
    /// EN: Parent table.
    /// PT: Tabela pai.
    /// </summary>
    public ITableMock Table { get; private set; }

    public string Name { get; private set; }

    public ITableMock RefTable { get; private set; }

    public HashSet<(ColumnDef col, ColumnDef refCol)> References { get; private set; }

    internal bool TryGetChildLookupPlan(out ForeignLookupPlan plan)
        => TryGetLookupPlan(
            Table,
            useReferenceColumnsAsSource: true,
            ref _childLookupPlanVersion,
            ref _childLookupPlan,
            out plan);

    internal bool TryGetRefLookupPlan(out ForeignLookupPlan plan)
        => TryGetLookupPlan(
            RefTable,
            useReferenceColumnsAsSource: false,
            ref _refLookupPlanVersion,
            ref _refLookupPlan,
            out plan);

    private bool TryGetLookupPlan(
        ITableMock targetTable,
        bool useReferenceColumnsAsSource,
        ref int cachedVersion,
        ref ForeignLookupPlan? cachedPlan,
        out ForeignLookupPlan plan)
    {
        if (targetTable is TableMock targetTableMock)
        {
            var currentVersion = targetTableMock.IndexVersion;
            if (cachedPlan is { } currentPlan && cachedVersion == currentVersion)
            {
                plan = currentPlan;
                return true;
            }

            if (TryBuildLookupPlan(targetTable, useReferenceColumnsAsSource, out plan))
            {
                cachedPlan = plan;
                cachedVersion = currentVersion;
                return true;
            }

            cachedPlan = null;
            cachedVersion = currentVersion;
            plan = default;
            return false;
        }

        return TryBuildLookupPlan(targetTable, useReferenceColumnsAsSource, out plan);
    }

    private bool TryBuildLookupPlan(
        ITableMock targetTable,
        bool useReferenceColumnsAsSource,
        out ForeignLookupPlan plan)
    {
        IndexDef? matchingIndex = null;
        var matchingKeyCount = -1;
        var targetColumnsByName = new Dictionary<string, int>(References.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var reference in References)
        {
            var targetColumn = useReferenceColumnsAsSource ? reference.col : reference.refCol;
            var sourceColumnIndex = useReferenceColumnsAsSource ? reference.refCol.Index : reference.col.Index;
            targetColumnsByName[targetColumn.Name.NormalizeName()] = sourceColumnIndex;
        }

        var indexes = targetTable is TableMock targetTableMock
            ? targetTableMock.IndexesRaw.Values
            : targetTable.Indexes.Values;

        foreach (var index in indexes)
        {
            var coversAllReferences = true;
            foreach (var reference in References)
            {
                var targetColumnName = useReferenceColumnsAsSource
                    ? reference.col.Name
                    : reference.refCol.Name;

                if (!index.KeyCols.Contains(targetColumnName, StringComparer.OrdinalIgnoreCase))
                {
                    coversAllReferences = false;
                    break;
                }
            }

            if (!coversAllReferences || index.KeyCols.Count <= matchingKeyCount)
                continue;

            matchingIndex = index;
            matchingKeyCount = index.KeyCols.Count;
        }

        if (matchingIndex is null)
        {
            plan = default;
            return false;
        }

        var sourceColumnIndexes = new int[matchingIndex.KeyCols.Count];
        for (var i = 0; i < matchingIndex.KeyCols.Count; i++)
        {
            if (!targetColumnsByName.TryGetValue(matchingIndex.KeyCols[i].NormalizeName(), out var sourceColumnIndex))
                sourceColumnIndex = -1;

            sourceColumnIndexes[i] = sourceColumnIndex;
        }

        plan = new ForeignLookupPlan(matchingIndex, sourceColumnIndexes);
        return true;
    }

    internal readonly record struct ForeignLookupPlan(IndexDef Index, int[] SourceColumnIndexes)
    {
        internal IndexKey BuildKey(IReadOnlyDictionary<int, object?> row)
        {
            return SourceColumnIndexes.Length switch
            {
                1 => new IndexKey(ReadValue(row, SourceColumnIndexes[0])),
                2 => new IndexKey(ReadValue(row, SourceColumnIndexes[0]), ReadValue(row, SourceColumnIndexes[1])),
                3 => new IndexKey(ReadValue(row, SourceColumnIndexes[0]), ReadValue(row, SourceColumnIndexes[1]), ReadValue(row, SourceColumnIndexes[2])),
                _ => BuildKeySlow(row),
            };
        }

        private IndexKey BuildKeySlow(IReadOnlyDictionary<int, object?> row)
        {
            var values = new object?[SourceColumnIndexes.Length];
            for (var i = 0; i < SourceColumnIndexes.Length; i++)
                values[i] = ReadValue(row, SourceColumnIndexes[i]);

            return new IndexKey(values);
        }

        private static object? ReadValue(IReadOnlyDictionary<int, object?> row, int index)
            => index >= 0 && row.TryGetValue(index, out var value) ? value : null;
    }
}
