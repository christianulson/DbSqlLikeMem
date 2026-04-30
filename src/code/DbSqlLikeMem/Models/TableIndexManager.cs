namespace DbSqlLikeMem;

internal sealed class TableIndexManager(TableMock table)
{
    internal void AddPrimaryKeyIndexes(params string[] columns)
    {
        foreach (var colName in columns)
            table.PrimaryKeyIndexesMutable.Add(table.ColumnsRaw[colName].Index);
        if (table.PrimaryKeyIndexesMutable.Count != columns.Length)
            throw new InvalidOperationException(SqlExceptionMessages.DuplicatePrimaryKeyColumns());

        table.SetPrimaryKeyIndexArray([.. table.PrimaryKeyIndexesMutable.OrderBy(static i => i)]);
        table.SetPrimaryKeyIndexesView(new ReadOnlyHashSet<int>(table.PrimaryKeyIndexesMutable));
        RebuildPkIndex();
    }

    internal IndexKey BuildPkKey(IReadOnlyDictionary<int, object?> row)
        => BuildPkKey(row, null, null);

    internal IndexKey BuildPkKey(
        IReadOnlyDictionary<int, object?> row,
        int? overrideIndex,
        object? overrideValue)
    {
        if (table.PkIndexArray.Length == 0)
            return default;

        if (table.PkIndexArray.Length == 1)
        {
            var pk0 = table.PkIndexArray[0];
            var v0 = overrideIndex == pk0
                ? overrideValue
                : row.TryGetValue(pk0, out var v) ? v : null;
            return new IndexKey(v0);
        }

        if (table.PkIndexArray.Length == 2)
        {
            var pk0 = table.PkIndexArray[0];
            var pk1 = table.PkIndexArray[1];
            var v0 = overrideIndex == pk0
                ? overrideValue
                : row.TryGetValue(pk0, out var vv0) ? vv0 : null;
            var v1 = overrideIndex == pk1
                ? overrideValue
                : row.TryGetValue(pk1, out var vv1) ? vv1 : null;
            return new IndexKey(v0, v1);
        }

        if (table.PkIndexArray.Length == 3)
        {
            var pk0 = table.PkIndexArray[0];
            var pk1 = table.PkIndexArray[1];
            var pk2 = table.PkIndexArray[2];
            var v0 = overrideIndex == pk0
                ? overrideValue
                : row.TryGetValue(pk0, out var vv0) ? vv0 : null;
            var v1 = overrideIndex == pk1
                ? overrideValue
                : row.TryGetValue(pk1, out var vv1) ? vv1 : null;
            var v2 = overrideIndex == pk2
                ? overrideValue
                : row.TryGetValue(pk2, out var vv2) ? vv2 : null;
            return new IndexKey(v0, v1, v2);
        }

        var values = new object?[table.PkIndexArray.Length];
        for (int i = 0; i < table.PkIndexArray.Length; i++)
        {
            var pkIndex = table.PkIndexArray[i];
            values[i] = overrideIndex == pkIndex
                ? overrideValue
                : row.TryGetValue(pkIndex, out var v) ? v : null;
        }

        return new IndexKey(values);
    }

    internal bool TryFindRowByPk(IReadOnlyDictionary<int, object?> row, out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0)
            return false;

        var key = BuildPkKey(row);
        return table.PrimaryKeyLookup.TryGetValue(key, out rowIndex);
    }

    internal bool TryFindRowByPkValues(object?[] values, out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0 || table.PkIndexArray.Length != values.Length)
            return false;

        return table.PrimaryKeyLookup.TryGetValue(new IndexKey(values), out rowIndex);
    }

    internal bool TryFindRowByPkValues(object? value, out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0 || table.PkIndexArray.Length != 1)
            return false;

        return table.PrimaryKeyLookup.TryGetValue(new IndexKey(value), out rowIndex);
    }

    internal bool TryFindRowByPkValues(object? v1, object? v2, out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0 || table.PkIndexArray.Length != 2)
            return false;

        return table.PrimaryKeyLookup.TryGetValue(new IndexKey(v1, v2), out rowIndex);
    }

    internal bool TryFindRowByPkValues(object? v1, object? v2, object? v3, out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0 || table.PkIndexArray.Length != 3)
            return false;

        return table.PrimaryKeyLookup.TryGetValue(new IndexKey(v1, v2, v3), out rowIndex);
    }

    internal void RebuildPkIndex()
    {
        table.PrimaryKeyLookup.Clear();
        if (table.PrimaryKeyIndexes.Count == 0)
            return;

        for (int i = 0; i < table.Count; i++)
            table.PrimaryKeyLookup[BuildPkKey(table[i])] = i;
    }

    internal void RegisterPrimaryKey(int rowIndex, IReadOnlyDictionary<int, object?> row)
    {
        if (table.PrimaryKeyIndexes.Count == 0)
            return;

        table.PrimaryKeyLookup[BuildPkKey(row)] = rowIndex;
    }

    internal void RegisterPrimaryKeys(int startIndex, IReadOnlyList<Dictionary<int, object?>> rows)
    {
        if (table.PrimaryKeyIndexes.Count == 0)
            return;

        for (var rowOffset = 0; rowOffset < rows.Count; rowOffset++)
            table.PrimaryKeyLookup[BuildPkKey(rows[rowOffset])] = startIndex + rowOffset;
    }

    internal void RemovePrimaryKey(int rowIndex, IReadOnlyDictionary<int, object?> row)
    {
        if (table.PrimaryKeyIndexes.Count == 0)
            return;

        var key = BuildPkKey(row);
        table.PrimaryKeyLookup.Remove(key);

        var keysToUpdate = new List<IndexKey>(table.PrimaryKeyLookup.Count);
        foreach (var entry in table.PrimaryKeyLookup)
        {
            if (entry.Value > rowIndex)
                keysToUpdate.Add(entry.Key);
        }

        foreach (var currentKey in keysToUpdate)
        {
            if (table.PrimaryKeyLookup.TryGetValue(currentKey, out var currentIndex))
                table.PrimaryKeyLookup[currentKey] = currentIndex - 1;
        }
    }

    internal void UpdatePrimaryKeyIfNeeded(
        int rowIndex,
        int changedColumnIndex,
        object? oldValue,
        IReadOnlyDictionary<int, object?> newRow)
    {
        if (table.PrimaryKeyIndexes.Count == 0 || !table.PrimaryKeyIndexes.Contains(changedColumnIndex))
            return;

        var oldKey = BuildPkKey(newRow, changedColumnIndex, oldValue);
        var newKey = BuildPkKey(newRow);
        if (!EqualityComparer<IndexKey>.Default.Equals(oldKey, newKey))
            table.PrimaryKeyLookup.Remove(oldKey);

        table.PrimaryKeyLookup[newKey] = rowIndex;
    }

    internal string BuildPrimaryKeyDescription(IReadOnlyDictionary<int, object?> row)
    {
        var parts = new List<string>(table.PkIndexArray.Length);
        foreach (var pkIdx in table.PkIndexArray)
        {
            var columnName = table.ColumnsByOrdinal[pkIdx].Name;
            parts.Add($"{columnName}: {(row.TryGetValue(pkIdx, out var value) ? value : null)}");
        }

        return string.Join(",", parts);
    }

    internal IndexDef CreateIndex(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentNullExceptionCompatible.ThrowIfNull(keyCols, nameof(keyCols));
        name = name.NormalizeName();
        if (table.IndexesMutable.ContainsKey(name))
            throw new InvalidOperationException(SqlExceptionMessages.IndexAlreadyExists(name));

        var normalizedKeyCols = keyCols is ICollection<string> keyColsCollection
            ? new List<string>(keyColsCollection.Count)
            : new List<string>();
        var seenKeyCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var keyCol in keyCols)
        {
            var normalizedKeyCol = keyCol.NormalizeName();
            if (!seenKeyCols.Add(normalizedKeyCol))
                throw new InvalidOperationException($"Index '{name}' cannot contain duplicate key columns.");

            normalizedKeyCols.Add(normalizedKeyCol);
        }

        foreach (var keyColumn in normalizedKeyCols)
            table.GetColumn(keyColumn);

        List<string>? normalizedIncludeCols = null;
        if (include is not null)
        {
            normalizedIncludeCols = include is ICollection<string> includeCollection
                ? new List<string>(includeCollection.Count)
                : [];
            var seenIncludeCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var includeCol in include)
            {
                var normalizedIncludeCol = includeCol.NormalizeName();
                if (!seenIncludeCols.Add(normalizedIncludeCol))
                    throw new InvalidOperationException($"Index '{name}' cannot contain duplicate include columns.");

                for (var i = 0; i < normalizedKeyCols.Count; i++)
                {
                    if (string.Equals(normalizedKeyCols[i], normalizedIncludeCol, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"Index '{name}' cannot include key columns redundantly.");
                }

                normalizedIncludeCols.Add(normalizedIncludeCol);
            }

            foreach (var includeColumn in normalizedIncludeCols)
                table.GetColumn(includeColumn);
        }

        var idx = new IndexDef(table, name, normalizedKeyCols, normalizedIncludeCols?.ToArray(), unique);
        table.IndexesMutable.Add(name, idx);
        if (unique)
            table.UniqueIndexesMutable.Add(idx);
        table.IndexVersionValue++;
        return idx;
    }

    internal void DropIndex(
        string name,
        bool ifExists = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        name = name.NormalizeName();

        if (table.IndexesMutable.Remove(name))
        {
            if (table.UniqueIndexesMutable.Count > 0)
                table.UniqueIndexesMutable.RemoveAll(index => string.Equals(index.Name, name, StringComparison.OrdinalIgnoreCase));

            table.IndexVersionValue++;
            return;
        }

        if (ifExists)
            return;

        throw new InvalidOperationException($"Index '{name}' does not exist.");
    }

    internal IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(
        IndexDef def,
        IndexKey key)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(def, nameof(def));
        if (!table.IndexesMutable.TryGetValue(def.Name.NormalizeName(), out var map))
            return null;

        return map.Lookup(key);
    }

    internal void RemoveRowFromIndexes(int rowIdx, IReadOnlyDictionary<int, object?> row)
    {
        if (table.IndexesMutable.Count == 0)
            return;

        foreach (var idx in table.IndexesMutable.Values)
            idx.RemoveRow(rowIdx, row);
    }

    internal void ShiftIndexPositionsAfterDelete(int deletedIdx)
    {
        if (table.IndexesMutable.Count == 0)
            return;

        foreach (var idx in table.IndexesMutable.Values)
            idx.ShiftPositionsAfter(deletedIdx);
    }

    internal void UpdateIndexesWithRow(int rowIdx)
    {
        if (table.IndexesMutable.Count == 0)
            return;

        foreach (var index in table.IndexesMutable.Values)
            index.UpdateIndexesWithRow(rowIdx, table[rowIdx]);
    }

    internal void UpdateIndexesWithRow(
        int rowIdx,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?> newRow)
    {
        if (table.IndexesMutable.Count == 0)
            return;

        foreach (var index in table.IndexesMutable.Values)
            index.UpdateIndexesWithRow(rowIdx, oldRow, newRow);
    }

    internal void RebuildAllIndexes()
    {
        if (table.IndexesMutable.Count == 0)
            return;

        if (table.IndexesMutable.Count <= 1 || !table.Schema.Db.ThreadSafe)
        {
            foreach (var ix in table.IndexesMutable)
                ix.Value.RebuildIndex();
            return;
        }

        Parallel.ForEach(table.IndexesMutable.Values, ix => ix.RebuildIndex());
    }

    internal void MarkAllIndexesDirty()
    {
        if (table.IndexesMutable.Count == 0)
            return;

        foreach (var ix in table.IndexesMutable.Values)
            ix.MarkDirty();
    }

    internal void EnsureUniqueOnInsert(Dictionary<int, object?> newRow)
    {
        CheckUniquePrimary(newRow, table.Count > 0);

        foreach (var idx in table.UniqueIndexes)
        {
            var key = idx.BuildIndexKey(newRow);
            var hits = idx.LookupMutable(key);
            if (hits is { Count: > 0 })
                throw table.DuplicateKey(table.TableName, idx.Name, key);
        }
    }

    internal void EnsurePrimaryKeyUniqueOnInsert(
        Dictionary<int, object?> newRow,
        HashSet<IndexKey> batchPrimaryKeys)
    {
        if (table.PrimaryKeyIndexes.Count == 0)
            return;

        var pkKey = BuildPkKey(newRow);
        if (table.PrimaryKeyLookup.ContainsKey(pkKey) || !batchPrimaryKeys.Add(pkKey))
            throw table.DuplicateKey(table.TableName, SqlConst.PRIMARY, BuildPrimaryKeyDescription(newRow));
    }

    internal int? FindConflictingRowIndex(
        IReadOnlyDictionary<int, object?> newRow,
        out string? conflictIndexName,
        out object? conflictKey)
    {
        conflictIndexName = null;
        conflictKey = null;

        if (table.PrimaryKeyIndexes.Count > 0 && table.TryFindRowByPk(newRow, out var pkConflict))
        {
            conflictIndexName = SqlConst.PRIMARY;
            conflictKey = BuildPrimaryKeyDescription(newRow);
            return pkConflict;
        }

        if (TryFindPrimaryConflictByIndex(newRow, out var conflictByIndex))
        {
            conflictIndexName = SqlConst.PRIMARY;
            conflictKey = BuildPrimaryKeyDescription(newRow);
            return conflictByIndex;
        }

        if (!CheckPrimary(newRow, ref conflictIndexName, ref conflictKey, out var value))
            return value;

        foreach (var idx in table.UniqueIndexes)
        {
            var key = idx.BuildIndexKey(newRow);
            var hits = idx.LookupMutable(key);
            if (hits is { Count: > 0 })
            {
                conflictIndexName = idx.Name;
                conflictKey = key;
                foreach (var hit in hits)
                    return hit.Key;
            }
        }

        return null;
    }

    internal void EnsureUniqueBeforeUpdate(
        string tableName,
        IReadOnlyDictionary<int, object?> existingRow,
        IReadOnlyDictionary<int, object?> simulatedRow,
        int rowIdx,
        IReadOnlyList<string> changedCols)
    {
        foreach (var ix in table.UniqueIndexes)
            ix.EnsureUniqueBeforeUpdate(rowIdx, existingRow, simulatedRow, changedCols);
    }

    private bool TryFindPrimaryConflictByIndex(
        IReadOnlyDictionary<int, object?> newRow,
        out int rowIndex)
    {
        rowIndex = -1;
        if (table.PrimaryKeyIndexes.Count == 0)
            return false;

        IndexDef? pkIndex = null;
        foreach (var index in table.UniqueIndexes)
        {
            if (index.KeyCols.Count != table.PkIndexArray.Length)
                continue;

            var matches = true;
            for (var i = 0; i < table.PkIndexArray.Length; i++)
            {
                var pkIdx = table.PkIndexArray[i];
                var columnName = table.ColumnsByOrdinal[pkIdx].Name;
                if (!index.KeyCols.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                pkIndex = index;
                break;
            }
        }

        if (pkIndex is null)
            return false;

        var valuesByColumn = new Dictionary<string, object?>(table.PkIndexArray.Length, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < table.PkIndexArray.Length; i++)
        {
            var pkIdx = table.PkIndexArray[i];
            var columnName = table.ColumnsByOrdinal[pkIdx].Name;
            valuesByColumn[columnName] = newRow.TryGetValue(pkIdx, out var val) ? val : null;
        }

        var key = pkIndex.BuildIndexKeyFromValues(valuesByColumn);
        var hits = pkIndex.LookupMutable(key);
        if (hits is not { Count: > 0 })
            return false;

        foreach (var hit in hits)
        {
            rowIndex = hit.Key;
            break;
        }
        return true;
    }

    private bool CheckPrimary(
        IReadOnlyDictionary<int, object?> newRow,
        ref string? conflictIndexName,
        ref object? conflictKey,
        out int? value)
    {
        value = default;
        if (table.PrimaryKeyIndexes.Count <= 0) return true;
        for (int i = 0; i < table.Count; i++)
        {
            var existingRow = table[i];
            var matchedCount = 0;
            for (var pkPos = 0; pkPos < table.PkIndexArray.Length; pkPos++)
            {
                var pkIdx = table.PkIndexArray[pkPos];
                if (newRow.TryGetValue(pkIdx, out var pkVal)
                    && existingRow.TryGetValue(pkIdx, out var cur)
                    && Equals(cur, pkVal))
                {
                    matchedCount++;
                }
            }
            if (table.PkIndexArray.Length != matchedCount)
                continue;

            conflictIndexName = SqlConst.PRIMARY;
            conflictKey = BuildPrimaryKeyDescription(existingRow);
            value = i;
            return false;
        }

        return true;
    }

    private void CheckUniquePrimary(Dictionary<int, object?> newRow, bool hasExistingRows)
    {
        if (table.PrimaryKeyIndexes.Count <= 0)
            return;

        if (hasExistingRows && table.PkIndexArray.Length > 0)
        {
            if (TryFindRowByPk(newRow, out _))
                throw table.DuplicateKey(table.TableName, SqlConst.PRIMARY, BuildPrimaryKeyDescription(newRow));
        }

        if (TryFindPrimaryConflictByIndex(newRow, out _))
            throw table.DuplicateKey(table.TableName, SqlConst.PRIMARY, BuildPrimaryKeyDescription(newRow));
    }
}
