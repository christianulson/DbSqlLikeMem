namespace DbSqlLikeMem;

internal sealed class TableForeignKeyManager(TableMock table, Func<string, string, Exception> foreignKeyFails)
{
    private readonly Dictionary<string, ForeignDef> _foreignKeys = new(StringComparer.OrdinalIgnoreCase);
    private bool _hasSelfReferencingForeignKey;

    internal IReadOnlyDictionary<string, ForeignDef> ForeignKeys => _foreignKeys;

    internal bool HasForeignKeys => _foreignKeys.Count > 0;

    internal bool HasSelfReferencingForeignKey => _hasSelfReferencingForeignKey;

    internal ForeignDef CreateForeignKey(
        string name,
        string refTable,
        HashSet<(string col, string refCol)> references)
    {
        var tbRef = ResolveReferencedTable(refTable);
        var fk = new ForeignDef(
            table,
            name,
            tbRef,
            [.. references.Select(reference =>
            {
                var col = table.Columns[reference.col];
                var refCol = tbRef is TableMock refTableMock
                    ? refTableMock.Columns[reference.refCol]
                    : tbRef.Columns[reference.refCol];
                return (col: col, refCol: refCol);
            })]
        );

        _foreignKeys.Add(name, fk);
        if (ReferenceEquals(tbRef, table))
            _hasSelfReferencingForeignKey = true;
        return fk;
    }

    internal void ValidateForeignKeysOnRow(IReadOnlyDictionary<int, object?> row)
    {
        if (_foreignKeys.Count == 0)
            return;

        foreach (var fk in _foreignKeys.Values)
        {
            var hasNull = false;
            foreach (var (col, _) in fk.References)
            {
                if (!row.TryGetValue(col.Index, out var val)
                    || val is null
                    || val is DBNull)
                {
                    hasNull = true;
                    break;
                }
            }

            if (hasNull)
                continue;

            if (!HasReferencedRow(fk, row))
            {
                var refCols = string.Join(",", fk.References.Select(_ => _.col.Name));
                throw foreignKeyFails(refCols, fk.RefTable.TableName);
            }
        }
    }

    private ITableMock ResolveReferencedTable(string refTable)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(refTable, nameof(refTable));

        var separatorIndex = refTable.IndexOf('.');
        if (separatorIndex <= 0 || separatorIndex == refTable.Length - 1)
            return table.Schema[refTable];

        var schemaName = refTable[..separatorIndex].NormalizeName();
        var tableName = refTable[(separatorIndex + 1)..].NormalizeName();
        return table.Schema.Db.GetTable(tableName, schemaName);
    }

    private bool HasReferencedRow(
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> row)
    {
        var refTable = fk.RefTable;
        if (fk.TryGetRefLookupPlan(out var lookupPlan))
        {
            var key = lookupPlan.BuildKey(row);
            if (lookupPlan.Index.LookupMutable(key)?.Count > 0)
                return true;
        }

        if (table.Schema.Db.ThreadSafe && refTable.Count >= 2048)
        {
            var found = 0;
            Parallel.For(0, refTable.Count, (refIndex, state) =>
            {
                if (Volatile.Read(ref found) != 0)
                {
                    state.Stop();
                    return;
                }

                var refRow = refTable[refIndex];
                foreach (var reference in fk.References)
                {
                    if (!Equals(refRow[reference.refCol.Index], row[reference.col.Index]))
                        return;
                }

                Interlocked.Exchange(ref found, 1);
                state.Stop();
            });

            return Volatile.Read(ref found) != 0;
        }

        foreach (var refRow in refTable)
        {
            var matches = true;
            foreach (var reference in fk.References)
            {
                if (!Equals(refRow[reference.refCol.Index], row[reference.col.Index]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
    }
}
