namespace DbSqlLikeMem;

internal sealed class TableStateManager(TableMock table)
{
    private List<Dictionary<int, object?>>? _backup;

    internal int FindRowIndexByReference(Dictionary<int, object?> row)
        => table.FindRowIndexByReferenceCore(row);

    internal void RemoveRowByReference(Dictionary<int, object?> row)
    {
        var rowIndex = FindRowIndexByReference(row);
        if (rowIndex >= 0)
            table.RemoveRowByReferenceCore(rowIndex);
    }

    internal void InsertRestoredRow(int rowIndex, Dictionary<int, object?> row)
        => table.InsertRestoredRowCore(rowIndex, row);

    internal void RestoreRowSnapshot(
        Dictionary<int, object?> targetRow,
        IReadOnlyDictionary<int, object?> snapshot)
    {
        targetRow.Clear();
        foreach (var entry in snapshot)
            targetRow[entry.Key] = entry.Value;
    }

    internal void RestoreIndexesAfterJournalReplay()
    {
        table.IndexManager.RebuildPkIndex();
        table.IndexManager.MarkAllIndexesDirty();
    }

    internal void Backup()
    {
        var backup = new List<Dictionary<int, object?>>(table.Count);
        for (var i = 0; i < table.Count; i++)
            backup.Add(TableMock.CloneRow(table[i]));

        _backup = backup;
    }

    internal void Restore()
    {
        if (_backup == null)
            return;

        table.ClearRowsCore();
        foreach (var row in _backup)
            table.InsertRestoredRowCore(table.Count, TableMock.CloneRow(row));

        table.IndexManager.RebuildPkIndex();
        table.IndexManager.MarkAllIndexesDirty();
    }

    internal void ClearBackup() => _backup = null;
}
