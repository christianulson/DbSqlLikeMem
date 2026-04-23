namespace DbSqlLikeMem;

internal sealed class DbConnectionTransactionStateManager
{
    private readonly Dictionary<string, int> _savepoints =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _savepointOrder = [];

    public int CurrentTransactionId { get; set; }

    public int TransactionBeginJournalPosition { get; set; }

    public bool IsReplayingTransactionJournal { get; set; }

    public bool HasRuntimeState(int transactionJournalCount)
        => transactionJournalCount != 0
            || _savepoints.Count != 0
            || _savepointOrder.Count != 0
            || TransactionBeginJournalPosition != 0
            || IsReplayingTransactionJournal;

    public bool TryGetSavepointJournalPosition(
        string savepointName,
        out int journalPosition)
        => _savepoints.TryGetValue(savepointName, out journalPosition);

    public void SetSavepoint(string savepointName, int journalPosition)
    {
        if (_savepoints.ContainsKey(savepointName))
            RemoveSavepointOrderEntries(savepointName);

        _savepoints[savepointName] = journalPosition;
        _savepointOrder.Add(savepointName);
    }

    public void RemoveSavepoint(string savepointName)
    {
        _savepoints.Remove(savepointName);
        RemoveSavepointOrderEntries(savepointName);
    }

    public int FindSavepointOrderIndex(string savepointName)
    {
        for (var i = _savepointOrder.Count - 1; i >= 0; i--)
        {
            if (_savepointOrder[i].Equals(savepointName, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    public void RemoveSavepointsAfterIndex(int savepointIndex)
    {
        for (var idx = _savepointOrder.Count - 1; idx > savepointIndex; idx--)
        {
            _savepoints.Remove(_savepointOrder[idx]);
            _savepointOrder.RemoveAt(idx);
        }
    }

    public void Clear()
    {
        _savepoints.Clear();
        _savepointOrder.Clear();
        TransactionBeginJournalPosition = 0;
        IsReplayingTransactionJournal = false;
    }

    private void RemoveSavepointOrderEntries(string savepointName)
    {
        for (var i = _savepointOrder.Count - 1; i >= 0; i--)
        {
            if (_savepointOrder[i].Equals(savepointName, StringComparison.OrdinalIgnoreCase))
                _savepointOrder.RemoveAt(i);
        }
    }
}
