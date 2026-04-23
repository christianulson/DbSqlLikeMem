namespace DbSqlLikeMem;

internal sealed class DbConnectionTransactionJournalManager
{
    private readonly List<DbConnectionMockBase.TransactionJournalEntry> _transactionJournal = [];

    public int JournalCount => _transactionJournal.Count;

    public void Append(DbConnectionMockBase.TransactionJournalEntry entry)
        => _transactionJournal.Add(entry);

    public void Clear()
        => _transactionJournal.Clear();

    public void RollbackJournalTo(
        int journalPosition,
        DbConnectionMockBase connection,
        DbConnectionTransactionStateManager transactionState)
    {
        if (_transactionJournal.Count <= journalPosition)
            return;

        var singleTouchedTable = (TableMock?)null;
        var secondTouchedTable = (TableMock?)null;
        var thirdTouchedTable = (TableMock?)null;
        HashSet<TableMock>? touchedTables = null;
        transactionState.IsReplayingTransactionJournal = true;
        try
        {
            for (var idx = _transactionJournal.Count - 1; idx >= journalPosition; idx--)
            {
                var entry = _transactionJournal[idx];
                if (entry.Table is not null)
                {
                    if (touchedTables is null)
                    {
                        if (singleTouchedTable is null)
                        {
                            singleTouchedTable = entry.Table;
                        }
                        else if (ReferenceEquals(singleTouchedTable, entry.Table))
                        {
                        }
                        else if (secondTouchedTable is null)
                        {
                            secondTouchedTable = entry.Table;
                        }
                        else if (ReferenceEquals(secondTouchedTable, entry.Table))
                        {
                        }
                        else if (thirdTouchedTable is null)
                        {
                            thirdTouchedTable = entry.Table;
                        }
                        else if (ReferenceEquals(thirdTouchedTable, entry.Table))
                        {
                        }
                        else
                        {
                            touchedTables = HashSetCompatibilityExtensions.Create<TableMock>(4);
                            touchedTables.Add(singleTouchedTable);
                            touchedTables.Add(secondTouchedTable);
                            touchedTables.Add(thirdTouchedTable);
                            touchedTables.Add(entry.Table);
                            singleTouchedTable = null;
                            secondTouchedTable = null;
                            thirdTouchedTable = null;
                        }
                    }
                    else
                    {
                        touchedTables.Add(entry.Table);
                    }
                }

                ReplayTransactionJournalEntry(connection, entry);
            }
        }
        finally
        {
            transactionState.IsReplayingTransactionJournal = false;
        }

        _transactionJournal.RemoveRange(
            journalPosition,
            _transactionJournal.Count - journalPosition);

        if (touchedTables is null)
        {
            if (singleTouchedTable is null)
                return;

            if (secondTouchedTable is null)
            {
                singleTouchedTable.RestoreIndexesAfterJournalReplay();
                return;
            }

            if (thirdTouchedTable is null)
            {
                connection.RestoreIndexesAfterJournalReplay([singleTouchedTable, secondTouchedTable]);
                return;
            }

            connection.RestoreIndexesAfterJournalReplay([singleTouchedTable, secondTouchedTable, thirdTouchedTable]);
            return;
        }

        connection.RestoreIndexesAfterJournalReplay(touchedTables);
    }

    private static void ReplayTransactionJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        switch (entry.Kind)
        {
            case DbConnectionMockBase.TransactionJournalEntryKind.Insert:
                entry.Table!.RemoveRowByReference(entry.Row!);
                entry.Table.NextIdentity = entry.PreviousNextIdentity;
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.Update:
                ReplayUpdateJournalEntry(entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.Delete:
                entry.Table!.InsertRestoredRow(entry.RowIndex, entry.Row!);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.CreateTable:
                connection.UnregisterTable(entry.Table!, entry.RegistrationKind, entry.RegistrationKey);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropTable:
                connection.RegisterTable(entry.Table!, entry.RegistrationKind, entry.RegistrationKey);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.CreateIndex:
                if (entry.IndexDefinition is not null)
                    entry.Table!.IndexManager.DropIndex(entry.IndexDefinition.Name, ifExists: true);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropIndex:
                ReplayDropIndexJournalEntry(entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.UpsertView:
                ReplayUpsertViewJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropView:
                ReplayDropViewJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.CreateSequence:
                if (entry.SequenceState is not null)
                {
                    connection.Db.DropSequence(entry.SequenceState.SequenceName, true, entry.SequenceState.SchemaName);
                    connection.ClearSessionSequenceValue(entry.SequenceState.SequenceName, entry.SequenceState.SchemaName);
                }
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropSequence:
            case DbConnectionMockBase.TransactionJournalEntryKind.UpdateSequence:
                ReplaySequenceJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.UpsertFunction:
                ReplayUpsertFunctionJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropFunction:
                ReplayDropFunctionJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropProcedure:
                ReplayDropProcedureJournalEntry(connection, entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.DropTrigger:
                ReplayDropTriggerJournalEntry(entry);
                break;
            case DbConnectionMockBase.TransactionJournalEntryKind.UpsertProcedure:
                ReplayUpsertProcedureJournalEntry(connection, entry);
                break;
        }
    }

    private static void ReplayUpdateJournalEntry(DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.NewRowSnapshot is not null && entry.OldRowSnapshot is not null)
        {
            entry.Table!.RestoreRowSnapshot(
                entry.Row!,
                MergeConcurrentUpdateRollback(
                    entry.Row!,
                    entry.OldRowSnapshot,
                    entry.NewRowSnapshot));
        }
        else
        {
            entry.Table!.RestoreRowSnapshot(
                entry.Row!,
                entry.OldRowSnapshot ?? new Dictionary<int, object?>());
        }
    }

    private static void ReplayDropIndexJournalEntry(DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.IndexDefinition is not null
            && !entry.Table!.Indexes.ContainsKey(entry.IndexDefinition.Name))
        {
            entry.Table.CreateIndex(
                entry.IndexDefinition.Name,
                entry.IndexDefinition.KeyCols,
                [.. entry.IndexDefinition.Include],
                entry.IndexDefinition.Unique);
        }
    }

    private static void ReplayUpsertViewJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.ViewState is null)
            return;

        if (entry.ViewState.PreviousDefinition is null)
            connection.Db.RemoveView(entry.ViewState.ViewName, entry.ViewState.SchemaName);
        else
            connection.Db.RestoreView(
                entry.ViewState.ViewName,
                entry.ViewState.PreviousDefinition,
                entry.ViewState.SchemaName);
    }

    private static void ReplayDropViewJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.ViewState?.PreviousDefinition is not null)
        {
            connection.Db.RestoreView(
                entry.ViewState.ViewName,
                entry.ViewState.PreviousDefinition,
                entry.ViewState.SchemaName);
        }
    }

    private static void ReplaySequenceJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.SequenceState?.PreviousDefinition is null)
            return;

        connection.Db.RestoreSequence(
            entry.SequenceState.SequenceName,
            DbConnectionMockBase.CloneSequence(entry.SequenceState.PreviousDefinition),
            entry.SequenceState.SchemaName);
        if (entry.SequenceState.HadSessionValue)
            connection.SetSessionSequenceValue(entry.SequenceState.SequenceName, entry.SequenceState.SessionValue, entry.SequenceState.SchemaName);
        else
            connection.ClearSessionSequenceValue(entry.SequenceState.SequenceName, entry.SequenceState.SchemaName);
    }

    private static void ReplayUpsertFunctionJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.FunctionState is null)
            return;

        if (entry.FunctionState.PreviousDefinition is null)
            connection.Db.RemoveFunction(entry.FunctionState.FunctionName, entry.FunctionState.SchemaName);
        else
            connection.Db.RestoreFunction(
                entry.FunctionState.FunctionName,
                entry.FunctionState.PreviousDefinition,
                entry.FunctionState.SchemaName);
    }

    private static void ReplayDropFunctionJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.FunctionState?.PreviousDefinition is not null)
        {
            connection.Db.RestoreFunction(
                entry.FunctionState.FunctionName,
                entry.FunctionState.PreviousDefinition,
                entry.FunctionState.SchemaName);
        }
    }

    private static void ReplayDropProcedureJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.ProcedureState?.PreviousDefinition is not null)
        {
            connection.Db.RestoreProcedure(
                entry.ProcedureState.ProcedureName,
                entry.ProcedureState.PreviousDefinition,
                entry.ProcedureState.SchemaName);
        }
    }

    private static void ReplayDropTriggerJournalEntry(DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.TriggerState is not null)
        {
            entry.Table!.TriggerManager.AddOrReplaceTrigger(
                entry.TriggerState.TriggerName,
                entry.TriggerState.PreviousEvent,
                static _ => { },
                orReplace: true);
        }
    }

    private static void ReplayUpsertProcedureJournalEntry(
        DbConnectionMockBase connection,
        DbConnectionMockBase.TransactionJournalEntry entry)
    {
        if (entry.ProcedureState is null)
            return;

        if (entry.ProcedureState.PreviousDefinition is null)
            connection.Db.RemoveProcedure(entry.ProcedureState.ProcedureName, entry.ProcedureState.SchemaName);
        else
            connection.Db.RestoreProcedure(
                entry.ProcedureState.ProcedureName,
                entry.ProcedureState.PreviousDefinition,
                entry.ProcedureState.SchemaName);
    }

    private static IReadOnlyDictionary<int, object?> MergeConcurrentUpdateRollback(
        IDictionary<int, object?> currentRow,
        IReadOnlyDictionary<int, object?> oldSnapshot,
        IReadOnlyDictionary<int, object?> newSnapshot)
    {
        var merged = new Dictionary<int, object?>(oldSnapshot.Count);
        foreach (var entry in oldSnapshot)
        {
            var columnIndex = entry.Key;
            var oldValue = entry.Value;
            newSnapshot.TryGetValue(columnIndex, out var newValue);
            currentRow.TryGetValue(columnIndex, out var currentValue);

            if (TryRestoreConcurrentNumericValue(oldValue, newValue, currentValue, out var restoredValue))
            {
                merged[columnIndex] = restoredValue;
                continue;
            }

            merged[columnIndex] = oldValue;
        }

        return merged;
    }

    private static bool TryRestoreConcurrentNumericValue(
        object? oldValue,
        object? newValue,
        object? currentValue,
        out object? restoredValue)
    {
        restoredValue = null;
        if (oldValue is null || newValue is null || currentValue is null)
            return false;

        if (!TryConvertToDecimal(oldValue, out var oldDecimal)
            || !TryConvertToDecimal(newValue, out var newDecimal)
            || !TryConvertToDecimal(currentValue, out var currentDecimal))
        {
            return false;
        }

        var delta = newDecimal - oldDecimal;
        var restoredDecimal = currentDecimal - delta;
        restoredValue = ConvertDecimalLikeValue(restoredDecimal, currentValue);
        return true;
    }

    private static bool TryConvertToDecimal(object value, out decimal decimalValue)
    {
        try
        {
            decimalValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            decimalValue = default;
            return false;
        }
    }

    private static object ConvertDecimalLikeValue(decimal value, object sample)
        => sample switch
        {
            byte _ => Convert.ChangeType((byte)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            sbyte _ => Convert.ChangeType((sbyte)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            short _ => Convert.ChangeType((short)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            ushort _ => Convert.ChangeType((ushort)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            int _ => Convert.ChangeType((int)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            uint _ => Convert.ChangeType((uint)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            long _ => Convert.ChangeType((long)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            ulong _ => Convert.ChangeType((ulong)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            float _ => Convert.ChangeType((float)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            double _ => Convert.ChangeType((double)value, sample.GetType(), CultureInfo.InvariantCulture)!,
            decimal _ => Convert.ChangeType(value, sample.GetType(), CultureInfo.InvariantCulture)!,
            _ => value
        };
}
