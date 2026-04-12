namespace DbSqlLikeMem;

internal static class DbInsertStrategy
{
    /// <summary>
    /// EN: Implements ExecuteInsert.
    /// PT: Implementa ExecuteInsert.
    /// </summary>
    public static DmlExecutionResult ExecuteInsert(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var context = QueryExecutionContext.FromConnection(connection, pars);
        if (!context.ThreadSafe)
            return Execute(context, query);
        lock (connection.Db.SyncRoot)
            return Execute(context, query);
    }

    /// <summary>
    /// EN: Implements ExecuteInsert using a pre-built execution context.
    /// PT: Implementa ExecuteInsert usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteInsert(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        QueryExecutionContext context)
    {
        if (!context.ThreadSafe)
            return Execute(context, query);
        lock (connection.Db.SyncRoot)
            return Execute(context, query);
    }

    /// <summary>
    /// EN: Implements ExecuteReplace.
    /// PT: Implementa ExecuteReplace.
    /// </summary>
    public static DmlExecutionResult ExecuteReplace(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var context = QueryExecutionContext.FromConnection(connection, pars);
        if (!context.ThreadSafe)
            return Execute(context, query with { IsReplace = true });
        lock (connection.Db.SyncRoot)
            return Execute(context, query with { IsReplace = true });
    }

    /// <summary>
    /// EN: Implements ExecuteReplace using a pre-built execution context.
    /// PT: Implementa ExecuteReplace usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteReplace(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        QueryExecutionContext context)
    {
        if (!context.ThreadSafe)
            return Execute(context, query with { IsReplace = true });
        lock (connection.Db.SyncRoot)
            return Execute(context, query with { IsReplace = true });
    }

    private static DmlExecutionResult Execute(
        QueryExecutionContext context,
        SqlInsertQuery query)
    {
        var connection = context.Connection;
        context.ResetPositionalParameterCursor();
        var dialect = context.Dialect;
        var capturePlans = context.CaptureExecutionPlans;
        var sw = capturePlans ? Stopwatch.StartNew() : null;
        var metricsEnabled = context.MetricsEnabled;
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query.Table!.Name, nameof(query.Table.Name));

        var tableName = query.Table.Name!; // Nome vindo do Parser
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName) || table == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);
        var targetRowCountBefore = table.Count;

        // Identifica linhas a inserir (seja via VALUES ou SELECT)
        List<Dictionary<int, object?>> newRows;

        if (query.InsertSelect != null)
        {
            // Caso: INSERT INTO ... SELECT ...
            newRows = CreateRowsFromSelect(context, query, table);
        }
        else
        {
            // Caso: INSERT INTO ... VALUES ...
            newRows = CreateRowsFromValues(context, query, table);
        }

        var newRowsCount = newRows.Count;
        int insertedCount = 0;
        int updatedCount = 0;
        var tableMock = (TableMock)table;
        var supportsTriggers = dialect.SupportsTriggers;
        var hasBeforeInsertTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeInsert);
        var hasAfterInsertTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterInsert);
        var hasForeignKeys = tableMock.ForeignKeys.Count > 0;
        ValidateInsertPartitions(query, tableMock);
        ValidatePartitionedInsertRows(query, tableMock, newRows);
        var canUseBatchInsert = CanUseBatchInsert(context, table, tableName, query.Table.DbName, tableMock);
        var affectedIndexes = new List<int>(newRows.Count);
        var hasBeforeUpdateTrigger = false;
        var hasAfterUpdateTrigger = false;
        var requiresOldSnapshotForIndex = false;
        var hasInsertConflictTargets = tableMock.PkIndexArray.Length > 0 || tableMock.UniqueIndexes.Count > 0;

        if (query.HasOnDuplicateKeyUpdate)
        {
            var onDupChangedColumns = new List<string>(query.OnDupAssignsParsed.Count);
            for (var i = 0; i < query.OnDupAssignsParsed.Count; i++)
                onDupChangedColumns.Add(query.OnDupAssignsParsed[i].Column);

            requiresOldSnapshotForIndex = HasIndexedKeyChanges(tableMock, onDupChangedColumns);
            hasBeforeUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate);
            hasAfterUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate);
        }

        if (query.IsReplace)
            return ExecuteReplaceCore(context, query, table, tableMock, newRows, targetRowCountBefore);

        if (!query.HasOnDuplicateKeyUpdate
            && !query.IsOnConflictDoNothing
            && newRowsCount > 1
            && canUseBatchInsert)
        {
            var beforeCount = table.Count;
            tableMock.AddBatch(newRows);
            var afterCount = beforeCount + newRowsCount;
            insertedCount = newRowsCount;
            for (int i = beforeCount; i < afterCount; i++)
                affectedIndexes.Add(i);
            TrySetLastInsertId(context, table, newRows[^1]);
        }
        else if (query.HasOnDuplicateKeyUpdate
            && newRowsCount > 1
            && canUseBatchInsert)
        {
            if (!hasInsertConflictTargets)
            {
                var beforeCount = table.Count;
                tableMock.AddBatch(newRows);
                var afterCount = beforeCount + newRowsCount;
                insertedCount = newRowsCount;
                for (int i = beforeCount; i < afterCount; i++)
                    affectedIndexes.Add(i);
                TrySetLastInsertId(context, table, newRows[^1]);
            }
            else
            {
                var pendingInsertRows = new List<Dictionary<int, object?>>(newRows.Count);
                HashSet<IndexKey>? pendingPrimaryKeys = tableMock.PkIndexArray.Length > 0
                    ? new HashSet<IndexKey>()
                    : null;
                List<(IndexDef Index, HashSet<IndexKey> Keys)>? pendingUniqueKeys = null;
                foreach (var index in tableMock.UniqueIndexes)
                {
                    pendingUniqueKeys ??= new List<(IndexDef Index, HashSet<IndexKey> Keys)>();
                    pendingUniqueKeys.Add((index, new HashSet<IndexKey>()));
                }
                var tracksPendingBatchConflicts = pendingPrimaryKeys is not null || pendingUniqueKeys is not null;
                var pendingUniqueKeysBuffer = pendingUniqueKeys is not null
                    ? new IndexKey[pendingUniqueKeys.Count]
                    : null;

                foreach (var newRow in newRows)
                {
                    IndexKey? pendingBatchPrimaryKey = null;
                    IndexKey[]? pendingBatchUniqueKeys = null;
                    if (tracksPendingBatchConflicts)
                    {
                        BuildPendingBatchKeys(
                            tableMock,
                            pendingPrimaryKeys,
                            pendingUniqueKeys,
                            pendingUniqueKeysBuffer,
                            newRow,
                            out pendingBatchPrimaryKey,
                            out pendingBatchUniqueKeys);
                    }

                    if (pendingInsertRows.Count > 0
                        && tracksPendingBatchConflicts
                        && HasPendingBatchConflict(pendingPrimaryKeys, pendingUniqueKeys, pendingBatchPrimaryKey, pendingBatchUniqueKeys))
                    {
                        var before = table.Count;
                        FlushPendingInsertBatch(context, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
                        for (int i = before; i < table.Count; i++) affectedIndexes.Add(i);
                    }

                    var conflictIdx = tableMock.FindConflictingRowIndex(newRow, out _, out _);
                    if (conflictIdx is null)
                    {
                        pendingInsertRows.Add(newRow);
                        if (tracksPendingBatchConflicts)
                            RegisterPendingBatchKeys(pendingPrimaryKeys, pendingUniqueKeys, pendingBatchPrimaryKey, pendingBatchUniqueKeys);
                        continue;
                    }
                    var beforeFlush = table.Count;
                    FlushPendingInsertBatch(context, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
                    for (int i = beforeFlush; i < table.Count; i++) affectedIndexes.Add(i);

                    if (query.IsOnConflictDoNothing)
                        continue;
                    if (!ShouldApplyOnConflictUpdateWhere(query, table, conflictIdx.Value, newRow, context))
                        continue;

                    var oldSnapshot = requiresOldSnapshotForIndex || hasBeforeUpdateTrigger || hasAfterUpdateTrigger
                        ? TableMock.SnapshotRow(table[conflictIdx.Value])
                        : null;
                    var simulatedUpdated = TableMock.CloneRow(table[conflictIdx.Value]);
                    ApplyOnDuplicateUpdateAstInMemory(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        context,
                        simulatedUpdated);
                    if (hasForeignKeys)
                        tableMock.ValidateForeignKeysOnRow(simulatedUpdated);

                    if (hasBeforeUpdateTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, TableMock.SnapshotRow(newRow));

                    ApplyOnDuplicateUpdateAst(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        context);

                    if (hasAfterUpdateTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[conflictIdx.Value]));

                    if (requiresOldSnapshotForIndex)
                        tableMock.UpdateIndexesWithRow(conflictIdx.Value, oldSnapshot, table[conflictIdx.Value]);
                    else
                        tableMock.UpdateIndexesWithRow(conflictIdx.Value);
                    updatedCount++;
                    affectedIndexes.Add(conflictIdx.Value);
                }
                var beforeFinalFlush = table.Count;
                FlushPendingInsertBatch(context, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
                for (int i = beforeFinalFlush; i < table.Count; i++) affectedIndexes.Add(i);
            }
        }
        else
        {
            foreach (var newRow in newRows)
            {
                if (!query.HasOnDuplicateKeyUpdate)
                {
                    // Insercao normal
                    if (query.IsOnConflictDoNothing)
                    {
                        if (hasInsertConflictTargets)
                        {
                            var conflictIdx1 = tableMock.FindConflictingRowIndex(newRow, out _, out _);
                            if (conflictIdx1 is not null)
                                continue;
                        }
                    }

                    if (hasForeignKeys)
                        tableMock.ValidateForeignKeysOnRow(newRow);
                    if (hasBeforeInsertTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

                    table.Add(newRow);
                    var insertedRow = table[table.Count - 1];

                    if (hasAfterInsertTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));
                    TrySetLastInsertId(context, table, insertedRow);
                    insertedCount++;
                    affectedIndexes.Add(table.Count - 1);
                    continue;
                }

                // Lógica ON DUPLICATE KEY UPDATE
                var conflictIdx = tableMock.FindConflictingRowIndex(newRow, out _, out _);
                if (conflictIdx is null)
                {
                    // Sem conflito -> Insere
                    if (hasForeignKeys)
                        tableMock.ValidateForeignKeysOnRow(newRow);
                    if (hasBeforeInsertTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

                    table.Add(newRow);
                    var insertedRow = table[table.Count - 1];

                    if (hasAfterInsertTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));
                    TrySetLastInsertId(context, table, insertedRow);
                    insertedCount++;
                    affectedIndexes.Add(table.Count - 1);
                }
                else
                {
                    if (query.IsOnConflictDoNothing)
                        continue;
                    if (!ShouldApplyOnConflictUpdateWhere(query, table, conflictIdx.Value, newRow, context))
                        continue;

                    // Conflito -> Update
                    var oldSnapshot = requiresOldSnapshotForIndex || hasBeforeUpdateTrigger || hasAfterUpdateTrigger
                        ? TableMock.SnapshotRow(table[conflictIdx.Value])
                        : null;
                    var simulatedUpdated = TableMock.CloneRow(table[conflictIdx.Value]);
                    ApplyOnDuplicateUpdateAstInMemory(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        context,
                        simulatedUpdated);
                    if (hasForeignKeys)
                        tableMock.ValidateForeignKeysOnRow(simulatedUpdated);

                    if (hasBeforeUpdateTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, TableMock.SnapshotRow(newRow));

                    ApplyOnDuplicateUpdateAst(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        context);

                    if (hasAfterUpdateTrigger)
                        TryExecuteTableTrigger(context, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[conflictIdx.Value]));

                    if (requiresOldSnapshotForIndex)
                        tableMock.UpdateIndexesWithRow(conflictIdx.Value, oldSnapshot, table[conflictIdx.Value]);
                    else
                        tableMock.UpdateIndexesWithRow(conflictIdx.Value);
                    updatedCount++;
                    affectedIndexes.Add(conflictIdx.Value);
                }
            }
        }

        if (metricsEnabled)
        {
            (context.Connection.Metrics).Inserts += insertedCount;
            (context.Connection.Metrics).Updates += updatedCount;
        }

        var affected = dialect.GetInsertUpsertAffectedRowCount(insertedCount, updatedCount);
        connection.SetLastFoundRows(affected);

        List<IReadOnlyDictionary<int, object?>>? affectedRowsData = null;
        if (connection.CaptureAffectedRowSnapshots)
        {
            affectedRowsData = new List<IReadOnlyDictionary<int, object?>>(affectedIndexes.Count);
            for (var i = 0; i < affectedIndexes.Count; i++)
                affectedRowsData.Add(TableMock.SnapshotRow(table[affectedIndexes[i]]));
        }

        if (capturePlans)
        {
            sw!.Stop();
            var metrics = new SqlPlanRuntimeMetrics(
                InputTables: query.InsertSelect is null ? 1 : 1 + CountInputTables(query.InsertSelect),
                EstimatedRowsRead: targetRowCountBefore + newRowsCount,
                ActualRows: affected,
                ElapsedMs: sw.ElapsedMilliseconds);
            var plan = SqlExecutionPlanFormatter.FormatInsert(
                query,
                metrics,
                new SqlPlanMockRuntimeContext(connection.SimulatedLatencyMs, connection.DropProbability, connection.Db.ThreadSafe));
            connection.RegisterExecutionPlan(plan);
        }

        return new DmlExecutionResult
        {
            AffectedRows = affected,
            AffectedIndexes = affectedIndexes,
            AffectedRowsData = affectedRowsData ?? []
        };
    }

    private static DmlExecutionResult ExecuteReplaceCore(
        QueryExecutionContext context,
        SqlInsertQuery query,
        ITableMock table,
        TableMock tableMock,
        List<Dictionary<int, object?>> newRows,
        int targetRowCountBefore)
    {
        var capturePlans = context.CaptureExecutionPlans;
        var sw = capturePlans ? Stopwatch.StartNew() : null;
        var metricsEnabled = (context.Connection.Metrics).Enabled;
        var supportsTriggers = context.Dialect.SupportsTriggers;
        var hasBeforeInsertTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeInsert);
        var hasAfterInsertTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterInsert);
        var hasBeforeDeleteTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeDelete);
        var hasAfterDeleteTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterDelete);
        var hasForeignKeys = tableMock.ForeignKeys.Count > 0;
        var affectedIndexes = new List<int>();
        List<IReadOnlyDictionary<int, object?>>? affectedRowsData = null;
        var insertedCount = 0;
        var deletedCount = 0;

        foreach (var newRow in newRows)
        {
            var conflictIndexes = FindReplaceConflictIndexes(tableMock, newRow);
            conflictIndexes.Sort(static (left, right) => right.CompareTo(left));

            if (conflictIndexes.Count > 0)
            {
                var deleteRows = new List<IReadOnlyDictionary<int, object?>>(conflictIndexes.Count);
                for (var i = 0; i < conflictIndexes.Count; i++)
                    deleteRows.Add(table[conflictIndexes[i]]);
                tableMock.Schema.ValidateForeignKeysOnDelete(query.Table!.Name!, tableMock, deleteRows);

                foreach (var idx in conflictIndexes)
                {
                    IReadOnlyDictionary<int, object?>? oldRow = null;
                    if (hasBeforeDeleteTrigger || hasAfterDeleteTrigger)
                        oldRow = TableMock.SnapshotRow(table[idx]);

                    if (hasBeforeDeleteTrigger)
                        TryExecuteTableTrigger(context, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.BeforeDelete, oldRow, null);

                    table.RemoveAt(idx);

                    if (hasAfterDeleteTrigger)
                        TryExecuteTableTrigger(context, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.AfterDelete, oldRow, null);

                    deletedCount++;
                }
            }

            if (hasForeignKeys)
                tableMock.ValidateForeignKeysOnRow(newRow);
            if (hasBeforeInsertTrigger)
                TryExecuteTableTrigger(context, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

            var beforeInsert = table.Count;
            table.Add(newRow);
            var insertedRow = table[table.Count - 1];

            if (hasAfterInsertTrigger)
                TryExecuteTableTrigger(context, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));

            TrySetLastInsertId(context, table, insertedRow);
            insertedCount++;
            affectedIndexes.Add(beforeInsert);
            if (context.CaptureAffectedRowSnapshots)
            {
                affectedRowsData ??= new List<IReadOnlyDictionary<int, object?>>(newRows.Count);
                affectedRowsData.Add(TableMock.SnapshotRow(insertedRow));
            }
        }

        if (metricsEnabled)
        {
            (context.Connection.Metrics).Inserts += insertedCount;
            (context.Connection.Metrics).Deletes += deletedCount;
        }

        var affected = deletedCount + insertedCount;
        context.Connection.SetLastFoundRows(affected);

        if (capturePlans)
        {
            sw!.Stop();
            var metrics = new SqlPlanRuntimeMetrics(
                InputTables: query.InsertSelect is null ? 1 : 1 + CountInputTables(query.InsertSelect),
                EstimatedRowsRead: targetRowCountBefore + newRows.Count,
                ActualRows: affected,
                ElapsedMs: sw.ElapsedMilliseconds);
            var plan = SqlExecutionPlanFormatter.FormatInsert(
                query,
                metrics,
                new SqlPlanMockRuntimeContext(context.Connection.SimulatedLatencyMs, context.Connection.DropProbability, context.ThreadSafe));
            context.Connection.RegisterExecutionPlan(plan);
        }

        return new DmlExecutionResult
        {
            AffectedRows = affected,
            AffectedIndexes = affectedIndexes,
            AffectedRowsData = affectedRowsData ?? []
        };
    }
    private static bool HasIndexedKeyChanges(ITableMock table, IReadOnlyList<string> changedCols)
    {
        if (changedCols.Count == 0)
            return false;

        if (table is TableMock tableMock)
        {
            foreach (var pkIndex in tableMock.PkIndexArray)
            {
                if (ContainsChangedColumn(changedCols, tableMock.GetColumnByIndex(pkIndex).Name))
                    return true;
            }

            foreach (var index in tableMock.IndexesRaw.Values)
            {
                foreach (var keyCol in index.KeyCols)
                {
                    if (ContainsChangedColumn(changedCols, keyCol))
                        return true;
                }
            }

            return false;
        }

        foreach (var pkIndex in table.PrimaryKeyIndexes)
        {
            var column = GetColumnByIndex(table, pkIndex);
            if (ContainsChangedColumn(changedCols, column.Name))
                return true;
        }

        foreach (var index in table.Indexes.Values)
        {
            foreach (var keyCol in index.KeyCols)
            {
                if (ContainsChangedColumn(changedCols, keyCol))
                    return true;
            }
        }

        return false;
    }

    private static ColumnDef GetColumnByIndex(ITableMock table, int index)
    {
        foreach (var column in table.Columns.Values)
        {
            if (column.Index == index)
                return column;
        }

        throw new InvalidOperationException($"Column index {index} was not found.");
    }

    private static bool ContainsChangedColumn(IReadOnlyList<string> changedCols, string columnName)
    {
        for (var i = 0; i < changedCols.Count; i++)
        {
            if (string.Equals(changedCols[i], columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
    private static List<int> FindReplaceConflictIndexes(
        TableMock table,
        IReadOnlyDictionary<int, object?> newRow)
    {
        var conflicts = new List<int>();
        var seen = new HashSet<int>();

        if (table.PkIndexArray.Length > 0)
        {
            if (TryFindRowByPrimaryKey(table, newRow, out var pkRowIndex))
            {
                if (seen.Add(pkRowIndex))
                    conflicts.Add(pkRowIndex);
            }
        }

        foreach (var index in table.UniqueIndexes)
        {
            var key = index.BuildIndexKey(newRow);
            var hits = index.LookupMutable(key);
            if (hits is null || hits.Count == 0)
                continue;

            foreach (var hit in hits)
            {
                if (seen.Add(hit.Key))
                    conflicts.Add(hit.Key);
            }
        }

        return conflicts;
    }

    private static bool TryFindRowByPrimaryKey(
        TableMock table,
        IReadOnlyDictionary<int, object?> newRow,
        out int rowIndex)
    {
        var pkIndexes = table.PkIndexArray;
        if (pkIndexes.Length == 1)
        {
            var value0 = newRow.TryGetValue(pkIndexes[0], out var pkValue0) ? pkValue0 : null;
            return table.TryFindRowByPkValues(value0, out rowIndex);
        }

        if (pkIndexes.Length == 2)
        {
            var value0 = newRow.TryGetValue(pkIndexes[0], out var pkValue0) ? pkValue0 : null;
            var value1 = newRow.TryGetValue(pkIndexes[1], out var pkValue1) ? pkValue1 : null;
            return table.TryFindRowByPkValues(value0, value1, out rowIndex);
        }

        if (pkIndexes.Length == 3)
        {
            var value0 = newRow.TryGetValue(pkIndexes[0], out var pkValue0) ? pkValue0 : null;
            var value1 = newRow.TryGetValue(pkIndexes[1], out var pkValue1) ? pkValue1 : null;
            var value2 = newRow.TryGetValue(pkIndexes[2], out var pkValue2) ? pkValue2 : null;
            return table.TryFindRowByPkValues(value0, value1, value2, out rowIndex);
        }

        var values = new object?[pkIndexes.Length];
        for (var i = 0; i < pkIndexes.Length; i++)
            values[i] = newRow.TryGetValue(pkIndexes[i], out var value) ? value : null;

        return table.TryFindRowByPkValues(values, out rowIndex);
    }

    private static void FlushPendingInsertBatch(
        QueryExecutionContext context,
        ITableMock table,
        TableMock tableMock,
        List<Dictionary<int, object?>> pendingInsertRows,
        HashSet<IndexKey>? pendingPrimaryKeys,
        List<(IndexDef Index, HashSet<IndexKey> Keys)>? pendingUniqueKeys,
        ref int insertedCount)
    {
        if (pendingInsertRows.Count == 0)
            return;

        tableMock.AddBatch(pendingInsertRows);
        insertedCount += pendingInsertRows.Count;
        TrySetLastInsertId(context, table, pendingInsertRows[^1]);
        pendingInsertRows.Clear();
        pendingPrimaryKeys?.Clear();
        if (pendingUniqueKeys is not null)
        {
            foreach (var item in pendingUniqueKeys)
                item.Keys.Clear();
        }
    }

    private static bool HasPendingBatchConflict(
        HashSet<IndexKey>? pendingPrimaryKeys,
        List<(IndexDef Index, HashSet<IndexKey> Keys)>? pendingUniqueKeys,
        IndexKey? pendingPrimaryKey,
        IndexKey[]? pendingUniqueKeysForRow)
    {
        if (pendingPrimaryKeys is null && pendingUniqueKeys is null)
            return false;

        if (pendingPrimaryKeys is not null && pendingPrimaryKey is { } pkKey)
        {
            if (pendingPrimaryKeys.Contains(pkKey))
                return true;
        }

        if (pendingUniqueKeys is not null && pendingUniqueKeysForRow is not null)
        {
            for (var i = 0; i < pendingUniqueKeys.Count; i++)
            {
                if (pendingUniqueKeys[i].Keys.Contains(pendingUniqueKeysForRow[i]))
                    return true;
            }
        }

        return false;
    }

    private static void RegisterPendingBatchKeys(
        HashSet<IndexKey>? pendingPrimaryKeys,
        List<(IndexDef Index, HashSet<IndexKey> Keys)>? pendingUniqueKeys,
        IndexKey? pendingPrimaryKey,
        IndexKey[]? pendingUniqueKeysForRow)
    {
        if (pendingPrimaryKeys is not null && pendingPrimaryKey is { } pkKey)
            pendingPrimaryKeys.Add(pkKey);

        if (pendingUniqueKeys is not null && pendingUniqueKeysForRow is not null)
        {
            for (var i = 0; i < pendingUniqueKeys.Count; i++)
                pendingUniqueKeys[i].Keys.Add(pendingUniqueKeysForRow[i]);
        }
    }

    private static void BuildPendingBatchKeys(
        TableMock tableMock,
        HashSet<IndexKey>? pendingPrimaryKeys,
        List<(IndexDef Index, HashSet<IndexKey> Keys)>? pendingUniqueKeys,
        IndexKey[]? pendingUniqueKeysBuffer,
        IReadOnlyDictionary<int, object?> row,
        out IndexKey? pendingPrimaryKey,
        out IndexKey[]? pendingUniqueKeysForRow)
    {
        pendingPrimaryKey = null;
        pendingUniqueKeysForRow = pendingUniqueKeysBuffer;

        if (pendingPrimaryKeys is not null)
            pendingPrimaryKey = tableMock.BuildPkKey(row);

        if (pendingUniqueKeys is not null && pendingUniqueKeysForRow is not null)
        {
            for (var i = 0; i < pendingUniqueKeys.Count; i++)
                pendingUniqueKeysForRow[i] = pendingUniqueKeys[i].Index.BuildIndexKey(row);
        }
    }

    private static void ValidateInsertPartitions(SqlInsertQuery query, TableMock table)
    {
        var requestedPartitionNames = GetRequestedPartitionNames(query);
        if (requestedPartitionNames.Count == 0)
            return;

        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            throw new InvalidOperationException("INSERT PARTITION requires captured partition metadata on the target table.");

        var availablePartitions = ExtractPartitionNames(table.PartitionClauseSql!);
        if (availablePartitions.Count == 0)
            throw new InvalidOperationException("INSERT PARTITION requires named partitions in the target table metadata.");

        foreach (var partitionName in requestedPartitionNames)
        {
            if (!availablePartitions.Contains(partitionName))
                throw new InvalidOperationException($"Unknown partition '{partitionName}'.");
        }
    }

    private static void ValidatePartitionedInsertRows(
        SqlInsertQuery query,
        TableMock table,
        IReadOnlyList<Dictionary<int, object?>> rows)
    {
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return;

        var requestedPartitionNames = GetRequestedPartitionNames(query);
        foreach (var row in rows)
        {
            if (requestedPartitionNames.Count > 0)
            {
                if (table.MatchesRequestedPartitions(row, requestedPartitionNames))
                    continue;

                throw new InvalidOperationException("INSERT PARTITION row does not belong to the requested partition subset.");
            }

            if (table.TryGetPartitionName(row, out _))
                continue;

            throw new InvalidOperationException("INSERT row does not belong to any defined partition.");
        }
    }

    private static IReadOnlyList<string> GetRequestedPartitionNames(SqlInsertQuery query)
        => query.PartitionNames.Count > 0
            ? query.PartitionNames
            : query.Table?.PartitionNames ?? [];

    private static HashSet<string> ExtractPartitionNames(string partitionClauseSql)
    {
        var partitions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in Regex.Matches(partitionClauseSql, @"\bPARTITION\s+(?!BY)(?<name>`?[A-Za-z0-9_]+`?)", RegexOptions.IgnoreCase))
        {
            var name = match.Groups["name"].Value.Trim('`', '"', '[', ']').NormalizeName();
            if (!string.IsNullOrWhiteSpace(name))
                partitions.Add(name);
        }

        return partitions;
    }

    private static bool CanUseBatchInsert(
        QueryExecutionContext context,
        ITableMock table,
        string tableName,
        string? schemaName,
        TableMock tableMock)
    {
        if (tableMock.ForeignKeys.Count > 0)
            return false;

        if (!context.Dialect.SupportsTriggers)
            return true;

        if (context.Connection.IsTemporaryTable(table, tableName, schemaName))
            return true;

        return !tableMock.HasRegisteredTriggers();
    }

    private static void TrySetLastInsertId(QueryExecutionContext context, ITableMock table, IReadOnlyDictionary<int, object?> insertedRow)
    {
        var identityColumn = table is TableMock tableMock
            ? tableMock.IdentityColumn
            : GetIdentityColumn(table);
        if (identityColumn is null)
            return;

        if (!insertedRow.TryGetValue(identityColumn.Index, out var value))
            return;

        context.Connection.SetLastInsertId(value);
    }

    private static ColumnDef? GetIdentityColumn(ITableMock table)
    {
        foreach (var column in table.Columns.Values)
        {
            if (column.Identity)
                return column;
        }

        return null;
    }

    private static int CountInputTables(SqlSelectQuery query)
    {
        var count = query.Table is not null ? 1 : 0;
        count += query.Joins.Count;
        return Math.Max(1, count);
    }

    // --- Helpers de Criação de Linhas ---

    private static List<Dictionary<int, object?>> CreateRowsFromValues(
        QueryExecutionContext context,
        SqlInsertQuery query,
        ITableMock table)
    {
        if (query.ValuesRaw.Count == 1)
            return [CreateSingleRowFromValues(context, query, table)];

        var dialect = context.Dialect;
        var rows = new List<Dictionary<int, object?>>(query.ValuesRaw.Count);
        var colNames = query.Columns; // Lista de colunas do Insert
        ColumnDef[]? explicitTargetColumns = null;
        IReadOnlyList<ColumnDef>? orderedTableColumns = null;
        List<ColumnDef>? nonIdentityColumns = null;
        IReadOnlyList<ColumnDef> targetColumns;
        var targetColumnCount = 0;

        var colNamesCount = colNames.Count;
        var firstValueCount = query.ValuesRaw.Count > 0 ? query.ValuesRaw[0].Count : 0;
        if (colNamesCount > 0)
        {
            explicitTargetColumns = new ColumnDef[colNamesCount];
            for (var i = 0; i < colNamesCount; i++)
                explicitTargetColumns[i] = ResolveInsertColumn(table, colNames[i], dialect);
            targetColumns = explicitTargetColumns;
            targetColumnCount = explicitTargetColumns.Length;
        }
        else
        {
            orderedTableColumns = table is TableMock tableMock
                ? tableMock.ColumnsByOrdinal
                : [.. table.Columns.Values.OrderBy(c => c.Index)];
            var orderedCount = orderedTableColumns.Count;
            nonIdentityColumns = new List<ColumnDef>(orderedCount);
            for (var i = 0; i < orderedCount; i++)
            {
                var col = orderedTableColumns[i];
                if (!col.Identity)
                    nonIdentityColumns.Add(col);
            }

            targetColumns = firstValueCount == orderedTableColumns.Count
                ? orderedTableColumns
                : firstValueCount == nonIdentityColumns.Count
                    ? nonIdentityColumns
                    : orderedTableColumns;
            targetColumnCount = firstValueCount < targetColumns.Count
                ? firstValueCount
                : targetColumns.Count;
        }

        var valuesRawCount = query.ValuesRaw.Count;
        var valuesExprCount = query.ValuesExpr.Count;
        for (var rowIndex = 0; rowIndex < valuesRawCount; rowIndex++)
        {
            var valueBlock = query.ValuesRaw[rowIndex];
            var parsedExprBlock = rowIndex < valuesExprCount
                ? query.ValuesExpr[rowIndex]
                : null;
            var valueCount = valueBlock.Count;
            var parsedExprCount = parsedExprBlock?.Count ?? 0;

            // Validação de count
            if (colNamesCount > 0 && colNamesCount != valueCount)
                throw new InvalidOperationException($"Column count ({colNamesCount}) does not match value count ({valueCount}).");

            var newRow = new Dictionary<int, object?>(Math.Max(1, valueCount));

            if (targetColumnCount > 0)
            {
                var limit = colNamesCount > 0
                    ? targetColumnCount
                    : valueCount < targetColumnCount
                        ? valueCount
                        : targetColumnCount;

                for (var i = 0; i < limit; i++)
                {
                    var parsedExpr = parsedExprBlock is not null && i < parsedExprCount
                        ? parsedExprBlock[i]
                        : null;
                    SetColValue(context, table, targetColumns[i], valueBlock[i], parsedExpr, newRow);
                }
            }

            rows.Add(newRow);
        }
        return rows;
    }

    private static Dictionary<int, object?> CreateSingleRowFromValues(
        QueryExecutionContext context,
        SqlInsertQuery query,
        ITableMock table)
    {
        var dialect = context.Dialect;
        var colNames = query.Columns;
        var colNamesCount = colNames.Count;
        var valueBlock = query.ValuesRaw[0];
        var valueCount = valueBlock.Count;
        var parsedExprBlock = query.ValuesExpr.Count > 0 ? query.ValuesExpr[0] : null;
        var parsedExprCount = parsedExprBlock?.Count ?? 0;

        if (colNamesCount > 0 && colNamesCount != valueCount)
            throw new InvalidOperationException($"Column count ({colNamesCount}) does not match value count ({valueCount}).");

        ColumnDef[]? explicitTargetColumns = null;
        IReadOnlyList<ColumnDef>? orderedTableColumns = null;
        List<ColumnDef>? nonIdentityColumns = null;
        IReadOnlyList<ColumnDef> targetColumns;
        var targetColumnCount = 0;

        var firstValueCount = valueCount;
        if (colNamesCount > 0)
        {
            explicitTargetColumns = new ColumnDef[colNamesCount];
            for (var i = 0; i < colNamesCount; i++)
                explicitTargetColumns[i] = ResolveInsertColumn(table, colNames[i], dialect);
            targetColumns = explicitTargetColumns;
            targetColumnCount = explicitTargetColumns.Length;
        }
        else
        {
            orderedTableColumns = table is TableMock tableMock
                ? tableMock.ColumnsByOrdinal
                : [.. table.Columns.Values.OrderBy(c => c.Index)];
            var orderedCount = orderedTableColumns.Count;
            nonIdentityColumns = new List<ColumnDef>(orderedCount);
            for (var i = 0; i < orderedCount; i++)
            {
                var col = orderedTableColumns[i];
                if (!col.Identity)
                    nonIdentityColumns.Add(col);
            }

            targetColumns = firstValueCount == orderedTableColumns.Count
                ? orderedTableColumns
                : firstValueCount == nonIdentityColumns.Count
                    ? nonIdentityColumns
                    : orderedTableColumns;
            targetColumnCount = firstValueCount < targetColumns.Count
                ? firstValueCount
                : targetColumns.Count;
        }

        var newRow = new Dictionary<int, object?>(Math.Max(1, valueCount));
        if (targetColumnCount > 0)
        {
            var limit = colNamesCount > 0
                ? targetColumnCount
                : valueCount < targetColumnCount
                    ? valueCount
                    : targetColumnCount;

            for (var i = 0; i < limit; i++)
            {
                var parsedExpr = parsedExprBlock is not null && i < parsedExprCount
                    ? parsedExprBlock[i]
                    : null;
                SetColValue(context, table, targetColumns[i], valueBlock[i], parsedExpr, newRow);
            }
        }

        return newRow;
    }

    private static ColumnDef ResolveInsertColumn(ITableMock table, string columnName, ISqlDialect dialect)
    {
        foreach (var candidate in BuildInsertColumnCandidates(columnName, dialect))
        {
            if (TryGetColumn(table, candidate, out var col))
                return col;
        }

        return table.GetColumn(columnName);
    }

    private static List<string> BuildInsertColumnCandidates(string columnName, ISqlDialect dialect)
    {
        var candidates = new List<string>();
        var normalized = columnName.Trim();
        AddInsertColumnCandidate(candidates, normalized);

        var trimmedPunctuation = normalized.TrimEnd(',', ';');
        AddInsertColumnCandidate(candidates, trimmedPunctuation);

        AddInsertColumnCandidate(candidates, GetFirstInsertColumnToken(trimmedPunctuation));

        AppendQuotedInsertColumnCandidates(candidates, normalized, dialect);

        return candidates;
    }

    private static string? GetFirstInsertColumnToken(string value)
    {
        var span = value.AsSpan();
        var start = 0;
        while (start < span.Length && char.IsWhiteSpace(span[start]))
            start++;

        if (start >= span.Length)
            return null;

        var end = start;
        while (end < span.Length && !char.IsWhiteSpace(span[end]))
            end++;

        return span[start..end].ToString();
    }

    private static void AppendQuotedInsertColumnCandidates(List<string> candidates, string normalized, ISqlDialect dialect)
    {
        AddInsertColumnCandidate(candidates, UnwrapInsertColumnIdentifier(normalized, '"', dialect.AllowsDoubleQuoteIdentifiers));
        AddInsertColumnCandidate(candidates, UnwrapInsertColumnIdentifier(normalized, '[', dialect.AllowsBracketIdentifiers));
        AddInsertColumnCandidate(candidates, UnwrapInsertColumnIdentifier(normalized, '`', dialect.AllowsBacktickIdentifiers));
    }

    private static void AddInsertColumnCandidate(List<string> candidates, string? candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate))
            return;

        for (var i = 0; i < candidates.Count; i++)
        {
            if (string.Equals(candidates[i], candidate, StringComparison.Ordinal))
                return;
        }

        candidates.Add(candidate!);
    }

    private static bool TrySplitQualifiedName(string rawName, out string firstPart, out string secondPart)
    {
        firstPart = string.Empty;
        secondPart = string.Empty;

        var span = rawName.AsSpan();
        var firstDot = span.IndexOf('.');
        if (firstDot < 0)
            return false;

        var first = span[..firstDot].Trim();
        if (first.Length == 0)
            return false;

        firstPart = first.ToString();
        var rest = span[(firstDot + 1)..];
        var secondDot = rest.IndexOf('.');
        secondPart = (secondDot >= 0 ? rest[..secondDot] : rest).Trim().ToString();
        return true;
    }

    private static string GetUnqualifiedName(string name)
    {
        var dotIndex = name.LastIndexOf('.');
        if (dotIndex < 0 || dotIndex + 1 >= name.Length)
            return name;

        return name[(dotIndex + 1)..];
    }

    private static string? UnwrapInsertColumnIdentifier(string value, char quoteStart, bool isAllowed)
    {
        if (!isAllowed || value.Length < 2)
            return null;

        return quoteStart switch
        {
            '"' when value[0] == '"' && value[^1] == '"' => value[1..^1],
            '[' when value[0] == '[' && value[^1] == ']' => value[1..^1],
            '`' when value[0] == '`' && value[^1] == '`' => value[1..^1],
            _ => null
        };
    }

    private static bool TryGetColumn(ITableMock table, string columnName, out ColumnDef col)
    {
        var normalized = columnName.NormalizeName();
        if (table.Columns.TryGetValue(normalized, out col!))
            return true;

        var dotIndex = normalized.LastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < normalized.Length)
        {
            var unqualified = normalized[(dotIndex + 1)..];
            if (table.Columns.TryGetValue(unqualified, out col!))
                return true;
        }

        col = null!;
        return false;
    }

    private static List<Dictionary<int, object?>> CreateRowsFromSelect(
        QueryExecutionContext context,
        SqlInsertQuery query,
        ITableMock targetTable)
    {
        var executor = context.CreateExecutor();
        var res = executor.ExecuteSelect(query.InsertSelect!);

        var rows = new List<Dictionary<int, object?>>(res.Count);
        var colNames = query.Columns;
        var colNamesCount = colNames.Count;
        int[]? explicitTargetIndexes = null;
        IReadOnlyList<ColumnDef>? orderedTableColumns = null;
        var selectColumnCount = res.Columns.Count;

        if (colNamesCount > 0 && colNamesCount != selectColumnCount)
            throw new InvalidOperationException(SqlExceptionMessages.ColumnCountDoesNotMatchSelectList());

        if (colNamesCount > 0)
        {
            explicitTargetIndexes = new int[colNamesCount];
            for (var i = 0; i < colNamesCount; i++)
                explicitTargetIndexes[i] = targetTable.GetColumn(colNames[i]).Index;
        }
        else
            orderedTableColumns = targetTable is TableMock targetTableMock
                ? targetTableMock.ColumnsByOrdinal
                : [.. targetTable.Columns.Values.OrderBy(c => c.Index)];

        var orderedTargetColumns = orderedTableColumns;
        var orderedTargetCount = orderedTargetColumns?.Count ?? 0;
        int[]? orderedTargetIndexes = null;
        if (orderedTargetColumns is not null)
        {
            orderedTargetIndexes = new int[orderedTargetCount];
            for (var i = 0; i < orderedTargetCount; i++)
                orderedTargetIndexes[i] = orderedTargetColumns[i].Index;
        }

        var selectLimit = orderedTargetColumns is not null
            ? (selectColumnCount < orderedTargetCount ? selectColumnCount : orderedTargetCount)
            : 0;

        foreach (var srcRow in res)
        {
            var newRow = new Dictionary<int, object?>(Math.Max(1, selectColumnCount));

            if (colNamesCount > 0)
            {
                var targetIndexes = explicitTargetIndexes!;
                var targetCount = targetIndexes.Length;
                for (int i = 0; i < targetCount; i++)
                {
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[targetIndexes[i]] = (val is DBNull) ? null : val;
                }
            }
            else
            {
                var targetIndexes = orderedTargetIndexes!;
                for (int i = 0; i < selectLimit; i++)
                {
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[targetIndexes[i]] = (val is DBNull) ? null : val;
                }
            }

            rows.Add(newRow);
        }
        return rows;
    }

    private static void SetColValue(
        QueryExecutionContext context,
        ITableMock table,
        ColumnDef colDef,
        string rawValue,
        SqlExpr? parsedExpr,
        Dictionary<int, object?> row)
    {
        object? resolved;
        if (parsedExpr is LiteralExpr literalExpr)
        {
            resolved = literalExpr.Value;
        }
        else if (parsedExpr is ParameterExpr parameterExpr
            && context.TryResolveParameter(parameterExpr.Name, out var parameterValue))
        {
            resolved = parameterValue;
        }
        else if (TryResolveCastAsJsonValue(parsedExpr, context, out var castJsonValue))
        {
            resolved = castJsonValue;
        }
        else if (TryResolveParsedSpecialValue(context, table, parsedExpr, out var parsedValue))
        {
            resolved = parsedValue;
        }
        else
        {
            var trimmedRawValue = rawValue;
            if (trimmedRawValue.Length > 0
                && (char.IsWhiteSpace(trimmedRawValue[0]) || char.IsWhiteSpace(trimmedRawValue[^1])))
            {
                trimmedRawValue = trimmedRawValue.Trim();
            }

            resolved = TryResolveTemporalValue(trimmedRawValue, context, out var temporalValue)
                ? temporalValue
                : context.TryResolveParameter(trimmedRawValue, out var rawParameterValue)
                    ? rawParameterValue
                    : ResolveInsertFallbackValue(table, colDef, rawValue, context);
        }

        resolved = context.NormalizeResolvedValue(resolved);
        var val = (resolved is DBNull) ? null : NormalizeValueForColumn(colDef.DbType, resolved);
        if (val == null && !colDef.Nullable)
            throw table.ColumnCannotBeNull("Idx:" + colDef.Index);

        row[colDef.Index] = val;
    }

    private static object? ResolveInsertFallbackValue(
        ITableMock table,
        ColumnDef colDef,
        string rawValue,
        QueryExecutionContext context)
    {
        table.CurrentColumn = colDef.Name;
        try
        {
            return table.Resolve(rawValue, colDef.DbType, colDef.Nullable, context.DbParameters, table.Columns);
        }
        finally
        {
            table.CurrentColumn = null;
        }
    }

    private static object? NormalizeValueForColumn(DbType dbType, object? value)
    {
        if (value is null or DBNull)
            return value;

        return dbType switch
        {
            DbType.Byte => value is byte ? value : Convert.ToByte(value, CultureInfo.InvariantCulture),
            DbType.SByte => value is sbyte ? value : Convert.ToSByte(value, CultureInfo.InvariantCulture),
            DbType.Int16 => value is short ? value : Convert.ToInt16(value, CultureInfo.InvariantCulture),
            DbType.Int32 => value is int ? value : Convert.ToInt32(value, CultureInfo.InvariantCulture),
            DbType.Int64 => value is long ? value : Convert.ToInt64(value, CultureInfo.InvariantCulture),
            DbType.UInt16 => value is ushort ? value : Convert.ToUInt16(value, CultureInfo.InvariantCulture),
            DbType.UInt32 => value is uint ? value : Convert.ToUInt32(value, CultureInfo.InvariantCulture),
            DbType.UInt64 => value is ulong ? value : Convert.ToUInt64(value, CultureInfo.InvariantCulture),
            DbType.Boolean => value is bool ? value : Convert.ToBoolean(value, CultureInfo.InvariantCulture),
            DbType.Decimal or DbType.Currency => value is decimal ? value : Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            DbType.Double => value is double ? value : Convert.ToDouble(value, CultureInfo.InvariantCulture),
            DbType.Single => value is float ? value : Convert.ToSingle(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    private static bool TryResolveParsedSpecialValue(
        QueryExecutionContext context,
        ITableMock table,
        SqlExpr? parsedExpr,
        out object? value)
    {
        var connection = context.Connection;
        var dialect = context.Dialect;
        value = null;
        if (parsedExpr is null)
            return false;

            object? Eval(SqlExpr expr)
            {
                return expr switch
                {
                    LiteralExpr lit => lit.Value,
                    ParameterExpr p when context.TryResolveParameter(p.Name, out var parameterValue) => parameterValue,
                    IdentifierExpr id when context.TryEvaluateZeroArgIdentifier(id.Name, out var temporalIdentifierValue) => temporalIdentifierValue,
                    ColumnExpr c when context.TryEvaluateZeroArgIdentifier(c.Name, out var temporalColumnValue) => temporalColumnValue,
                    _ => null
                };
            }

        if (parsedExpr is CallExpr call)
        {
            EnsureDialectSupportsSequenceFunction(dialect, call.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(connection, call.Name, call.Args, Eval, out var callValue))
            {
                value = callValue;
                return true;
            }
        }

        if (parsedExpr is FunctionCallExpr function)
        {
            EnsureDialectSupportsSequenceFunction(dialect, function.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(connection, function.Name, function.Args, Eval, out var functionValue))
            {
                value = functionValue;
                return true;
            }
        }

        return false;
    }

    private static bool TryResolveCastAsJsonValue(
        SqlExpr? parsedExpr,
        QueryExecutionContext context,
        out object? value)
    {
        value = null;
        if (parsedExpr is not CallExpr cast
            || !cast.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || cast.Args.Count < 2
            || !IsJsonCastType(cast.Args[1]))
            return false;

        if (!TryResolveCastOperandValue(cast.Args[0], context, out var operand))
            return false;

        value = NormalizeJsonCastValue(operand);
        return true;
    }

    private static bool IsJsonCastType(SqlExpr expr)
    {
        if (expr is RawSqlExpr raw)
        {
            var sqlType = raw.Sql.Trim();
            return sqlType.Equals("JSON", StringComparison.OrdinalIgnoreCase)
                || sqlType.Equals("JSONB", StringComparison.OrdinalIgnoreCase);
        }

        if (expr is LiteralExpr { Value: string s })
        {
            var sqlType = s.Trim();
            return sqlType.Equals("JSON", StringComparison.OrdinalIgnoreCase)
                || sqlType.Equals("JSONB", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool TryResolveCastOperandValue(
        SqlExpr expr,
        QueryExecutionContext context,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                value = lit.Value;
                return true;
            case ParameterExpr p:
                return context.TryResolveParameter(p.Name, out value);
            default:
                value = null;
                return false;
        }
    }

    private static object? NormalizeJsonCastValue(object? operand)
    {
        if (operand is null || operand is DBNull)
            return null;

        if (operand is JsonDocument || operand is JsonElement)
            return operand;

        if (operand is string)
            return operand;

        return JsonSerializer.Serialize(operand);
    }

    private static bool TryResolveTemporalValue(
        string rawValue,
        QueryExecutionContext context,
        out object? value)
    {
        var trimmed = rawValue.Trim();
        if (context.TryEvaluateZeroArgIdentifier(trimmed, out value))
            return true;

        var openParen = trimmed.IndexOf('(');
        var closeParen = trimmed.LastIndexOf(')');
        if (openParen <= 0 || closeParen <= openParen)
            return false;

        var functionName = trimmed[..openParen].Trim();
        var argsRaw = trimmed[(openParen + 1)..closeParen].Trim();
        if (argsRaw.Length > 0)
            return false;

        return context.TryEvaluateZeroArgCall(functionName, out value);
    }

    private static void ApplyOnDuplicateUpdateAstInMemory(
        ITableMock table,
        int existinIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        IReadOnlyList<SqlAssignment> assigns,
        QueryExecutionContext context,
        IDictionary<int, object?> targetRow)
    {
        object? GetInsertedColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return insertedRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        object? GetExistingColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return targetRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        static object? Coerce(DbType dbType, object? value)
        {
            if (value is null || value is DBNull) return null;
            try
            {
                return dbType switch
                {
                    DbType.String => value.ToString(),
                    DbType.Int16 => Convert.ToInt16(value),
                    DbType.Int32 => Convert.ToInt32(value),
                    DbType.Int64 => Convert.ToInt64(value),
                    DbType.Byte => Convert.ToByte(value),
                    DbType.Boolean => value is bool b ? b : Convert.ToInt32(value) != 0,
                    DbType.Decimal => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                    DbType.Double => Convert.ToDouble(value),
                    DbType.Single => Convert.ToSingle(value),
                    DbType.DateTime => value is DateTime dt ? dt : Convert.ToDateTime(value),
                    _ => value
                };
            }
            catch { return value; }
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => context.TryResolveParameter(p.Name, out var parameterValue) ? parameterValue : null,
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : context.TryEvaluateZeroArgIdentifier(id.Name, out var temporalIdentifierValue)
                        ? temporalIdentifierValue
                        : GetExistingColumnValue(GetUnqualifiedName(id.Name)),
                ColumnExpr c => string.Equals(c.Qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
                    ? GetInsertedColumnValue(c.Name)
                    : GetExistingColumnValue(c.Name),
                UnaryExpr u when u.Op == SqlUnaryOp.Not => !(Convert.ToBoolean(Eval(u.Expr) ?? false)),
                IsNullExpr n => (Eval(n.Expr) is null) ^ n.Negated,
                BinaryExpr b => b.Op switch
                {
                    SqlBinaryOp.Add => (Convert.ToDecimal(Eval(b.Left) ?? 0m) + Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Subtract => (Convert.ToDecimal(Eval(b.Left) ?? 0m) - Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Multiply => (Convert.ToDecimal(Eval(b.Left) ?? 0m) * Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Divide => (Convert.ToDecimal(Eval(b.Left) ?? 0m) / Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Concat => EvalConcat(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Eq => Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Neq => !Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.And => Convert.ToBoolean(Eval(b.Left) ?? false) && Convert.ToBoolean(Eval(b.Right) ?? false),
                    SqlBinaryOp.Or => Convert.ToBoolean(Eval(b.Left) ?? false) || Convert.ToBoolean(Eval(b.Right) ?? false),
                    _ => throw new NotSupportedException($"Operador não suportado em ON DUPLICATE: {b.Op}")
                },
                CallExpr call => EvalCall(call),
                FunctionCallExpr fn => EvalFunction(fn),
                _ => throw new NotSupportedException($"Expressão não suportada em ON DUPLICATE: {expr.GetType().Name}")
            };
        }

        object? EvalConcat(object? left, object? right)
        {
            var nullInputReturnsNull = context.Dialect.PlusStringConcatReturnsNullOnNullInput;
            if (left is null or DBNull || right is null or DBNull)
            {
                if (nullInputReturnsNull)
                    return null;
            }

            var leftText = left is null or DBNull ? string.Empty : left.ToString() ?? string.Empty;
            var rightText = right is null or DBNull ? string.Empty : right.ToString() ?? string.Empty;
            return string.Concat(leftText, rightText);
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            EnsureDialectSupportsSequenceFunction(context.Dialect, fn.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, fn.Name, fn.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (fn.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase)
                && fn.Args.Count == 1)
            {
                var col = fn.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };

                if (!string.IsNullOrWhiteSpace(col))
                    return GetInsertedColumnValue(col!);
            }

            throw new NotSupportedException($"Função não suportada em ON DUPLICATE: {fn.Name}()");
        }

        object? EvalCall(CallExpr call)
        {
            EnsureDialectSupportsSequenceFunction(context.Dialect, call.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, call.Name, call.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (call.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase)
                && call.Args.Count == 1)
            {
                var col = call.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };

                if (string.IsNullOrWhiteSpace(col))
                    throw new NotSupportedException("VALUES() espera 1 coluna");

                return GetInsertedColumnValue(col!);
            }

            throw new NotSupportedException($"CALL não suportado em ON DUPLICATE: {call.Name}");
        }

        bool TryGetExcludedValueFromName(string rawName, out object? val)
        {
            val = null;
            var n = rawName.Trim();
            int dot = n.IndexOf('.');
            if (dot <= 0) return false;

            var qualifier = n[..dot];
            var col = n[(dot + 1)..];

            if (!(string.Equals(qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
               || string.Equals(qualifier, "values", StringComparison.OrdinalIgnoreCase)))
                return false;

            val = GetInsertedColumnValue(col);
            return true;
        }

        foreach (var assignment in assigns)
        {
            var colInfo = table.GetColumn(assignment.Column);
            if (colInfo.GetGenValue != null) continue;
            var expr = assignment.ValueExpr ?? SqlExpressionParser.ParseScalar(
                assignment.ValueRaw,
                context.Connection.Db,
                context.Dialect,
                null,
                SqlCustomFunctionResolverFactory.Create(table.Schema.Db, table.Schema.SchemaName));
            var resolved = Eval(expr);
            var coerced = Coerce(colInfo.DbType, resolved);
            targetRow[colInfo.Index] = coerced;
        }
    }

    private static void ApplyOnDuplicateUpdateAst(
        ITableMock table,
        int existinIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        IReadOnlyList<SqlAssignment> assigns,
        QueryExecutionContext context)
    {
        object? GetInsertedColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return insertedRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        object? GetExistingColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return table[existinIndex].TryGetValue(info.Index, out var v) ? v : null;
        }

        static object? Coerce(DbType dbType, object? value)
        {
            if (value is null || value is DBNull) return null;

            try
            {
                return dbType switch
                {
                    DbType.String => value.ToString(),
                    DbType.Int16 => Convert.ToInt16(value),
                    DbType.Int32 => Convert.ToInt32(value),
                    DbType.Int64 => Convert.ToInt64(value),
                    DbType.Byte => Convert.ToByte(value),
                    DbType.Boolean => value is bool b ? b : Convert.ToInt32(value) != 0,
                    DbType.Decimal => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                    DbType.Double => Convert.ToDouble(value),
                    DbType.Single => Convert.ToSingle(value),
                    DbType.DateTime => value is DateTime dt ? dt : Convert.ToDateTime(value),
                    _ => value
                };
            }
            catch
            {
                return value;
            }
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => context.TryResolveParameter(p.Name, out var parameterValue) ? parameterValue : null,
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : GetExistingColumnValue(GetUnqualifiedName(id.Name)),
                ColumnExpr c => string.Equals(c.Qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
                    ? GetInsertedColumnValue(c.Name)
                    : GetExistingColumnValue(c.Name),
                UnaryExpr u when u.Op == SqlUnaryOp.Not => !(Convert.ToBoolean(Eval(u.Expr) ?? false)),
                IsNullExpr n => (Eval(n.Expr) is null) ^ n.Negated,
                BinaryExpr b => b.Op switch
                {
                    SqlBinaryOp.Add => (Convert.ToDecimal(Eval(b.Left) ?? 0m) + Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Subtract => (Convert.ToDecimal(Eval(b.Left) ?? 0m) - Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Multiply => (Convert.ToDecimal(Eval(b.Left) ?? 0m) * Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Divide => (Convert.ToDecimal(Eval(b.Left) ?? 0m) / Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Concat => EvalConcat(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Eq => Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Neq => !Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.And => Convert.ToBoolean(Eval(b.Left) ?? false) && Convert.ToBoolean(Eval(b.Right) ?? false),
                    SqlBinaryOp.Or => Convert.ToBoolean(Eval(b.Left) ?? false) || Convert.ToBoolean(Eval(b.Right) ?? false),
                    _ => throw new InvalidOperationException($"Operador não suportado no ON DUPLICATE: {b.Op}")
                },
                CallExpr call => EvalCall(call),
                FunctionCallExpr fn => EvalFunction(fn),
                RawSqlExpr raw => throw new InvalidOperationException($"Expressão não suportada no ON DUPLICATE: {raw.Sql}"),
                _ => throw new InvalidOperationException($"Expressão não suportada no ON DUPLICATE: {expr.GetType().Name}")
            };
        }

        object? EvalConcat(object? left, object? right)
        {
            var nullInputReturnsNull = context.Dialect.PlusStringConcatReturnsNullOnNullInput;
            if (left is null or DBNull || right is null or DBNull)
            {
                if (nullInputReturnsNull)
                    return null;
            }

            var leftText = left is null or DBNull ? string.Empty : left.ToString() ?? string.Empty;
            var rightText = right is null or DBNull ? string.Empty : right.ToString() ?? string.Empty;
            return string.Concat(leftText, rightText);
        }

        bool TryGetExcludedValueFromName(string rawName, out object? value)
        {
            value = null;
            if (string.IsNullOrWhiteSpace(rawName))
                return false;

            if (TrySplitQualifiedName(rawName, out var firstPart, out var secondPart)
                && string.Equals(firstPart, "excluded", StringComparison.OrdinalIgnoreCase))
            {
                value = GetInsertedColumnValue(secondPart);
                return true;
            }

            return false;
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            var name = fn.Name;
            EnsureDialectSupportsSequenceFunction(context.Dialect, name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, name, fn.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (fn.Args.Count == 0 && context.TryEvaluateZeroArgCall(name, out var temporalValue))
                return temporalValue;

            if (name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase)
                && fn.Args.Count == 1)
            {
                var col = fn.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };
                if (!string.IsNullOrWhiteSpace(col))
                    return GetInsertedColumnValue(col!);
            }

            throw new InvalidOperationException($"Função não suportada no ON DUPLICATE: {fn.Name}()");
        }

        object? EvalCall(CallExpr call)
        {
            var name = call.Name;
            EnsureDialectSupportsSequenceFunction(context.Dialect, name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, name, call.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase)
                && call.Args.Count == 1)
            {
                var col = call.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };
                if (string.IsNullOrWhiteSpace(col))
                    throw new InvalidOperationException("VALUES() espera 1 coluna");

                return GetInsertedColumnValue(col!);
            }

            if (call.Args.Count == 0 && context.TryEvaluateZeroArgCall(name, out var temporalValue))
                return temporalValue;

            throw new InvalidOperationException($"CALL não suportado no ON DUPLICATE: {call.Name}");
        }

        foreach (var assignment in assigns)
        {
            var colInfo = table.GetColumn(assignment.Column);
            if (colInfo.GetGenValue != null) continue;

            var ast = assignment.ValueExpr ?? SqlExpressionParser.ParseScalar(
                assignment.ValueRaw,
                context.Connection.Db,
                context.Dialect,
                null,
                SqlCustomFunctionResolverFactory.Create(table.Schema.Db, table.Schema.SchemaName));
            var value = Eval(ast);

            table.UpdateRowColumn(
                existinIndex,
                colInfo.Index,
                Coerce(colInfo.DbType, value));
        }
    }

    private static bool ShouldApplyOnConflictUpdateWhere(
        SqlInsertQuery query,
        ITableMock table,
        int existingIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        QueryExecutionContext context)
    {
        var where = query.OnConflictUpdateWhereExpr;
        if (where is null)
            return true;

        object? GetInsertedColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return insertedRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        object? GetExistingColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return table[existingIndex].TryGetValue(info.Index, out var v) ? v : null;
        }

        bool TryGetExcludedValueFromName(string rawName, out object? value)
        {
            value = null;
            if (string.IsNullOrWhiteSpace(rawName)) return false;
            if (!TrySplitQualifiedName(rawName, out var firstPart, out var secondPart))
                return false;
            if (string.Equals(firstPart, "excluded", StringComparison.OrdinalIgnoreCase)
                || string.Equals(firstPart, "values", StringComparison.OrdinalIgnoreCase))
            {
                value = GetInsertedColumnValue(secondPart);
                return true;
            }
            return false;
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            EnsureDialectSupportsSequenceFunction(context.Dialect, fn.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, fn.Name, fn.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (fn.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase) && fn.Args.Count == 1)
            {
                var col = fn.Args[0] switch { IdentifierExpr id => id.Name, ColumnExpr c => c.Name, _ => null };
                if (!string.IsNullOrWhiteSpace(col)) return GetInsertedColumnValue(col!);
            }
            if (fn.Args.Count == 0 && context.TryEvaluateZeroArgCall(fn.Name, out var temporal))
                return temporal;
            throw new InvalidOperationException($"Função não suportada no ON CONFLICT WHERE: {fn.Name}()");
        }

        object? EvalCall(CallExpr call)
        {
            EnsureDialectSupportsSequenceFunction(context.Dialect, call.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, call.Name, call.Args, Eval, out var sequenceValue))
                return sequenceValue;
            if (call.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase) && call.Args.Count == 1)
            {
                var col = call.Args[0] switch { IdentifierExpr id => id.Name, ColumnExpr c => c.Name, _ => null };
                if (string.IsNullOrWhiteSpace(col)) throw new InvalidOperationException("VALUES() espera 1 coluna");
                return GetInsertedColumnValue(col!);
            }
            if (call.Args.Count == 0 && context.TryEvaluateZeroArgCall(call.Name, out var temporal))
                return temporal;
            throw new InvalidOperationException($"CALL não suportado no ON CONFLICT WHERE: {call.Name}");
        }

        bool CompareBinary(SqlBinaryOp op, object? left, object? right)
        {
            return op switch
            {
                SqlBinaryOp.Eq => left.EqualsSql(right, context),
                SqlBinaryOp.Neq => !left.EqualsSql(right, context),
                SqlBinaryOp.Greater => left is not null && right is not null && context.Compare(left, right) > 0,
                SqlBinaryOp.GreaterOrEqual => left is not null && right is not null && context.Compare(left, right) >= 0,
                SqlBinaryOp.Less => left is not null && right is not null && context.Compare(left, right) < 0,
                SqlBinaryOp.LessOrEqual => left is not null && right is not null && context.Compare(left, right) <= 0,
                _ => throw new InvalidOperationException($"Operador não suportado no ON CONFLICT WHERE: {op}")
            };
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => context.TryResolveParameter(p.Name, out var parameterValue) ? parameterValue : null,
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : context.TryEvaluateZeroArgIdentifier(id.Name, out var temporalIdentifierValue)
                        ? temporalIdentifierValue
                        : GetExistingColumnValue(GetUnqualifiedName(id.Name)),
                ColumnExpr c => string.Equals(c.Qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
                    ? GetInsertedColumnValue(c.Name)
                    : GetExistingColumnValue(c.Name),
                UnaryExpr u when u.Op == SqlUnaryOp.Not => !Eval(u.Expr).ToBool(),
                IsNullExpr n => (Eval(n.Expr) is null) ^ n.Negated,
                BinaryExpr b when b.Op == SqlBinaryOp.Add => Eval(b.Left).ToDec() + Eval(b.Right).ToDec(),
                BinaryExpr b when b.Op == SqlBinaryOp.Subtract => Eval(b.Left).ToDec() - Eval(b.Right).ToDec(),
                BinaryExpr b when b.Op == SqlBinaryOp.Multiply => Eval(b.Left).ToDec() * Eval(b.Right).ToDec(),
                BinaryExpr b when b.Op == SqlBinaryOp.Divide => Eval(b.Left).ToDec() / Eval(b.Right).ToDec(),
                BinaryExpr b when b.Op == SqlBinaryOp.And => Eval(b.Left).ToBool() && Eval(b.Right).ToBool(),
                BinaryExpr b when b.Op == SqlBinaryOp.Or => Eval(b.Left).ToBool() || Eval(b.Right).ToBool(),
                BinaryExpr b => CompareBinary(b.Op, Eval(b.Left), Eval(b.Right)),
                FunctionCallExpr fn => EvalFunction(fn),
                CallExpr call => EvalCall(call),
                RawSqlExpr raw => throw new InvalidOperationException($"Expressão não suportada no ON CONFLICT WHERE: {raw.Sql}"),
                _ => throw new InvalidOperationException($"Expressão não suportada no ON CONFLICT WHERE: {expr.GetType().Name}")
            };
        }

        return Eval(where).ToBool();
    }

    private static void EnsureDialectSupportsSequenceFunction(ISqlDialect dialect, string? functionName)
        => SequenceFunctionSupportHelper.EnsureSupported(dialect, functionName);

    private static void TryExecuteTableTrigger(
        QueryExecutionContext context,
        ITableMock table,
        string tableName,
        string? schemaName,
        TableTriggerEvent evt,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?>? newRow)
    {
        var connection = context.Connection;
        var dialect = context.Dialect;
        if (!dialect.SupportsTriggers) return;
        if (connection.IsTemporaryTable(table, tableName, schemaName)) return;
        if (table is TableMock tableMock)
            tableMock.ExecuteTriggers(evt, oldRow, newRow);
    }
}
