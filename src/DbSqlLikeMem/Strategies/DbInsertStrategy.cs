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
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars, dialect);
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
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query with { IsReplace = true }, pars, dialect);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query with { IsReplace = true }, pars, dialect);
    }

    private static DmlExecutionResult Execute(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        var sw = Stopwatch.StartNew();
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
            newRows = CreateRowsFromSelect(query, table, connection, pars, dialect);
        }
        else
        {
            // Caso: INSERT INTO ... VALUES ...
            newRows = CreateRowsFromValues(query, table, connection, pars, dialect);
        }

        int insertedCount = 0;
        int updatedCount = 0;
        var tableMock = (TableMock)table;
        var canUseBatchInsert = CanUseBatchInsert(connection, dialect, table, tableName, query.Table.DbName, tableMock);
        var affectedIndexes = new List<int>();

        if (query.IsReplace)
            return ExecuteReplaceCore(connection, query, pars, dialect, table, tableMock, newRows, targetRowCountBefore);

        if (!query.HasOnDuplicateKeyUpdate
            && newRows.Count > 1
            && canUseBatchInsert)
        {
            var beforeCount = table.Count;
            tableMock.AddBatch(newRows);
            insertedCount = newRows.Count;
            for (int i = beforeCount; i < table.Count; i++)
                affectedIndexes.Add(i);
            TrySetLastInsertId(connection, table, table[table.Count - 1]);
        }
        else if (query.HasOnDuplicateKeyUpdate
            && newRows.Count > 1
            && canUseBatchInsert)
        {
            var pendingInsertRows = new List<Dictionary<int, object?>>(newRows.Count);
            HashSet<IndexKey>? pendingPrimaryKeys = tableMock.PrimaryKeyIndexes.Count > 0
                ? new HashSet<IndexKey>()
                : null;
            var pendingUniqueKeys = tableMock.Indexes.Values
                .Where(static index => index.Unique)
                .ToDictionary(
                    static index => index,
                    static _ => new HashSet<IndexKey>());

            foreach (var newRow in newRows)
            {
                if (pendingInsertRows.Count > 0
                    && HasPendingBatchConflict(tableMock, pendingPrimaryKeys, pendingUniqueKeys, newRow))
                {
                    var before = table.Count;
                    FlushPendingInsertBatch(connection, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
                    for (int i = before; i < table.Count; i++) affectedIndexes.Add(i);
                }

                var conflictIdx = tableMock.FindConflictingRowIndex(newRow, out _, out _);
                if (conflictIdx is null)
                {
                    pendingInsertRows.Add(newRow);
                    RegisterPendingBatchKeys(tableMock, pendingPrimaryKeys, pendingUniqueKeys, newRow);
                    continue;
                }
                var beforeFlush = table.Count;
                FlushPendingInsertBatch(connection, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
                for (int i = beforeFlush; i < table.Count; i++) affectedIndexes.Add(i);

                if (query.IsOnConflictDoNothing)
                    continue;
                if (!ShouldApplyOnConflictUpdateWhere(query, table, conflictIdx.Value, newRow, pars, dialect))
                    continue;

                var oldSnapshot = (dialect.SupportsTriggers && (table.HasTriggers(TableTriggerEvent.BeforeUpdate) || table.HasTriggers(TableTriggerEvent.AfterUpdate)))
                    ? TableMock.SnapshotRow(table[conflictIdx.Value])
                    : null;

                var simulatedUpdated = table[conflictIdx.Value].ToDictionary(_ => _.Key, _ => _.Value);
                ApplyOnDuplicateUpdateAstInMemory(
                    table,
                    conflictIdx.Value,
                    newRow,
                    query.OnDupAssignsParsed,
                    pars,
                    dialect,
                    simulatedUpdated);
                tableMock.ValidateForeignKeysOnRow(new System.Collections.ObjectModel.ReadOnlyDictionary<int, object?>(simulatedUpdated));

                if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate))
                    TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, TableMock.SnapshotRow(newRow));

                ApplyOnDuplicateUpdateAst(
                    table,
                    conflictIdx.Value,
                    newRow,
                    query.OnDupAssignsParsed,
                    pars,
                    dialect);

                if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate))
                    TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[conflictIdx.Value]));

                tableMock.UpdateIndexesWithRow(conflictIdx.Value, oldSnapshot, table[conflictIdx.Value]);
                updatedCount++;
                affectedIndexes.Add(conflictIdx.Value);
            }
            var beforeFinalFlush = table.Count;
            FlushPendingInsertBatch(connection, table, tableMock, pendingInsertRows, pendingPrimaryKeys, pendingUniqueKeys, ref insertedCount);
            for (int i = beforeFinalFlush; i < table.Count; i++) affectedIndexes.Add(i);
        }
        else
        {
            foreach (var newRow in newRows)
            {
                if (!query.HasOnDuplicateKeyUpdate)
                {
                    // Inserção normal
                    tableMock.ValidateForeignKeysOnRow(newRow);
                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeInsert))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

                    table.Add(newRow);
                    var insertedRow = table[table.Count - 1];

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterInsert))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));
                    TrySetLastInsertId(connection, table, insertedRow);
                    insertedCount++;
                    affectedIndexes.Add(table.Count - 1);
                    continue;
                }

                // Lógica ON DUPLICATE KEY UPDATE
                var conflictIdx = tableMock.FindConflictingRowIndex(newRow, out _, out _);
                if (conflictIdx is null)
                {
                    // Sem conflito -> Insere
                    tableMock.ValidateForeignKeysOnRow(newRow);
                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeInsert))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

                    table.Add(newRow);
                    var insertedRow = table[table.Count - 1];

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterInsert))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));
                    TrySetLastInsertId(connection, table, insertedRow);
                    insertedCount++;
                    affectedIndexes.Add(table.Count - 1);
                }
                else
                {
                    if (query.IsOnConflictDoNothing)
                        continue;
                    if (!ShouldApplyOnConflictUpdateWhere(query, table, conflictIdx.Value, newRow, pars, dialect))
                        continue;

                    // Conflito -> Update
                    var oldSnapshot = dialect.SupportsTriggers && (table.HasTriggers(TableTriggerEvent.BeforeUpdate) || table.HasTriggers(TableTriggerEvent.AfterUpdate))
                        ? TableMock.SnapshotRow(table[conflictIdx.Value])
                        : null;

                    var simulatedUpdated = table[conflictIdx.Value].ToDictionary(_ => _.Key, _ => _.Value);
                    ApplyOnDuplicateUpdateAstInMemory(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        pars,
                        dialect,
                        simulatedUpdated);
                    tableMock.ValidateForeignKeysOnRow(new System.Collections.ObjectModel.ReadOnlyDictionary<int, object?>(simulatedUpdated));

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, TableMock.SnapshotRow(newRow));

                    ApplyOnDuplicateUpdateAst(
                        table,
                        conflictIdx.Value,
                        newRow,
                        query.OnDupAssignsParsed,
                        pars,
                        dialect);

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate))
                        TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[conflictIdx.Value]));

                    tableMock.UpdateIndexesWithRow(conflictIdx.Value, oldSnapshot, table[conflictIdx.Value]);
                    updatedCount++;
                    affectedIndexes.Add(conflictIdx.Value);
                }
            }
        }

        connection.Metrics.Inserts += insertedCount;
        connection.Metrics.Updates += updatedCount;

        var affected = dialect.GetInsertUpsertAffectedRowCount(insertedCount, updatedCount);
        connection.SetLastFoundRows(affected);
        sw.Stop();

        var affectedRowsData = connection.CaptureAffectedRowSnapshots
            ? affectedIndexes.ConvertAll(idx => TableMock.SnapshotRow(table[idx]))
            : new List<IReadOnlyDictionary<int, object?>>();

        var metrics = new SqlPlanRuntimeMetrics(
            InputTables: query.InsertSelect is null ? 1 : 1 + CountInputTables(query.InsertSelect),
            EstimatedRowsRead: targetRowCountBefore + newRows.Count,
            ActualRows: affected,
            ElapsedMs: sw.ElapsedMilliseconds);
        if (connection.Db.CaptureExecutionPlans)
        {
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
            AffectedRowsData = affectedRowsData
        };
    }

    private static DmlExecutionResult ExecuteReplaceCore(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection? pars,
        ISqlDialect dialect,
        ITableMock table,
        TableMock tableMock,
        List<Dictionary<int, object?>> newRows,
        int targetRowCountBefore)
    {
        var sw = Stopwatch.StartNew();
        var affectedIndexes = new List<int>();
        var affectedRowsData = connection.CaptureAffectedRowSnapshots
            ? new List<IReadOnlyDictionary<int, object?>>()
            : new List<IReadOnlyDictionary<int, object?>>();
        var insertedCount = 0;
        var deletedCount = 0;

        foreach (var newRow in newRows)
        {
            var conflictIndexes = FindReplaceConflictIndexes(tableMock, newRow)
                .OrderByDescending(static idx => idx)
                .ToList();

            if (conflictIndexes.Count > 0)
            {
                var deleteSnapshots = conflictIndexes
                    .Select(idx => TableMock.SnapshotRow(table[idx]))
                    .ToList();
                tableMock.Schema.ValidateForeignKeysOnDelete(query.Table!.Name!, tableMock, deleteSnapshots);

                foreach (var idx in conflictIndexes)
                {
                    IReadOnlyDictionary<int, object?>? oldRow = null;
                    if (dialect.SupportsTriggers && (table.HasTriggers(TableTriggerEvent.BeforeDelete) || table.HasTriggers(TableTriggerEvent.AfterDelete)))
                        oldRow = TableMock.SnapshotRow(table[idx]);

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeDelete))
                        TryExecuteTableTrigger(connection, dialect, table, query.Table.Name!, query.Table.DbName, TableTriggerEvent.BeforeDelete, oldRow, null);

                    table.RemoveAt(idx);

                    if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterDelete))
                        TryExecuteTableTrigger(connection, dialect, table, query.Table.Name!, query.Table.DbName, TableTriggerEvent.AfterDelete, oldRow, null);

                    deletedCount++;
                }
            }

            tableMock.ValidateForeignKeysOnRow(newRow);
            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeInsert))
                TryExecuteTableTrigger(connection, dialect, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, TableMock.SnapshotRow(newRow));

            var beforeInsert = table.Count;
            table.Add(newRow);
            var insertedRow = table[table.Count - 1];

            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterInsert))
                TryExecuteTableTrigger(connection, dialect, table, query.Table!.Name!, query.Table.DbName, TableTriggerEvent.AfterInsert, null, TableMock.SnapshotRow(insertedRow));

            TrySetLastInsertId(connection, table, insertedRow);
            insertedCount++;
            affectedIndexes.Add(beforeInsert);
            affectedRowsData.Add(TableMock.SnapshotRow(insertedRow));
        }

        connection.Metrics.Inserts += insertedCount;
        connection.Metrics.Deletes += deletedCount;

        var affected = deletedCount + insertedCount;
        connection.SetLastFoundRows(affected);
        sw.Stop();

        var metrics = new SqlPlanRuntimeMetrics(
            InputTables: query.InsertSelect is null ? 1 : 1 + CountInputTables(query.InsertSelect),
            EstimatedRowsRead: targetRowCountBefore + newRows.Count,
            ActualRows: affected,
            ElapsedMs: sw.ElapsedMilliseconds);
        if (connection.Db.CaptureExecutionPlans)
        {
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
            AffectedRowsData = affectedRowsData
        };
    }

    private static IReadOnlyList<int> FindReplaceConflictIndexes(
        TableMock table,
        IReadOnlyDictionary<int, object?> newRow)
    {
        var conflicts = new HashSet<int>();

        if (table.PrimaryKeyIndexes.Count > 0)
        {
            for (var rowIndex = 0; rowIndex < table.Count; rowIndex++)
            {
                if (MatchesColumns(table, newRow, table.PrimaryKeyIndexes, table[rowIndex]))
                    conflicts.Add(rowIndex);
            }
        }

        foreach (var index in table.Indexes.Values.Where(static ix => ix.Unique))
        {
            for (var rowIndex = 0; rowIndex < table.Count; rowIndex++)
            {
                if (MatchesColumns(table, newRow, index.KeyCols, table[rowIndex]))
                    conflicts.Add(rowIndex);
            }
        }

        return conflicts.ToList();
    }

    private static bool MatchesColumns(
        TableMock table,
        IReadOnlyDictionary<int, object?> newRow,
        IReadOnlyCollection<int> columnIndexes,
        IReadOnlyDictionary<int, object?> existingRow)
        => columnIndexes.All(colIdx =>
            existingRow.TryGetValue(colIdx, out var existingValue)
            && newRow.TryGetValue(colIdx, out var newValue)
            && Equals(existingValue, newValue));

    private static bool MatchesColumns(
        TableMock table,
        IReadOnlyDictionary<int, object?> newRow,
        IReadOnlyList<string> columnNames,
        IReadOnlyDictionary<int, object?> existingRow)
    {
        foreach (var columnName in columnNames)
        {
            var info = table.GetColumn(columnName);
            if (!existingRow.TryGetValue(info.Index, out var existingValue)
                || !newRow.TryGetValue(info.Index, out var newValue)
                || !Equals(existingValue, newValue))
            {
                return false;
            }
        }

        return true;
    }

    private static void FlushPendingInsertBatch(
        DbConnectionMockBase connection,
        ITableMock table,
        TableMock tableMock,
        List<Dictionary<int, object?>> pendingInsertRows,
        HashSet<IndexKey>? pendingPrimaryKeys,
        Dictionary<IndexDef, HashSet<IndexKey>> pendingUniqueKeys,
        ref int insertedCount)
    {
        if (pendingInsertRows.Count == 0)
            return;

        tableMock.AddBatch(pendingInsertRows);
        insertedCount += pendingInsertRows.Count;
        TrySetLastInsertId(connection, table, pendingInsertRows[^1]);
        pendingInsertRows.Clear();
        pendingPrimaryKeys?.Clear();
        foreach (var keys in pendingUniqueKeys.Values)
            keys.Clear();
    }

    private static bool HasPendingBatchConflict(
        TableMock tableMock,
        HashSet<IndexKey>? pendingPrimaryKeys,
        Dictionary<IndexDef, HashSet<IndexKey>> pendingUniqueKeys,
        IReadOnlyDictionary<int, object?> newRow)
    {
        if (pendingPrimaryKeys is not null)
        {
            var newPkKey = tableMock.BuildPkKey(newRow);
            if (pendingPrimaryKeys.Contains(newPkKey))
                return true;
        }

        foreach (var it in pendingUniqueKeys)
        {
            var newKey = it.Key.BuildIndexKey(newRow);
            if (it.Value.Contains(newKey))
                return true;
        }

        return false;
    }

    private static void RegisterPendingBatchKeys(
        TableMock tableMock,
        HashSet<IndexKey>? pendingPrimaryKeys,
        Dictionary<IndexDef, HashSet<IndexKey>> pendingUniqueKeys,
        IReadOnlyDictionary<int, object?> row)
    {
        pendingPrimaryKeys?.Add(tableMock.BuildPkKey(row));

        foreach (var it in pendingUniqueKeys)
            it.Value.Add(it.Key.BuildIndexKey(row));
    }

    private static bool CanUseBatchInsert(
        DbConnectionMockBase connection,
        ISqlDialect dialect,
        ITableMock table,
        string tableName,
        string? schemaName,
        TableMock tableMock)
    {
        if (tableMock.ForeignKeys.Count > 0)
            return false;

        if (!dialect.SupportsTriggers)
            return true;

        if (connection.IsTemporaryTable(table, tableName, schemaName))
            return true;

        return !tableMock.HasRegisteredTriggers();
    }

    private static void TrySetLastInsertId(DbConnectionMockBase connection, ITableMock table, IReadOnlyDictionary<int, object?> insertedRow)
    {
        var identityColumn = table.Columns.Values.FirstOrDefault(c => c.Identity);
        if (identityColumn is null)
            return;

        if (!insertedRow.TryGetValue(identityColumn.Index, out var value))
            return;

        connection.SetLastInsertId(value);
    }

    private static int CountInputTables(SqlSelectQuery query)
    {
        var count = query.Table is not null ? 1 : 0;
        count += query.Joins.Count;
        return Math.Max(1, count);
    }

    // --- Helpers de Criação de Linhas ---

    private static List<Dictionary<int, object?>> CreateRowsFromValues(
        SqlInsertQuery query,
        ITableMock table,
        DbConnectionMockBase connection,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        var rows = new List<Dictionary<int, object?>>(query.ValuesRaw.Count);
        var colNames = query.Columns; // Lista de colunas do Insert
        ColumnDef[]? explicitTargetColumns = null;
        List<ColumnDef>? orderedTableColumns = null;
        List<ColumnDef>? nonIdentityColumns = null;

        if (colNames.Count > 0)
            explicitTargetColumns = [.. colNames.Select(colName => ResolveInsertColumn(table, colName, dialect))];
        else
        {
            orderedTableColumns = [.. table.Columns.Values.OrderBy(c => c.Index)];
            nonIdentityColumns = [.. orderedTableColumns.Where(c => !c.Identity)];
        }

        for (var rowIndex = 0; rowIndex < query.ValuesRaw.Count; rowIndex++)
        {
            var valueBlock = query.ValuesRaw[rowIndex];
            var parsedExprBlock = rowIndex < query.ValuesExpr.Count
                ? query.ValuesExpr[rowIndex]
                : null;

            // Validação de count
            if (colNames.Count > 0 && colNames.Count != valueBlock.Count)
                throw new InvalidOperationException($"Column count ({colNames.Count}) does not match value count ({valueBlock.Count}).");

            var newRow = new Dictionary<int, object?>(Math.Max(1, valueBlock.Count));

            if (colNames.Count == 0 && valueBlock.Count > 0)
            {
                var targetCols = valueBlock.Count == orderedTableColumns!.Count
                    ? orderedTableColumns
                    : (valueBlock.Count == nonIdentityColumns!.Count ? nonIdentityColumns : orderedTableColumns);

                for (int i = 0; i < valueBlock.Count; i++)
                {
                    if (i >= targetCols.Count) break;
                    var parsedExpr = parsedExprBlock is not null && i < parsedExprBlock.Count
                        ? parsedExprBlock[i]
                        : null;
                    SetColValue(table, connection, pars, targetCols[i], valueBlock[i], parsedExpr, newRow, dialect);
                }
            }
            else if (colNames.Count > 0)
            {
                for (int i = 0; i < explicitTargetColumns!.Length; i++)
                {
                    var parsedExpr = parsedExprBlock is not null && i < parsedExprBlock.Count
                        ? parsedExprBlock[i]
                        : null;
                    SetColValue(table, connection, pars, explicitTargetColumns[i], valueBlock[i], parsedExpr, newRow, dialect);
                }
            }

            rows.Add(newRow);
        }
        return rows;
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

        var firstToken = trimmedPunctuation
            .Split(' ')
            .Select(_ => _.Trim())
            .FirstOrDefault(_ => !string.IsNullOrWhiteSpace(_));
        AddInsertColumnCandidate(candidates, firstToken);

        AppendQuotedInsertColumnCandidates(candidates, normalized, dialect);

        return candidates;
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

        if (!candidates.Any(existing => string.Equals(existing, candidate, StringComparison.Ordinal)))
            candidates.Add(candidate!);
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
        SqlInsertQuery query,
        ITableMock targetTable,
        DbConnectionMockBase connection,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars ?? connection.CreateCommand().Parameters);
        var res = executor.ExecuteSelect(query.InsertSelect!);

        var rows = new List<Dictionary<int, object?>>();
        var colNames = query.Columns;
        ColumnDef[]? explicitTargetColumns = null;
        List<ColumnDef>? orderedTableColumns = null;

        if (colNames.Count > 0 && colNames.Count != res.Columns.Count)
            throw new InvalidOperationException(SqlExceptionMessages.ColumnCountDoesNotMatchSelectList());

        if (colNames.Count > 0)
            explicitTargetColumns = [.. colNames.Select(targetTable.GetColumn)];
        else
            orderedTableColumns = [.. targetTable.Columns.Values.OrderBy(c => c.Index)];

        foreach (var srcRow in res)
        {
            var newRow = new Dictionary<int, object?>(Math.Max(1, res.Columns.Count));

            if (colNames.Count > 0)
            {
                for (int i = 0; i < explicitTargetColumns!.Length; i++)
                {
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[explicitTargetColumns[i].Index] = (val is DBNull) ? null : val;
                }
            }
            else
            {
                for (int i = 0; i < res.Columns.Count; i++)
                {
                    if (i >= orderedTableColumns!.Count) break;
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[orderedTableColumns[i].Index] = (val is DBNull) ? null : val;
                }
            }

            rows.Add(newRow);
        }
        return rows;
    }

    private static void SetColValue(
        ITableMock table,
        DbConnectionMockBase connection,
        DbParameterCollection? pars,
        ColumnDef colDef,
        string rawValue,
        SqlExpr? parsedExpr,
        Dictionary<int, object?> row,
        ISqlDialect dialect)
    {
        table.CurrentColumn = colDef.Name;
        object? resolved;
        if (TryResolveCastAsJsonValue(parsedExpr, pars, out var castJsonValue))
        {
            resolved = castJsonValue;
        }
        else if (TryResolveParsedSpecialValue(table, connection, parsedExpr, pars, dialect, out var parsedValue))
        {
            resolved = parsedValue;
        }
        else
        {
            resolved = TryResolveTemporalValue(rawValue, dialect, out var temporalValue)
                ? temporalValue
                : table.Resolve(rawValue, colDef.DbType, colDef.Nullable, pars, table.Columns);
        }
        table.CurrentColumn = null;

        var val = (resolved is DBNull) ? null : resolved;
        if (val == null && !colDef.Nullable)
            throw table.ColumnCannotBeNull("Idx:" + colDef.Index);

        row[colDef.Index] = val;
    }

    private static bool TryResolveParsedSpecialValue(
        ITableMock table,
        DbConnectionMockBase connection,
        SqlExpr? parsedExpr,
        DbParameterCollection? pars,
        ISqlDialect dialect,
        out object? value)
    {
        value = null;
        if (parsedExpr is null)
            return false;

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p when TryResolveParameterValue(pars, p.Name, out var parameterValue) => parameterValue,
                IdentifierExpr id when SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, id.Name, out var temporalIdentifierValue) => temporalIdentifierValue,
                ColumnExpr c when SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, c.Name, out var temporalColumnValue) => temporalColumnValue,
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
        DbParameterCollection? pars,
        out object? value)
    {
        value = null;
        if (parsedExpr is not CallExpr cast
            || !cast.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || cast.Args.Count < 2
            || !IsJsonCastType(cast.Args[1]))
            return false;

        if (!TryResolveCastOperandValue(cast.Args[0], pars, out var operand))
            return false;

        value = NormalizeJsonCastValue(operand);
        return true;
    }

    private static bool IsJsonCastType(SqlExpr expr)
    {
        if (expr is RawSqlExpr raw)
            return raw.Sql.Trim().Equals("JSON", StringComparison.OrdinalIgnoreCase);

        if (expr is LiteralExpr { Value: string s })
            return s.Trim().Equals("JSON", StringComparison.OrdinalIgnoreCase);

        return false;
    }

    private static bool TryResolveCastOperandValue(
        SqlExpr expr,
        DbParameterCollection? pars,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                value = lit.Value;
                return true;
            case ParameterExpr p:
                return TryResolveParameterValue(pars, p.Name, out value);
            default:
                value = null;
                return false;
        }
    }

    private static bool TryResolveParameterValue(
        DbParameterCollection? pars,
        string parameterToken,
        out object? value)
    {
        value = null;
        if (pars is null)
            return false;

        if (parameterToken == "?")
        {
            if (pars.Count <= 0 || pars[0] is not IDataParameter first)
                return false;

            value = first.Value is DBNull ? null : first.Value;
            return true;
        }

        var normalized = parameterToken.TrimStart('@', ':', '?');
        foreach (IDataParameter p in pars)
        {
            var candidate = (p.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            value = p.Value is DBNull ? null : p.Value;
            return true;
        }

        return false;
    }

    private static object? NormalizeJsonCastValue(object? operand)
    {
        if (operand is null || operand is DBNull)
            return null;

        if (operand is System.Text.Json.JsonDocument || operand is System.Text.Json.JsonElement)
            return operand;

        if (operand is string)
            return operand;

        return System.Text.Json.JsonSerializer.Serialize(operand);
    }

    private static bool TryResolveTemporalValue(string rawValue, ISqlDialect dialect, out object? value)
    {
        var trimmed = rawValue.Trim();
        if (SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, trimmed, out value))
            return true;

        var openParen = trimmed.IndexOf('(');
        var closeParen = trimmed.LastIndexOf(')');
        if (openParen <= 0 || closeParen <= openParen)
            return false;

        var functionName = trimmed[..openParen].Trim();
        var argsRaw = trimmed[(openParen + 1)..closeParen].Trim();
        if (argsRaw.Length > 0)
            return false;

        return SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, functionName, out value);
    }

    private static void ApplyOnDuplicateUpdateAstInMemory(
        ITableMock table,
        int existinIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        IReadOnlyList<SqlAssignment> assigns,
        DbParameterCollection? pars,
        ISqlDialect dialect,
        IDictionary<int, object?> targetRow)
    {
        object? GetParamValue(string rawName)
        {
            if (pars is null) return null;
            var n = rawName.Trim();
            if (n.Length > 0 && (n[0] == '@' || n[0] == ':' || n[0] == '?')) n = n[1..];

            foreach (DbParameter p in pars)
            {
                var pn = p.ParameterName?.TrimStart('@', ':', '?') ?? "";
                if (string.Equals(pn, n, StringComparison.OrdinalIgnoreCase))
                    return p.Value is DBNull ? null : p.Value;
            }
            return null;
        }

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
                    DbType.Decimal => Convert.ToDecimal(value),
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
                ParameterExpr p => GetParamValue(p.Name),
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, id.Name, out var temporalIdentifierValue)
                        ? temporalIdentifierValue
                        : GetExistingColumnValue(id.Name.Contains('.') ? id.Name.Split('.').Last() : id.Name),
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
            var nullInputReturnsNull = dialect.ConcatReturnsNullOnNullInput;
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
            EnsureDialectSupportsSequenceFunction(dialect, fn.Name);
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
            EnsureDialectSupportsSequenceFunction(dialect, call.Name);
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
            var expr = assignment.ValueExpr ?? SqlExpressionParser.ParseScalar(assignment.ValueRaw, dialect);
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
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        object? GetParamValue(string rawName)
        {
            if (pars is null) return null;
            var n = rawName.Trim();
            if (n.Length > 0 && (n[0] == '@' || n[0] == ':' || n[0] == '?')) n = n[1..];

            foreach (DbParameter p in pars)
            {
                var pn = p.ParameterName?.TrimStart('@', ':', '?') ?? "";
                if (string.Equals(pn, n, StringComparison.OrdinalIgnoreCase))
                    return p.Value is DBNull ? null : p.Value;
            }
            return null;
        }

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
                    DbType.Decimal => Convert.ToDecimal(value),
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
                ParameterExpr p => GetParamValue(p.Name),
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : GetExistingColumnValue(id.Name.Contains('.') ? id.Name.Split('.').Last() : id.Name),
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
            var nullInputReturnsNull = dialect.ConcatReturnsNullOnNullInput;
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

            var parts = rawName.Split('.').Select(_ => _.Trim()).Take(2).ToArray();
            if (parts.Length == 2 && string.Equals(parts[0], "excluded", StringComparison.OrdinalIgnoreCase))
            {
                value = GetInsertedColumnValue(parts[1]);
                return true;
            }

            return false;
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            var name = fn.Name;
            EnsureDialectSupportsSequenceFunction(dialect, name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, name, fn.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (fn.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, name, out var temporalValue))
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
            EnsureDialectSupportsSequenceFunction(dialect, name);
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

            if (call.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, name, out var temporalValue))
                return temporalValue;

            throw new InvalidOperationException($"CALL não suportado no ON DUPLICATE: {call.Name}");
        }

        foreach (var assignment in assigns)
        {
            var colInfo = table.GetColumn(assignment.Column);
            if (colInfo.GetGenValue != null) continue;

            var ast = assignment.ValueExpr ?? SqlExpressionParser.ParseScalar(assignment.ValueRaw, dialect);
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
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        var where = query.OnConflictUpdateWhereExpr;
        if (where is null)
            return true;

        object? GetParamValue(string rawName)
        {
            if (pars is null) return null;
            var n = rawName.Trim();
            if (n.Length > 0 && (n[0] == '@' || n[0] == ':' || n[0] == '?')) n = n[1..];
            foreach (DbParameter p in pars)
            {
                var pn = p.ParameterName?.TrimStart('@', ':', '?') ?? "";
                if (string.Equals(pn, n, StringComparison.OrdinalIgnoreCase))
                    return p.Value is DBNull ? null : p.Value;
            }
            return null;
        }

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
            var parts = rawName.Split('.').Select(_ => _.Trim()).Take(2).ToArray();
            if (parts.Length != 2) return false;
            if (string.Equals(parts[0], "excluded", StringComparison.OrdinalIgnoreCase)
                || string.Equals(parts[0], "values", StringComparison.OrdinalIgnoreCase))
            {
                value = GetInsertedColumnValue(parts[1]);
                return true;
            }
            return false;
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            EnsureDialectSupportsSequenceFunction(dialect, fn.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, fn.Name, fn.Args, Eval, out var sequenceValue))
                return sequenceValue;

            if (fn.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase) && fn.Args.Count == 1)
            {
                var col = fn.Args[0] switch { IdentifierExpr id => id.Name, ColumnExpr c => c.Name, _ => null };
                if (!string.IsNullOrWhiteSpace(col)) return GetInsertedColumnValue(col!);
            }
            if (fn.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, fn.Name, out var temporal))
                return temporal;
            throw new InvalidOperationException($"Função não suportada no ON CONFLICT WHERE: {fn.Name}()");
        }

        object? EvalCall(CallExpr call)
        {
            EnsureDialectSupportsSequenceFunction(dialect, call.Name);
            if (SqlSequenceEvaluator.TryEvaluateCall(table, call.Name, call.Args, Eval, out var sequenceValue))
                return sequenceValue;
            if (call.Name.Equals(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase) && call.Args.Count == 1)
            {
                var col = call.Args[0] switch { IdentifierExpr id => id.Name, ColumnExpr c => c.Name, _ => null };
                if (string.IsNullOrWhiteSpace(col)) throw new InvalidOperationException("VALUES() espera 1 coluna");
                return GetInsertedColumnValue(col!);
            }
            if (call.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, call.Name, out var temporal))
                return temporal;
            throw new InvalidOperationException($"CALL não suportado no ON CONFLICT WHERE: {call.Name}");
        }

        bool CompareBinary(SqlBinaryOp op, object? left, object? right)
        {
            return op switch
            {
                SqlBinaryOp.Eq => left.EqualsSql(right, dialect),
                SqlBinaryOp.Neq => !left.EqualsSql(right, dialect),
                SqlBinaryOp.Greater => left is not null && right is not null && left.Compare(right, dialect) > 0,
                SqlBinaryOp.GreaterOrEqual => left is not null && right is not null && left.Compare(right, dialect) >= 0,
                SqlBinaryOp.Less => left is not null && right is not null && left.Compare(right, dialect) < 0,
                SqlBinaryOp.LessOrEqual => left is not null && right is not null && left.Compare(right, dialect) <= 0,
                _ => throw new InvalidOperationException($"Operador não suportado no ON CONFLICT WHERE: {op}")
            };
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => GetParamValue(p.Name),
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, id.Name, out var temporalIdentifierValue)
                        ? temporalIdentifierValue
                        : GetExistingColumnValue(id.Name.Contains('.') ? id.Name.Split('.').Last() : id.Name),
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
    {
        if (string.IsNullOrWhiteSpace(functionName)) return;
        if (functionName!.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase) && !dialect.SupportsNextValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(dialect, "NEXT VALUE FOR");
        if (functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase) && !dialect.SupportsPreviousValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(dialect, "PREVIOUS VALUE FOR");
        if ((functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase)) && !SupportsSequenceFunctionCall(dialect, functionName))
            throw SqlUnsupported.ForDialect(dialect, functionName.ToUpperInvariant());
    }

    private static bool SupportsSequenceFunctionCall(ISqlDialect dialect, string functionName)
    {
        if (dialect.SupportsSequenceFunctionCall(functionName)) return true;
        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
            return functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase);
        if (dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
            return (functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)) && dialect.SupportsSequenceDotValueExpression(functionName);
        return false;
    }

    private static void TryExecuteTableTrigger(
        DbConnectionMockBase connection,
        ISqlDialect dialect,
        ITableMock table,
        string tableName,
        string? schemaName,
        TableTriggerEvent evt,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?>? newRow)
    {
        if (!dialect.SupportsTriggers) return;
        if (connection.IsTemporaryTable(table, tableName, schemaName)) return;
        if (table is TableMock tableMock)
            tableMock.ExecuteTriggers(evt, oldRow, newRow);
    }
}
