namespace DbSqlLikeMem;

internal static class DbDeleteStrategy
{
    private const int ParallelFkScanThreshold = 2048;

    /// <summary>
    /// EN: Implements ExecuteDelete.
    /// PT: Implementa ExecuteDelete.
    /// </summary>
    public static DmlExecutionResult ExecuteDelete(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars = null)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars);
    }

    private static DmlExecutionResult Execute(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars)
    {
        var sw = Stopwatch.StartNew();
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query!.Table!.Name, nameof(query.Table.Name));
        var tableName = query.Table.Name!;
        var dialect = connection.ExecutionDialect;
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName)
            || table == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);
        var rowCountBefore = table.Count;

        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);

        var rowsToDelete = new List<IReadOnlyDictionary<int, object?>>();
        var indexesToDelete = new List<int>();
        var tableMock = table as TableMock;

        foreach (var i in GetCandidateRowIndexes(table, pars, conditions))
        {
            var row = table[i];
            if (TableMock.IsMatchSimple(table, pars, conditions, row))
            {
                // Para o RETURNING, precisamos capturar o estado ANTES da remoção. 
                // Usamos snapshots para garantir que se o objeto row for alterado por triggers, 
                // tenhamos o estado estável da deleção.
                rowsToDelete.Add(TableMock.SnapshotRow(row));
                indexesToDelete.Add(i);
            }
        }

        // 2. FK Validation
        if (tableMock is not null)
        {
            tableMock.Schema.ValidateForeignKeysOnDelete(tableName, tableMock, rowsToDelete);
        }
        else
        {
            ValidateForeignKeys(connection, tableName, table, rowsToDelete, query.Table.DbName);
        }

        // 3. Remover (ordem inversa para não quebrar índices durante loop)
        foreach (var idx in indexesToDelete.OrderByDescending(x => x))
        {
            IReadOnlyDictionary<int, object?>? oldRow = null;
            if (dialect.SupportsTriggers && (table.HasTriggers(TableTriggerEvent.BeforeDelete) || table.HasTriggers(TableTriggerEvent.AfterDelete)))
            {
                oldRow = TableMock.SnapshotRow(table[idx]);
            }

            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeDelete))
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeDelete, oldRow, null);
            
            table.RemoveAt(idx);
            
            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterDelete))
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterDelete, oldRow, null);
        }

        connection.Metrics.Deletes += rowsToDelete.Count;
        sw.Stop();

        var metrics = new SqlPlanRuntimeMetrics(
            InputTables: 1,
            EstimatedRowsRead: rowCountBefore,
            ActualRows: rowsToDelete.Count,
            ElapsedMs: sw.ElapsedMilliseconds);
        if (connection.Db.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatDelete(
                query,
                metrics,
                new SqlPlanMockRuntimeContext(connection.SimulatedLatencyMs, connection.DropProbability, connection.Db.ThreadSafe));
            connection.RegisterExecutionPlan(plan);
        }
        return new DmlExecutionResult 
        { 
            AffectedRows = rowsToDelete.Count, 
            AffectedIndexes = indexesToDelete,
            AffectedRowsData = connection.CaptureAffectedRowSnapshots ? rowsToDelete : new List<IReadOnlyDictionary<int, object?>>()
        };
    }

    private static IEnumerable<int> GetCandidateRowIndexes(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions)
    {
        if (conditions.Count > 0
            && TableMock.TryFindRowByPkConditions(table, pars, conditions, out var rowIndex))
        {
            yield return rowIndex;
            yield break;
        }

        for (int rowIdx = 0; rowIdx < table.Count; rowIdx++)
            yield return rowIdx;
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
        if (!dialect.SupportsTriggers)
            return;

        if (connection.IsTemporaryTable(table, tableName, schemaName))
            return;

        if (table is TableMock tableMock)
            tableMock.ExecuteTriggers(evt, oldRow, newRow);
    }

    private static void ValidateForeignKeys(
        this DbConnectionMockBase connection,
        string tableName,
        ITableMock table,
        List<IReadOnlyDictionary<int, object?>> rowsToDelete,
        string? dbName)
    {
        if (!connection.Db.ThreadSafe || rowsToDelete.Count <= 1)
        {
            foreach (var parentRow in rowsToDelete)
                ValidateForeignKeysForRow(connection, tableName, table, parentRow, dbName);
            return;
        }

        Exception? failure = null;
        var gate = new object();
        Parallel.ForEach(rowsToDelete, (parentRow, state) =>
        {
            try
            {
                ValidateForeignKeysForRow(connection, tableName, table, parentRow, dbName);
            }
            catch (Exception ex)
            {
                lock (gate)
                {
                    failure ??= ex;
                }

                state.Stop();
            }
        });

        if (failure is not null)
            throw failure;
    }

    private static void ValidateForeignKeysForRow(
        DbConnectionMockBase connection,
        string tableName,
        ITableMock table,
        IReadOnlyDictionary<int, object?> parentRow,
        string? dbName)
    {
        foreach (var childTable in connection.ListTables(dbName))
        {
            foreach (var fk in childTable.ForeignKeys.Values.Where(f =>
                f.RefTable.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
            {
                if (HasReferenceByIndex(childTable, fk, parentRow)
                    || HasReferenceByScan(childTable, fk, parentRow, connection.Db.ThreadSafe))
                {
                    throw table.ReferencedRow(tableName);
                }
            }
        }
    }

    private static bool HasReferenceByScan(
        ITableMock childTable,
        Models.ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow,
        bool allowParallel)
    {
        if (allowParallel && childTable.Count >= ParallelFkScanThreshold)
        {
            return childTable
                .AsParallel()
                .Any(childRow => fk.References.All(r =>
                    Equals(childRow[r.col.Index], parentRow[r.refCol.Index])));
        }

        return childTable.Any(childRow => fk.References.All(r =>
            Equals(childRow[r.col.Index], parentRow[r.refCol.Index])));
    }

    private static bool HasReferenceByIndex(
        ITableMock childTable,
        Models.ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        var matchingIndex = childTable.Indexes.Values
            .OrderByDescending(_ => _.KeyCols.Count)
            .FirstOrDefault(ix => fk.References.All(r =>
                ix.KeyCols.Contains(r.col.Name, StringComparer.OrdinalIgnoreCase)));

        if (matchingIndex is null)
            return false;

        var valuesByColumn = fk.References.ToDictionary(
            _ => _.col.Name.NormalizeName(),
            _ => parentRow[_.refCol.Index],
            StringComparer.OrdinalIgnoreCase);

        var key = matchingIndex.BuildIndexKeyFromValues(valuesByColumn);
        return matchingIndex.LookupMutable(key)?.Count > 0;
    }
}
