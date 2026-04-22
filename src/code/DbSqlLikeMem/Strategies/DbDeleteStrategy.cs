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
        DbParameterCollection pars)
        => connection.ExecuteDelete(query, QueryExecutionContext.FromConnection(connection, pars));

    /// <summary>
    /// EN: Implements ExecuteDelete using a pre-built execution context.
    /// PT: Implementa ExecuteDelete usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteDelete(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        QueryExecutionContext context)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(context, query);
        lock (connection.Db.SyncRoot)
            return Execute(context, query);
    }

    private static DmlExecutionResult Execute(
        QueryExecutionContext context,
        SqlDeleteQuery query)
    {
        var connection = context.Connection;
        var pars = context.DbParameters;
        var capturePlans = context.CaptureExecutionPlans;
        var sw = capturePlans ? Stopwatch.StartNew() : null;
        var metricsEnabled = context.MetricsEnabled;
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query!.Table!.Name, nameof(query.Table.Name));
        var tableName = query.Table.Name!;
        var dialect = context.Dialect;
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName)
            || table == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);
        var rowCountBefore = table.Count;

        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);
        var whereHasPositionalParameters = HasPositionalParameters(conditions);

        var rowsToDelete = new List<IReadOnlyDictionary<int, object?>>(4);
        List<IReadOnlyDictionary<int, object?>>? affectedRowsData = connection.CaptureAffectedRowSnapshots
            ? new List<IReadOnlyDictionary<int, object?>>(4)
            : null;
        var indexesToDelete = new List<int>(4);
        var tableMock = table as TableMock;
        var whereContextBase = context.Fork();
        var supportsTriggers = dialect.SupportsTriggers;
        var hasBeforeDeleteTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeDelete);
        var hasAfterDeleteTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterDelete);

        foreach (var i in GetCandidateRowIndexes(table, whereContextBase, pars, conditions, whereHasPositionalParameters))
        {
            var row = table[i];
            var rowContext = whereHasPositionalParameters ? whereContextBase.Fork() : whereContextBase;
            if (TableMock.IsMatchSimple(table, rowContext, pars, conditions, row))
            {
                // Mantemos a linha original para validação de FK antes da remoção.
                // Snapshot só é criado quando o caller pede o retorno das linhas afetadas.
                rowsToDelete.Add(row);
                if (affectedRowsData is not null)
                    affectedRowsData.Add(TableMock.SnapshotRow(row));
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

        // 3. Remover (ordem inversa para nao quebrar indices durante loop)
        for (var deletePos = indexesToDelete.Count - 1; deletePos >= 0; deletePos--)
        {
            var idx = indexesToDelete[deletePos];
            IReadOnlyDictionary<int, object?>? oldRow = null;
            if (hasBeforeDeleteTrigger || hasAfterDeleteTrigger)
            {
                oldRow = TableMock.SnapshotRow(table[idx]);
            }

            if (hasBeforeDeleteTrigger)
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeDelete, oldRow, null);

            table.RemoveAt(idx);

            if (hasAfterDeleteTrigger)
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterDelete, oldRow, null);
        }

        if (metricsEnabled)
            connection.Metrics.Deletes += rowsToDelete.Count;

        if (capturePlans)
        {
            sw!.Stop();
            var metrics = new SqlPlanRuntimeMetrics(
                InputTables: 1,
                EstimatedRowsRead: rowCountBefore,
                ActualRows: rowsToDelete.Count,
                ElapsedMs: sw.ElapsedMilliseconds);
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
            AffectedRowsData = affectedRowsData ?? []
        };
    }
    private static IEnumerable<int> GetCandidateRowIndexes(
        ITableMock table,
        QueryExecutionContext context,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        bool hasPositionalParameters)
    {
        if (conditions.Count > 0
            && TableMock.TryFindRowByPkConditions(table, hasPositionalParameters ? context.Fork() : context, pars, conditions, out var rowIndex))
        {
            yield return rowIndex;
            yield break;
        }

        for (int rowIdx = 0; rowIdx < table.Count; rowIdx++)
            yield return rowIdx;
    }

    private static bool HasPositionalParameters(List<(string C, string Op, string V)> conditions)
    {
        for (var i = 0; i < conditions.Count; i++)
        {
            if (conditions[i].V.IndexOf('?') >= 0)
                return true;
        }

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
            if (childTable.ForeignKeys.Count == 0)
                continue;

            foreach (var fk in childTable.ForeignKeys.Values)
            {
                if (!fk.RefTable.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    continue;

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
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow,
        bool allowParallel)
    {
        if (allowParallel && childTable.Count >= ParallelFkScanThreshold)
        {
            var found = 0;
            Parallel.For(0, childTable.Count, (childIndex, state) =>
            {
                if (Volatile.Read(ref found) != 0)
                {
                    state.Stop();
                    return;
                }

                var childRow = childTable[childIndex];
                foreach (var reference in fk.References)
                {
                    if (!Equals(childRow[reference.col.Index], parentRow[reference.refCol.Index]))
                        return;
                }

                Interlocked.Exchange(ref found, 1);
                state.Stop();
            });

            return Volatile.Read(ref found) != 0;
        }

        foreach (var childRow in childTable)
        {
            var matches = true;
            foreach (var reference in fk.References)
            {
                if (!Equals(childRow[reference.col.Index], parentRow[reference.refCol.Index]))
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

    private static bool HasReferenceByIndex(
        ITableMock childTable,
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        if (!fk.TryGetChildLookupPlan(out var lookupPlan))
            return false;

        var key = lookupPlan.BuildKey(parentRow);
        return lookupPlan.Index.LookupMutable(key)?.Count > 0;
    }
}
