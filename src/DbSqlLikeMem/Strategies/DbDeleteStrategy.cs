namespace DbSqlLikeMem;

internal static class DbDeleteStrategy
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteDelete(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars = null)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars);
    }

    private static int Execute(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query!.Table!.Name, nameof(query.Table.Name));
        var tableName = query.Table.Name!;
        var dialect = connection.Db.Dialect;
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName)
            || table == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // 1. Filtrar linhas
        // Usa a mesma lógica simplificada de WHERE do UpdateStrategy.
        // Fallback: se WhereRaw não foi preenchido, extraímos do RawSql.
        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);

        var rowsToDelete = new List<IReadOnlyDictionary<int, object?>>();
        var indexesToDelete = new List<int>();

        for (int i = 0; i < table.Count; i++)
        {
            var row = table[i];
            if (TableMock.IsMatchSimple(table, pars, conditions, row))
            {
                rowsToDelete.Add(row);
                indexesToDelete.Add(i);
            }
        }

        // 2. FK Validation
        if (table is TableMock tableMock)
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
            var oldRow = SnapshotRow(table[idx]);
            TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeDelete, oldRow, null);
            table.RemoveAt(idx);
            TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterDelete, oldRow, null);
        }

        // Rebuild índices (necessário pois posições mudaram)
        table.RebuildAllIndexes();

        connection.Metrics.Deletes += rowsToDelete.Count;
        return rowsToDelete.Count;
    }

    private static IReadOnlyDictionary<int, object?> SnapshotRow(IReadOnlyDictionary<int, object?> row)
        => row.ToDictionary(_ => _.Key, _ => _.Value);

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
        // Verifica se alguma tabela filha referencia as linhas deletadas
        foreach (var parentRow in rowsToDelete)
        {
            foreach (var childTable in connection.ListTables(dbName))
            {
                foreach (var kvp in childTable.ForeignKeys
                    .Where(f => f.Value.RefTable.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                {
                    if (kvp.Value.References.All(_ =>
                    {
                        var keyVal = parentRow[_.refCol.Index];
                        return childTable.Any(childRow => Equals(childRow[_.col.Index], keyVal));
                    }))
                    {
                        throw table.ReferencedRow(tableName);
                    }
                }
            }
        }
    }

}
