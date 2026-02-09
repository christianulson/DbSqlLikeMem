namespace DbSqlLikeMem;

internal static class DbDeleteStrategy
{
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
        ArgumentNullException.ThrowIfNull(query.Table);
        ArgumentException.ThrowIfNullOrWhiteSpace(query.Table.Name);
        var tableName = query.Table.Name;
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
            table.RemoveAt(idx);
        }

        // Rebuild índices (necessário pois posições mudaram)
        table.RebuildAllIndexes();

        connection.Metrics.Deletes += rowsToDelete.Count;
        return rowsToDelete.Count;
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
                foreach (var (Col, _, RefCol) in childTable.ForeignKeys.Where(f => f.RefTable.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                {
                    var parentInfo = table.GetColumn(RefCol);
                    var childInfo = childTable.GetColumn(Col);

                    var keyVal = parentRow[parentInfo.Index];

                    // Se encontrar qualquer linha na filha com esse valor de FK
                    if (childTable.Any(childRow => Equals(childRow[childInfo.Index], keyVal)))
                    {
                        throw table.ReferencedRow(tableName);
                    }
                }
            }
        }
    }

}
