namespace DbSqlLikeMem;

internal static class DbSelectIntoAndInsertSelectStrategies
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteCreateView(
        this DbConnectionMockBase connection,
        SqlCreateViewQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        if (!connection.Db.ThreadSafe)
            return ExecuteCreateViewImpl(connection, query);
        lock (connection.Db.SyncRoot)
            return ExecuteCreateViewImpl(connection, query);
    }

    private static int ExecuteCreateViewImpl(
        DbConnectionMockBase connection,
        SqlCreateViewQuery query)
    {
        connection.AddView(query);
        return 0;
    }

    public static int ExecuteDropView(
        this DbConnectionMockBase connection,
        SqlDropViewQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        if (!connection.Db.ThreadSafe)
            return ExecuteDropViewImpl(connection, query);
        lock (connection.Db.SyncRoot)
            return ExecuteDropViewImpl(connection, query);
    }

    private static int ExecuteDropViewImpl(
        DbConnectionMockBase connection,
        SqlDropViewQuery query)
    {
        var viewName = query.Table?.Name;
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        connection.DropView(viewName, query.IfExists, query.Table?.DbName);
        return 0;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteCreateTableAsSelect(
        this DbConnectionMockBase connection,
        string sql,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteCreateTableAsSelectImpl(connection, sql, pars, dialect);
        lock (connection.Db.SyncRoot)
        {
            return ExecuteCreateTableAsSelectImpl(connection, sql, pars, dialect);
        }
    }

    private static int ExecuteCreateTableAsSelectImpl(
        this DbConnectionMockBase connection,
        string sql,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // CREATE TABLE name AS SELECT ...
        var m = Regex.Match(sql, @"^CREATE\s+TABLE\s+`?(?<name>[A-Za-z0-9_]+)`?\s+AS\s+(?<select>(SELECT|WITH)\s+.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            throw new InvalidOperationException("Invalid CREATE TABLE ... AS SELECT statement.");

        var tableName = m.Groups["name"].Value.NormalizeName();
        var selectSql = m.Groups["select"].Value;

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var q = SqlQueryParser.Parse(selectSql, dialect);
        var res = executor.ExecuteSelect((SqlSelectQuery)q);

        var newTable = connection.AddTable(tableName);
        // map columns
        for (int i = 0; i < res.Columns.Count; i++)
        {
            var colName = res.Columns[i].ColumnName;
            var dbType = InferDbType(res, i);
            newTable.Columns[colName] = new ColumnDef(i, dbType, nullable: true);
        }

        foreach (var row in res)
        {
            var d = new Dictionary<int, object?>();
            for (int i = 0; i < res.Columns.Count; i++)
                d[i] = row.TryGetValue(i, out var v) ? v : null;
            newTable.Add(d);
        }

        return 0;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteCreateTemporaryTableAsSelect(
        this DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteCreateTemporaryTableAsSelectImpl(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
        {
            return ExecuteCreateTemporaryTableAsSelectImpl(connection, query, pars, dialect);
        }
    }

    private static int ExecuteCreateTemporaryTableAsSelectImpl(
        this DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var tableName = query.Table?.Name?.NormalizeName();
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var schemaName = query.Table?.DbName;
        var tempScope = query.Scope;
        if (tempScope == TemporaryTableScope.Global)
        {
            if (connection.TryGetGlobalTemporaryTable(tableName, out _, schemaName))
            {
                if (query.IfNotExists) return 0;
                throw new InvalidOperationException($"Table '{tableName}' already exists.");
            }
        }
        else if (connection.TryGetTemporaryTable(tableName, out _, schemaName))
        {
            if (query.IfNotExists) return 0;
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var res = executor.ExecuteSelect(query.AsSelect);

        var newTable = tempScope == TemporaryTableScope.Global
            ? connection.Db.AddGlobalTemporaryTable(tableName, schemaName: schemaName)
            : connection.AddTemporaryTable(tableName, schemaName: schemaName);

        // column names: prefer explicit list if provided; else use select result columns
        var names = (query.ColumnNames is { Count: > 0 })
            ? query.ColumnNames
            : res.Columns.Select(c => c.ColumnName).ToList();

        for (int i = 0; i < names.Count; i++)
        {
            var colName = names[i];
            var dbType = (i < res.Columns.Count) ? InferDbType(res, i) : DbType.String;
            newTable.Columns[colName] = new ColumnDef(i, dbType, nullable: true);
        }

        foreach (var row in res)
        {
            var d = new Dictionary<int, object?>();
            for (int i = 0; i < names.Count; i++)
                d[i] = row.TryGetValue(i, out var v) ? v : null;
            newTable.Add(d);
        }

        return 0;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteInsertSmart(
            this DbConnectionMockBase connection,
            SqlInsertQuery query,
            DbParameterCollection pars,
            ISqlDialect dialect)
    {
        // Preserve existing behavior for VALUES inserts.
        // If it is INSERT ... SELECT ..., execute select and insert rows.
        if (query.InsertSelect != null)
            return connection.ExecuteInsertSelect(query, pars, dialect);

        // fall back to original extension in MySqlInsertStrategy
        return connection.ExecuteInsert(query, pars, dialect);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteInsertSelect(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteInsertSelectImpl(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
        {
            return ExecuteInsertSelectImpl(connection, query, pars, dialect);
        }
    }

    private static int ExecuteInsertSelectImpl(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // INSERT INTO t (c1,c2) SELECT ...
        var m = Regex.Match(query.RawSql,
            @"^INSERT\s+INTO\s+`?(?<table>[A-Za-z0-9_]+)`?\s*\((?<cols>[^)]*)\)\s*(?<select>(SELECT|WITH)\s+.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            throw new InvalidOperationException("Invalid INSERT ... SELECT statement.");

        var tableName = m.Groups["table"].Value.NormalizeName();
        if (!connection.TryGetTable(tableName, out var target)
            || target == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        var cols = m.Groups["cols"].Value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(c => c.Replace("`", string.Empty, StringComparison.Ordinal).Trim())
            .ToList();

        var selectSql = m.Groups["select"].Value;
        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var q = SqlQueryParser.Parse(selectSql, dialect);
        var res = executor.ExecuteSelect((SqlSelectQuery)q);

        if (cols.Count != res.Columns.Count)
            throw new InvalidOperationException("Column count does not match SELECT list.");

        int inserted = 0;
        foreach (var row in res)
        {
            var newRow = new Dictionary<int, object?>();
            for (int i = 0; i < cols.Count; i++)
            {
                var colName = cols[i];
                var info = target.GetColumn(colName);
                if (info.GetGenValue is not null) continue;
                var val = row.TryGetValue(i, out var v) ? v : null;
                if (val is DBNull) val = null;
                if (val == null && !info.Nullable)
                    throw target.ColumnCannotBeNull(colName);
                newRow[info.Index] = val;
            }

            // defaults and identity handled like regular insert
            // reuse internal helper by calling VALUES insert strategy would require SQL building;
            // do minimal default fill here.
            foreach (var (k, col) in target.Columns)
            {
                if (newRow.ContainsKey(col.Index)) continue;
                if (col.Identity)
                    newRow[col.Index] = target.NextIdentity++;
                else if (col.DefaultValue is not null)
                    newRow[col.Index] = col.DefaultValue;
                else if (!col.Nullable)
                    throw target.ColumnCannotBeNull(k);
            }

            target.Add(newRow);
            target.UpdateIndexesWithRow(target.Count - 1);
            connection.Metrics.Inserts++;
            inserted++;
        }
        return inserted;
    }

    private static DbType InferDbType(TableResultMock res, int colIndex)
    {
        foreach (var row in res)
        {
            if (!row.TryGetValue(colIndex, out var v)
                || v is null
                || v is DBNull) continue;
            return v switch
            {
                int or long or short or byte => DbType.Int32,
                decimal => DbType.Decimal,
                double or float => DbType.Double,
                bool => DbType.Boolean,
                Guid => DbType.Guid,
                DateTime => DbType.DateTime,
                string => DbType.String,
                _ => DbType.Object
            };
        }
        return DbType.Object;
    }
}
