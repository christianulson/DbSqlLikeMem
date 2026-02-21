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


    /// <summary>
    /// Auto-generated summary.
    /// </summary>
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
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(viewName, nameof(viewName));
        connection.DropView(viewName!, query.IfExists, query.Table?.DbName);
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
        if (m.Success)
        {
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
                AddInferredColumn(newTable, colName, dbType);
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

        // CREATE TABLE name (id INT, name VARCHAR(100), ...)
        var createTableMatch = Regex.Match(
            sql,
            @"^CREATE\s+TABLE\s+`?(?<name>[A-Za-z0-9_]+)`?\s*\((?<columns>.*)\)\s*;?$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!createTableMatch.Success)
            throw new InvalidOperationException("Invalid CREATE TABLE statement.");

        var table = connection.AddTable(createTableMatch.Groups["name"].Value.NormalizeName());
        var primaryKeyColumns = new List<string>();
        foreach (var columnSql in SplitColumnDefinitions(createTableMatch.Groups["columns"].Value))
        {
            var tablePrimaryKeyColumns = ParsePrimaryKeyConstraint(columnSql);
            if (tablePrimaryKeyColumns.Count > 0)
            {
                primaryKeyColumns.AddRange(tablePrimaryKeyColumns);
                continue;
            }

            var col = ParseColumnDefinition(columnSql);
            if (col is null)
                continue;
            table.AddColumn(col.Value.Name, col.Value.Type, nullable: col.Value.Nullable);
            if (col.Value.PrimaryKey)
                primaryKeyColumns.Add(col.Value.Name);
        }

        if (primaryKeyColumns.Count > 0)
            table.AddPrimaryKeyIndexes([.. primaryKeyColumns.Distinct(StringComparer.OrdinalIgnoreCase)]);

        return 0;
    }

    private static IEnumerable<string> SplitColumnDefinitions(string columnsSql)
    {
        var start = 0;
        var depth = 0;
        for (var i = 0; i < columnsSql.Length; i++)
        {
            var c = columnsSql[i];
            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            else if (c == ',' && depth == 0)
            {
                var slice = columnsSql[start..i].Trim();
                if (!string.IsNullOrWhiteSpace(slice))
                    yield return slice;
                start = i + 1;
            }
        }

        var last = columnsSql[start..].Trim();
        if (!string.IsNullOrWhiteSpace(last))
            yield return last;
    }

    private static (string Name, DbType Type, bool Nullable, bool PrimaryKey)? ParseColumnDefinition(string columnSql)
    {
        var m = Regex.Match(
            columnSql,
            @"^`?(?<name>[A-Za-z0-9_]+)`?\s+(?<type>[A-Za-z0-9_]+)(\s*\([^)]*\))?(?<rest>.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            return null;

        var rest = m.Groups["rest"].Value;
        if (Regex.IsMatch(rest, @"\b(CONSTRAINT|UNIQUE\s*\(|FOREIGN\s+KEY|CHECK)\b", RegexOptions.IgnoreCase))
            return null;

        var name = m.Groups["name"].Value;
        var type = ParseDbTypeFromSqlType(m.Groups["type"].Value);
        var nullable = !Regex.IsMatch(rest, @"\bNOT\s+NULL\b", RegexOptions.IgnoreCase);
        var primaryKey = Regex.IsMatch(rest, @"\bPRIMARY\s+KEY\b", RegexOptions.IgnoreCase);
        return (name, type, nullable, primaryKey);
    }

    private static IReadOnlyList<string> ParsePrimaryKeyConstraint(string columnSql)
    {
        var m = Regex.Match(
            columnSql,
            @"^(CONSTRAINT\s+`?[A-Za-z0-9_]+`?\s+)?PRIMARY\s+KEY\s*\((?<cols>[^)]*)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!m.Success)
            return [];

        var cols = m.Groups["cols"].Value
            .Split(',')
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(static name => name.NormalizeName())
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .ToList();

        return cols;
    }

    private static DbType ParseDbTypeFromSqlType(string sqlType)
    {
        return sqlType.Trim().NormalizeName() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };
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
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var schemaName = query.Table?.DbName;
        var tempScope = query.Scope;
        if (tempScope == TemporaryTableScope.Global)
        {
            if (connection.TryGetGlobalTemporaryTable(tableName!, out _, schemaName))
            {
                if (query.IfNotExists) return 0;
                throw new InvalidOperationException($"Table '{tableName}' already exists.");
            }
        }
        else if (connection.TryGetTemporaryTable(tableName!, out _, schemaName))
        {
            if (query.IfNotExists) return 0;
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var res = executor.ExecuteSelect(query.AsSelect);

        var newTable = tempScope == TemporaryTableScope.Global
            ? connection.Db.AddGlobalTemporaryTable(tableName!, schemaName: schemaName)
            : connection.AddTemporaryTable(tableName!, schemaName: schemaName);

        // column names: prefer explicit list if provided; else use select result columns
        var names = (query.ColumnNames is { Count: > 0 })
            ? query.ColumnNames
            : [.. res.Columns.Select(c => c.ColumnName)];

        for (int i = 0; i < names.Count; i++)
        {
            var colName = names[i];
            var dbType = (i < res.Columns.Count) ? InferDbType(res, i) : DbType.String;
            AddInferredColumn(newTable, colName, dbType);
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

        var cols = m.Groups["cols"].Value.Split(',')
            .Select(c => c.Replace("`", string.Empty).Trim())
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
            foreach (var it in target.Columns)
            {
                var col = it.Value;
                if (newRow.ContainsKey(col.Index)) continue;
                if (col.Identity)
                    newRow[col.Index] = target.NextIdentity++;
                else if (col.DefaultValue is not null)
                    newRow[col.Index] = col.DefaultValue;
                else if (!col.Nullable)
                    throw target.ColumnCannotBeNull(it.Key);
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

    private static void AddInferredColumn(ITableMock table, string columnName, DbType dbType)
    {
        if (dbType == DbType.String)
        {
            table.AddColumn(columnName, dbType, nullable: true, size: 255);
            return;
        }

        if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
        {
            table.AddColumn(columnName, dbType, nullable: true, decimalPlaces: 2);
            return;
        }

        table.AddColumn(columnName, dbType, nullable: true);
    }
}
