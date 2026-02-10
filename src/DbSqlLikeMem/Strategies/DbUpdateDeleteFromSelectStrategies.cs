namespace DbSqlLikeMem;

internal static class DbUpdateDeleteFromSelectStrategies
{
    private static readonly Regex _regexDelete = new Regex(
        @"^DELETE\s+(?<a>[A-Za-z0-9_]+)\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a2>[A-Za-z0-9_]+)\s+JOIN\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+ON\s+(?<on>[\s\S]*?)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex _regexOnSql = new Regex(@"^(?<l>[A-Za-z0-9_]+)\.(?<lc>[A-Za-z0-9_`]+)\s*=\s*(?<r>[A-Za-z0-9_]+)\.(?<rc>[A-Za-z0-9_`]+)$", RegexOptions.IgnoreCase);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteUpdateSmart(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect UPDATE ... JOIN (SELECT ...) alias ON ... SET ... [WHERE ...]
        if (query.UpdateFromSelect != null)
            return connection.ExecuteUpdateFromSelect(query, pars, dialect);
        return connection.ExecuteUpdate(query, pars);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteDeleteSmart(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect DELETE a FROM t a JOIN (SELECT ...) s ON ...
        if (query.DeleteFromSelect != null)
            return connection.ExecuteDeleteFromSelect(query, pars, dialect);
        return connection.ExecuteDelete(query, pars);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteUpdateFromSelect(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteUpdateFromSelectImpl(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
        {
            return ExecuteUpdateFromSelectImpl(connection, query, pars, dialect);
        }
    }

    private static int ExecuteUpdateFromSelectImpl(
        DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Minimal grammar for unit tests:
        // UPDATE <table> <a> JOIN (<select>) <s> ON <s>.<k> = <a>.<k> SET <a>.<col> = <s>.<col> [WHERE <a>.<col>=...]
        var m = Regex.Match(query.RawSql,
            @"^UPDATE\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a>[A-Za-z0-9_]+)\s+JOIN\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+ON\s+(?<on>[\s\S]*?)\s+SET\s+(?<set>[\s\S]*?)(\s+WHERE\s+(?<where>[\s\S]*))?;?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            throw new InvalidOperationException("Invalid UPDATE ... JOIN (SELECT ...) statement.");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = m.Groups["a"].Value;
        var subSql = m.Groups["sub"].Value;
        var sAlias = m.Groups["s"].Value;
        var onSql = m.Groups["on"].Value.Trim();
        var setSql = m.Groups["set"].Value.Trim();
        var whereSql = m.Groups["where"].Success ? m.Groups["where"].Value.Trim() : null;

        if (!connection.TryGetTable(tableName, out var target, query.Table?.DbName) || target == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // Parse ON: s.k = a.k  OR a.k = s.k
        var onM = Regex.Match(onSql,
            @"^(?<l>[A-Za-z0-9_]+)\.(?<lc>[A-Za-z0-9_`]+)\s*=\s*(?<r>[A-Za-z0-9_]+)\.(?<rc>[A-Za-z0-9_`]+)$",
            RegexOptions.IgnoreCase);
        if (!onM.Success)
            throw new InvalidOperationException("Only simple equality ON is supported in UPDATE FROM SELECT.");

        var leftAlias = onM.Groups["l"].Value;
        var leftCol = onM.Groups["lc"].Value.Trim('`');
        var rightAlias = onM.Groups["r"].Value;
        var rightCol = onM.Groups["rc"].Value.Trim('`');

        string targetJoinCol;
        string subJoinCol;
        if (string.Equals(leftAlias, aAlias, StringComparison.OrdinalIgnoreCase) && string.Equals(rightAlias, sAlias, StringComparison.OrdinalIgnoreCase))
        {
            targetJoinCol = leftCol;
            subJoinCol = rightCol;
        }
        else if (string.Equals(leftAlias, sAlias, StringComparison.OrdinalIgnoreCase) && string.Equals(rightAlias, aAlias, StringComparison.OrdinalIgnoreCase))
        {
            targetJoinCol = rightCol;
            subJoinCol = leftCol;
        }
        else
        {
            throw new InvalidOperationException("ON must reference target alias and subquery alias.");
        }

        // Parse SET: a.col = s.col  (single assignment for now)
        var setM = Regex.Match(setSql,
            @"^(?<ta>[A-Za-z0-9_]+)\.(?<tcol>[A-Za-z0-9_`]+)\s*=\s*(?<sa>[A-Za-z0-9_]+)\.(?<scol>[A-Za-z0-9_`]+)$",
            RegexOptions.IgnoreCase);
        if (!setM.Success)
            throw new InvalidOperationException("Only single assignment SET a.col = s.col is supported.");
        if (!string.Equals(setM.Groups["ta"].Value, aAlias, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(setM.Groups["sa"].Value, sAlias, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("SET must assign from subquery alias to target alias.");

        var targetSetCol = setM.Groups["tcol"].Value.Trim('`');
        var subSetCol = setM.Groups["scol"].Value.Trim('`');

        // Execute subquery
        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var q = SqlQueryParser.Parse(subSql, dialect);
        var subRes = executor.ExecuteSelect((SqlSelectQuery)q);

        // Map join key -> set value (last wins, like typical join)
        int subJoinIdx = subRes.GetColumnIndexOrThrow(subJoinCol);
        int subSetIdx = subRes.GetColumnIndexOrThrow(subSetCol);
        var map = new Dictionary<object, object?>(new ObjectEqualityComparer());
        foreach (var r in subRes)
        {
            if (!r.TryGetValue(subJoinIdx, out var k) || k is null || k is DBNull) continue;
            r.TryGetValue(subSetIdx, out var v);
            map[k] = v is DBNull ? null : v;
        }

        // Optional WHERE on target alias: supports simple equality AND-chain
        var whereConds = ParseWhereEqualsList(whereSql);

        var joinInfo = target.GetColumn(targetJoinCol);
        var setInfo = target.GetColumn(targetSetCol);

        int updated = 0;
        for (int i = 0; i < target.Count; i++)
        {
            var row = target[i];
            if (whereConds.Count > 0 && !MatchWhereEquals(target, row, whereConds, pars))
                continue;

            var key = joinInfo.GetGenValue != null ? joinInfo.GetGenValue(row, target) : row[joinInfo.Index];
            if (key is null || key is DBNull) continue;
            if (!map.TryGetValue(key, out var newVal)) continue;

            if (setInfo.GetGenValue is null)
            {
                target.UpdateRowColumn(i, setInfo.Index, newVal);
                target.UpdateIndexesWithRow(i);
                updated++;
            }
        }

        connection.Metrics.Updates += updated;
        return updated;
    }

    private static List<(string Col, string Val)> ParseWhereEqualsList(string? whereSql)
    {
        var list = new List<(string Col, string Val)>();
        if (string.IsNullOrWhiteSpace(whereSql)) return list;

        whereSql = whereSql.Trim().TrimEnd(';');
        // split by AND (case-insensitive)
        var parts = Regex.Split(whereSql, @"\s+AND\s+", RegexOptions.IgnoreCase)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0);

        foreach (var p in parts)
        {
            var kv = p.Split('=', 2);
            if (kv.Length != 2) continue;
            var col = kv[0].Trim();
            // drop alias prefix if present
            var dot = col.IndexOf('.', StringComparison.Ordinal);
            if (dot >= 0) col = col[(dot + 1)..];
            col = col.Trim('`');
            list.Add((col, kv[1].Trim()));
        }
        return list;
    }

    private static bool MatchWhereEquals(
        ITableMock table,
        IReadOnlyDictionary<int, object?> row,
        List<(string Col, string Val)> conds,
        DbParameterCollection? pars)
    {
        foreach (var (Col, Val) in conds)
        {
            var info = table.GetColumn(Col);
            table.CurrentColumn = Col;
            var exp = table.Resolve(Val, info.DbType, info.Nullable, pars, table.Columns);
            table.CurrentColumn = null;
            var expected = exp is DBNull ? null : exp;
            var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];
            if (!Equals(actual, expected)) return false;
        }
        return true;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteDeleteFromSelect(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteDeleteFromSelectImpl(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
            return ExecuteDeleteFromSelectImpl(connection, query, pars, dialect);
    }

    private static int ExecuteDeleteFromSelectImpl(
        DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Minimal grammar for unit tests:
        // DELETE a FROM <table> a JOIN (<select>) s ON s.k = a.k
        var m = _regexDelete.Match(query.RawSql);
        if (!m.Success)
            throw new InvalidOperationException("Invalid DELETE ... JOIN (SELECT ...) statement.");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = m.Groups["a2"].Value;
        var subSql = m.Groups["sub"].Value;
        var sAlias = m.Groups["s"].Value;
        var onSql = m.Groups["on"].Value.Trim();

        if (!connection.TryGetTable(tableName, out var target, query.Table?.DbName) || target == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        var onM = _regexOnSql.Match(onSql);
        if (!onM.Success)
            throw new InvalidOperationException("Only simple equality ON is supported in DELETE FROM SELECT.");

        var leftAlias = onM.Groups["l"].Value;
        var leftCol = onM.Groups["lc"].Value.Trim('`');
        var rightAlias = onM.Groups["r"].Value;
        var rightCol = onM.Groups["rc"].Value.Trim('`');

        string targetJoinCol;
        string subJoinCol;
        if (string.Equals(leftAlias, aAlias, StringComparison.OrdinalIgnoreCase) && string.Equals(rightAlias, sAlias, StringComparison.OrdinalIgnoreCase))
        {
            targetJoinCol = leftCol;
            subJoinCol = rightCol;
        }
        else if (string.Equals(leftAlias, sAlias, StringComparison.OrdinalIgnoreCase) && string.Equals(rightAlias, aAlias, StringComparison.OrdinalIgnoreCase))
        {
            targetJoinCol = rightCol;
            subJoinCol = leftCol;
        }
        else
        {
            throw new InvalidOperationException("ON must reference target alias and subquery alias.");
        }

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var q = SqlQueryParser.Parse(subSql, dialect);
        var subRes = executor.ExecuteSelect((SqlSelectQuery)q);

        int subJoinIdx = subRes.GetColumnIndexOrThrow(subJoinCol);
        var keys = new HashSet<object>(new ObjectEqualityComparer());
        foreach (var r in subRes)
        {
            if (!r.TryGetValue(subJoinIdx, out var k) || k is null || k is DBNull) continue;
            keys.Add(k);
        }

        var joinInfo = target.GetColumn(targetJoinCol);
        int deleted = 0;
        for (int i = target.Count - 1; i >= 0; i--)
        {
            var row = target[i];
            var key = joinInfo.GetGenValue != null ? joinInfo.GetGenValue(row, target) : row[joinInfo.Index];
            if (key is null || key is DBNull) continue;
            if (!keys.Contains(key)) continue;
            target.RemoveAt(i);
            deleted++;
        }

        // rebuild indexes
        target.RebuildAllIndexes();
        connection.Metrics.Deletes += deleted;
        return deleted;
    }

    private sealed class ObjectEqualityComparer : IEqualityComparer<object>
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public new bool Equals(object? x, object? y) => object.Equals(x, y);
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int GetHashCode(object obj) => obj?.GetHashCode() ?? 0;
    }
}
