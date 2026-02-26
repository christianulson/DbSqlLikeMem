namespace DbSqlLikeMem;

internal static class DbUpdateDeleteFromSelectStrategies
{
    private static readonly Regex _regexDelete = new(
        @"^DELETE\s+(?<a>[A-Za-z0-9_]+)\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a2>[A-Za-z0-9_]+)\s+JOIN\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+ON\s+(?<on>[\s\S]*?)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex _regexOnSql = new(@"^(?<l>[A-Za-z0-9_]+)\.(?<lc>[A-Za-z0-9_`]+)\s*=\s*(?<r>[A-Za-z0-9_]+)\.(?<rc>[A-Za-z0-9_`]+)$", RegexOptions.IgnoreCase);
    private static readonly Regex _regexUpdateJoinMySql = new(
        @"^UPDATE\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a>[A-Za-z0-9_]+)\s+JOIN\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+ON\s+(?<on>[\s\S]*?)\s+SET\s+(?<set>[\s\S]*?)(\s+WHERE\s+(?<where>[\s\S]*))?;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex _regexUpdateFromClause = new(
        @"^UPDATE\s+(?<a>[A-Za-z0-9_]+)\s+SET\s+(?<set>[\s\S]*?)\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a2>[A-Za-z0-9_]+)\s+JOIN\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+ON\s+(?<on>[\s\S]*?)(\s+WHERE\s+(?<where>[\s\S]*))?;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex _regexDeleteUsing = new(
        @"^DELETE\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?\s+(?<a>[A-Za-z0-9_]+)\s+USING\s*\(\s*(?<sub>(SELECT|WITH)\b[\s\S]*?)\s*\)\s+(?<s>[A-Za-z0-9_]+)\s+WHERE\s+(?<where>[\s\S]*?)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static bool IsMySql(ISqlDialect dialect)
        => dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase);

    private static bool IsSqlServer(ISqlDialect dialect)
        => dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase);

    private static bool IsPostgreSql(ISqlDialect dialect)
        => dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase);

    private static bool IsUpdateFromSelectSql(string sql)
        => _regexUpdateJoinMySql.IsMatch(sql) || _regexUpdateFromClause.IsMatch(sql);

    private static bool IsDeleteFromSelectSql(string sql)
        => _regexDelete.IsMatch(sql) || _regexDeleteUsing.IsMatch(sql);

    /// <summary>
    /// EN: Implements ExecuteUpdateSmart.
    /// PT: Implementa ExecuteUpdateSmart.
    /// </summary>
    public static int ExecuteUpdateSmart(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect UPDATE ... JOIN/UPDATE ... FROM ... JOIN (SELECT ...)
        if (IsUpdateFromSelectSql(query.RawSql))
            return connection.ExecuteUpdateFromSelect(query, pars, dialect);
        return connection.ExecuteUpdate(query, pars);
    }

    /// <summary>
    /// EN: Implements ExecuteDeleteSmart.
    /// PT: Implementa ExecuteDeleteSmart.
    /// </summary>
    public static int ExecuteDeleteSmart(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect DELETE ... JOIN / DELETE ... USING (SELECT ...)
        if (IsDeleteFromSelectSql(query.RawSql))
            return connection.ExecuteDeleteFromSelect(query, pars, dialect);
        return connection.ExecuteDelete(query, pars);
    }

    /// <summary>
    /// EN: Implements ExecuteUpdateFromSelect.
    /// PT: Implementa ExecuteUpdateFromSelect.
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
        // MySQL:                 UPDATE <table> <a> JOIN (<select>) <s> ON ... SET ... [WHERE ...]
        // SQL Server/PostgreSQL: UPDATE <a> SET ... FROM <table> <a> JOIN (<select>) <s> ON ... [WHERE ...]
        var m = _regexUpdateJoinMySql.Match(query.RawSql);
        var fromClause = false;
        if (!m.Success)
        {
            m = _regexUpdateFromClause.Match(query.RawSql);
            fromClause = m.Success;
        }

        if (!m.Success)
            throw new InvalidOperationException("UPDATE ... JOIN inválido. Use os formatos: UPDATE <tabela> <alias> JOIN (<select>) ... SET ... ou UPDATE <alias> SET ... FROM <tabela> <alias> JOIN (<select>) ...");

        if (!fromClause && !IsMySql(dialect))
            throw SqlUnsupported.ForDialect(dialect, "UPDATE ... JOIN (subquery)");

        if (fromClause && !IsSqlServer(dialect) && !IsPostgreSql(dialect))
            throw SqlUnsupported.ForDialect(dialect, "UPDATE ... FROM ... JOIN (subquery)");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = fromClause ? m.Groups["a2"].Value : m.Groups["a"].Value;
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
            throw new InvalidOperationException("Apenas ON com igualdade simples é suportado em UPDATE ... JOIN: <alias1>.<col> = <alias2>.<col>.");

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
            throw new InvalidOperationException("ON deve referenciar o alias da tabela alvo e o alias da subconsulta.");
        }

        // Parse SET: a.col = s.col  (single assignment for now)
        var setM = Regex.Match(setSql,
            @"^(?<ta>[A-Za-z0-9_]+)\.(?<tcol>[A-Za-z0-9_`]+)\s*=\s*(?<sa>[A-Za-z0-9_]+)\.(?<scol>[A-Za-z0-9_`]+)$",
            RegexOptions.IgnoreCase);
        if (!setM.Success)
            throw new InvalidOperationException("Apenas SET com única atribuição é suportado: <aliasAlvo>.<col> = <aliasSub>.<col>.");
        if (!string.Equals(setM.Groups["ta"].Value, aAlias, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(setM.Groups["sa"].Value, sAlias, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("SET deve atribuir do alias da subconsulta para o alias da tabela alvo.");

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

        whereSql = whereSql!.Trim().TrimEnd(';');
        // split by AND (case-insensitive)
        var parts = Regex.Split(whereSql, @"\s+AND\s+", RegexOptions.IgnoreCase)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0);

        foreach (var p in parts)
        {
            var kv = p.Split('=').Take(2).ToArray();
            if (kv.Length != 2) continue;
            var col = kv[0].Trim();
            // drop alias prefix if present
            var dot = col.IndexOf('.');
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
    /// EN: Implements ExecuteDeleteFromSelect.
    /// PT: Implementa ExecuteDeleteFromSelect.
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
        // MySQL/SQL Server: DELETE a FROM <table> a JOIN (<select>) s ON s.k = a.k
        // PostgreSQL:       DELETE FROM <table> a USING (<select>) s WHERE s.k = a.k [AND ...]
        var m = _regexDelete.Match(query.RawSql);
        var usingSyntax = false;
        if (!m.Success)
        {
            m = _regexDeleteUsing.Match(query.RawSql);
            usingSyntax = m.Success;
        }
        if (!m.Success)
            throw new InvalidOperationException("DELETE ... JOIN inválido. Use os formatos: DELETE <alvo> FROM <tabela> <alias> JOIN (<select>) ... ON ... ou DELETE FROM <tabela> <alias> USING (<select>) ... WHERE ...");

        if (!usingSyntax && !IsMySql(dialect) && !IsSqlServer(dialect))
            throw SqlUnsupported.ForDialect(dialect, "DELETE <alvo> FROM ... JOIN (subquery)");

        if (usingSyntax && !IsPostgreSql(dialect))
            throw SqlUnsupported.ForDialect(dialect, "DELETE FROM ... USING (subquery)");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = usingSyntax ? m.Groups["a"].Value : m.Groups["a2"].Value;
        var subSql = m.Groups["sub"].Value;
        var sAlias = m.Groups["s"].Value;
        string? whereSql = null;
        var onSql = m.Groups["on"].Value.Trim();
        if (usingSyntax)
            onSql = ExtractJoinConditionFromWhere(m.Groups["where"].Value, aAlias, sAlias, out whereSql);

        if (!connection.TryGetTable(tableName, out var target, query.Table?.DbName) || target == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        var onM = _regexOnSql.Match(onSql);
        if (!onM.Success)
            throw new InvalidOperationException("Apenas ON com igualdade simples é suportado em DELETE ... JOIN: <alias1>.<col> = <alias2>.<col>.");

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
            throw new InvalidOperationException("ON deve referenciar o alias da tabela alvo e o alias da subconsulta.");
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
        var whereConds = ParseWhereEqualsList(whereSql);
        int deleted = 0;
        for (int i = target.Count - 1; i >= 0; i--)
        {
            var row = target[i];
            var key = joinInfo.GetGenValue != null ? joinInfo.GetGenValue(row, target) : row[joinInfo.Index];
            if (key is null || key is DBNull) continue;
            if (!keys.Contains(key)) continue;
            if (!string.IsNullOrWhiteSpace(whereSql) && !MatchWhereEquals(target, row, whereConds, pars)) continue;
            target.RemoveAt(i);
            deleted++;
        }

        // rebuild _indexes
        target.RebuildAllIndexes();
        connection.Metrics.Deletes += deleted;
        return deleted;
    }

    private static string ExtractJoinConditionFromWhere(string whereSql, string targetAlias, string subAlias, out string? remainingWhere)
    {
        remainingWhere = null;
        var parts = Regex.Split(whereSql.Trim().TrimEnd(';'), @"\s+AND\s+", RegexOptions.IgnoreCase)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .ToList();

        for (int i = 0; i < parts.Count; i++)
        {
            var candidate = parts[i].Trim();
            while (candidate.StartsWith('(') && candidate.EndsWith(')'))
                candidate = candidate[1..^1].Trim();
            var onM = _regexOnSql.Match(candidate);
            if (!onM.Success)
                continue;

            var l = onM.Groups["l"].Value;
            var r = onM.Groups["r"].Value;
            var valid = (l.Equals(targetAlias, StringComparison.OrdinalIgnoreCase) && r.Equals(subAlias, StringComparison.OrdinalIgnoreCase))
                || (l.Equals(subAlias, StringComparison.OrdinalIgnoreCase) && r.Equals(targetAlias, StringComparison.OrdinalIgnoreCase));
            if (!valid)
                continue;

            parts.RemoveAt(i);
            remainingWhere = parts.Count == 0 ? null : string.Join(" AND ", parts);
            return candidate;
        }

        throw new InvalidOperationException("WHERE deve conter uma condição de junção por igualdade entre aliases de alvo e subconsulta (ex.: s.id = a.id).");
    }

    private sealed class ObjectEqualityComparer : IEqualityComparer<object>
    {
        /// <summary>
        /// EN: Implements Equals.
        /// PT: Implementa Equals.
        /// </summary>
        public new bool Equals(object? x, object? y) => object.Equals(x, y);
        /// <summary>
        /// EN: Implements GetHashCode.
        /// PT: Implementa GetHashCode.
        /// </summary>
        public int GetHashCode(object obj) => obj?.GetHashCode() ?? 0;
    }
}
