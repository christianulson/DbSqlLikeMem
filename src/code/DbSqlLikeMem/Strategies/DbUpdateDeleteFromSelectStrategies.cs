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

    private static bool IsUpdateFromSelectSql(string sql)
        => _regexUpdateJoinMySql.IsMatch(sql) || _regexUpdateFromClause.IsMatch(sql);

    private static bool IsDeleteFromSelectSql(string sql)
        => _regexDelete.IsMatch(sql) || _regexDeleteUsing.IsMatch(sql);

    /// <summary>
    /// EN: Implements ExecuteUpdateSmart.
    /// PT-br: Implementa ExecuteUpdateSmart.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdateSmart(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect UPDATE ... JOIN/UPDATE ... FROM ... JOIN (SELECT ...)
        var result = IsUpdateFromSelectSql(query.RawSql)
            ? connection.ExecuteUpdateFromSelect(query, pars, dialect)
            : connection.ExecuteUpdate(query, pars);

        connection.SetLastFoundRows(result.AffectedRows);
        return result;
    }

    /// <summary>
    /// EN: Implements ExecuteUpdateFromSelect.
    /// PT-br: Implementa ExecuteUpdateFromSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdateFromSelect(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
        => connection.ExecuteUpdateFromSelect(query, new QueryExecutionContext(connection, dialect, pars));

    /// <summary>
    /// EN: Implements ExecuteUpdateSmart using a pre-built execution context.
    /// PT-br: Implementa ExecuteUpdateSmart usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdateSmart(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        QueryExecutionContext context)
        => connection.ExecuteUpdateSmart(query, context.DbParameters, context.Dialect);

    /// <summary>
    /// EN: Implements ExecuteDeleteSmart.
    /// PT-br: Implementa ExecuteDeleteSmart.
    /// </summary>
    public static DmlExecutionResult ExecuteDeleteSmart(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        // Detect DELETE ... JOIN / DELETE ... USING (SELECT ...)
        var affected = IsDeleteFromSelectSql(query.RawSql)
            ? connection.ExecuteDeleteFromSelect(query, pars, dialect)
            : connection.ExecuteDelete(query, pars);

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    /// <summary>
    /// EN: Implements ExecuteDeleteSmart using a pre-built execution context.
    /// PT-br: Implementa ExecuteDeleteSmart usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteDeleteSmart(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        QueryExecutionContext context)
        => connection.ExecuteDeleteSmart(query, context.DbParameters, context.Dialect);

    /// <summary>
    /// EN: Implements ExecuteUpdateFromSelect.
    /// PT-br: Implementa ExecuteUpdateFromSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdateFromSelect(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        QueryExecutionContext context)
        => connection.Db.ExecuteWithLock(() => ExecuteUpdateFromSelectImpl(connection, query, context));

    private static DmlExecutionResult ExecuteUpdateFromSelectImpl(
        DbConnectionMockBase connection,
        SqlUpdateQuery query,
        QueryExecutionContext context)
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
            throw new InvalidOperationException(SqlExceptionMessages.UpdateJoinInvalid());
        var dialect = context.Dialect;
        if (!fromClause && !dialect.SupportsUpdateJoinFromSubquerySyntax)
            throw SqlUnsupported.NotSupported(dialect, "UPDATE ... JOIN (subquery)");

        if (fromClause && !dialect.SupportsUpdateFromJoinSubquerySyntax)
            throw SqlUnsupported.NotSupported(dialect, "UPDATE ... FROM ... JOIN (subquery)");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = fromClause ? m.Groups["a2"].Value : m.Groups["a"].Value;
        var subSql = m.Groups["sub"].Value;
        var sAlias = m.Groups["s"].Value;
        var onSql = m.Groups["on"].Value.Trim();
        var setSql = m.Groups["set"].Value.Trim();
        var whereSql = m.Groups["where"].Success ? m.Groups["where"].Value.Trim() : null;

        if (!connection.TryGetTable(tableName, out var target, query.Table?.DbName) || target == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);

        // ParseCreateView ON: s.k = a.k  OR a.k = s.k
        var onM = Regex.Match(onSql,
            @"^(?<l>[A-Za-z0-9_]+)\.(?<lc>[A-Za-z0-9_`]+)\s*=\s*(?<r>[A-Za-z0-9_]+)\.(?<rc>[A-Za-z0-9_`]+)$",
            RegexOptions.IgnoreCase);
        if (!onM.Success)
            throw new InvalidOperationException(SqlExceptionMessages.UpdateJoinOnlySimpleEqualityOnSupported());

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
            throw new InvalidOperationException(SqlExceptionMessages.JoinOnMustReferenceTargetAndSubqueryAliases());
        }

        // ParseCreateView SET: a.col = s.col  (single assignment for now)
        var setM = Regex.Match(setSql,
            @"^(?<ta>[A-Za-z0-9_]+)\.(?<tcol>[A-Za-z0-9_`]+)\s*=\s*(?<sa>[A-Za-z0-9_]+)\.(?<scol>[A-Za-z0-9_`]+)$",
            RegexOptions.IgnoreCase);
        if (!setM.Success)
            throw new InvalidOperationException(SqlExceptionMessages.UpdateJoinOnlySingleSetAssignmentSupported());
        if (!string.Equals(setM.Groups["ta"].Value, aAlias, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(setM.Groups["sa"].Value, sAlias, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(SqlExceptionMessages.UpdateJoinSetMustAssignFromSubqueryToTargetAlias());

        var targetSetCol = setM.Groups["tcol"].Value.Trim('`');
        var subSetCol = setM.Groups["scol"].Value.Trim('`');

        // Execute subquery
        var executor = context.CreateExecutor();
        var q = SqlQueryParser.Parse(
            subSql,
            connection.Db,
            connection.ExecutionDialect,
            null,
            SqlCustomFunctionResolverFactory.Create(context));
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
        var resolvedWhereConds = ResolveWhereEqualsList(target, whereConds, context.DbParameters);

        var joinInfo = target.GetColumn(targetJoinCol);
        var setInfo = target.GetColumn(targetSetCol);
        var changedCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { targetSetCol };
        var requiresOldSnapshotForIndex = HasIndexedKeyChanges(target, changedCols);
        var updated = new DmlExecutionResult();
        for (int i = 0; i < target.Count; i++)
        {
            var row = target[i];
            if (resolvedWhereConds.Count > 0 && !MatchWhereEquals(target, row, resolvedWhereConds))
                continue;

            var key = joinInfo.GetGenValue != null ? joinInfo.GetGenValue(row, target) : row[joinInfo.Index];
            if (key is null || key is DBNull) continue;
            if (!map.TryGetValue(key, out var newVal)) continue;

            if (setInfo.GetGenValue is null)
            {
                var oldSnapshot = requiresOldSnapshotForIndex
                    ? TableMock.CloneRow(row)
                    : null;
                var simulatedRow = oldSnapshot is null
                    ? TableMock.CloneRow(row)
                    : TableMock.CloneRow(oldSnapshot);
                simulatedRow[setInfo.Index] = newVal;

                if (target is TableMock targetTableMock)
                    targetTableMock.ValidateCheckConstraintsOnRow(simulatedRow);

                target.UpdateRowColumn(i, setInfo.Index, newVal);
                if (target is TableMock targetTableMock2)
                {
                    if (requiresOldSnapshotForIndex)
                        targetTableMock2.IndexManager.UpdateIndexesWithRow(i, oldSnapshot, target[i]);
                    else
                        targetTableMock2.IndexManager.UpdateIndexesWithRow(i);
                }
                else
                {
                    target.UpdateIndexesWithRow(i);
                }
                updated.IncreseAffected();
                updated.AffectedIndexes.Add(i);
                updated.AffectedRowsData.Add(TableMock.SnapshotRow(target[i]));
            }
        }

        if (connection.Metrics.Enabled)
            connection.Metrics.Updates += updated.AffectedRows;
        return updated;
    }

    private static bool HasIndexedKeyChanges(ITableMock table, HashSet<string> changedCols)
    {
        if (changedCols.Count == 0)
            return false;

        if (table is TableMock tableMock)
        {
            foreach (var pkIndex in tableMock.PkIndexArray)
            {
                if (changedCols.Contains(tableMock.GetColumnByIndex(pkIndex).Name))
                    return true;
            }

            foreach (var index in tableMock.UniqueIndexes)
            {
                foreach (var keyCol in index.KeyCols)
                {
                    if (changedCols.Contains(keyCol))
                        return true;
                }
            }

            return false;
        }

        foreach (var pkIndex in table.PrimaryKeyIndexes)
        {
            var column = table is TableMock tableAsMock
                ? tableAsMock.GetColumnByIndex(pkIndex)
                : GetColumnByIndex(table, pkIndex);

            if (changedCols.Contains(column.Name))
                return true;
        }

        foreach (var index in table.Indexes.Values)
        {
            foreach (var keyCol in index.KeyCols)
            {
                if (changedCols.Contains(keyCol))
                    return true;
            }
        }

        return false;
    }
    private static List<(string Col, string Val)> ParseWhereEqualsList(string? whereSql)
    {
        var list = new List<(string Col, string Val)>();
        if (string.IsNullOrWhiteSpace(whereSql))
            return list;

        var span = whereSql.AsSpan().Trim().TrimEnd(';');
        var start = 0;
        while (start < span.Length)
        {
            var andIndex = IndexOfAndSeparator(span, start);
            var part = (andIndex >= 0 ? span[start..andIndex] : span[start..]).Trim();
            if (part.Length > 0)
                TryAddWhereEqualsPart(list, part);

            if (andIndex < 0)
                break;

            start = andIndex + 3;
        }

        return list;
    }

    private static List<ResolvedWhereCondition> ResolveWhereEqualsList(
        ITableMock table,
        IReadOnlyList<(string Col, string Val)> conds,
        DbParameterCollection? pars)
    {
        if (conds.Count == 0)
            return [];

        var resolved = new List<ResolvedWhereCondition>(conds.Count);
        foreach (var (colName, rawValue) in conds)
        {
            var info = table.GetColumn(colName);
            table.CurrentColumn = colName;
            try
            {
                var exp = table.Resolve(rawValue, info.DbType, info.Nullable, pars, table.Columns);
                resolved.Add(new ResolvedWhereCondition(info, exp is DBNull ? null : exp));
            }
            finally
            {
                table.CurrentColumn = null;
            }
        }

        return resolved;
    }

    private static bool MatchWhereEquals(
        ITableMock table,
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyList<ResolvedWhereCondition> conds)
    {
        foreach (var condition in conds)
        {
            var info = condition.Column;
            var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];
            if (!Equals(actual, condition.Expected))
                return false;
        }

        return true;
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

    /// <summary>
    /// EN: Implements ExecuteDeleteFromSelect.
    /// PT-br: Implementa ExecuteDeleteFromSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteDeleteFromSelect(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var context = new QueryExecutionContext(connection, dialect, pars);
        return connection.Db.ExecuteWithLock(() => ExecuteDeleteFromSelectImpl(connection, query, context));
    }

    private static DmlExecutionResult ExecuteDeleteFromSelectImpl(
        DbConnectionMockBase connection,
        SqlDeleteQuery query,
        QueryExecutionContext context)
    {
        // Minimal grammar for unit tests:
        // MySQL/SQL Server: DELETE a FROM <table> a JOIN (<select>) s ON s.k = a.k
        // PostgreSQL:       DELETE FROM <table> a USING (<select>) s WHERE s.k = a.k [AND ...]
        var dialect = context.Dialect;
        var m = _regexDelete.Match(query.RawSql);
        var usingSyntax = false;
        if (!m.Success)
        {
            m = _regexDeleteUsing.Match(query.RawSql);
            usingSyntax = m.Success;
        }
        if (!m.Success)
            throw new InvalidOperationException(SqlExceptionMessages.DeleteJoinInvalid());

        if (!usingSyntax && !dialect.SupportsDeleteTargetFromJoinSubquerySyntax)
            throw SqlUnsupported.NotSupported(dialect, "DELETE <alvo> FROM ... JOIN (subquery)");

        if (usingSyntax && !dialect.SupportsDeleteUsingSubquerySyntax)
            throw SqlUnsupported.NotSupported(dialect, "DELETE FROM ... USING (subquery)");

        var tableName = m.Groups["table"].Value.NormalizeName();
        var aAlias = usingSyntax ? m.Groups["a"].Value : m.Groups["a2"].Value;
        var subSql = m.Groups["sub"].Value;
        var sAlias = m.Groups["s"].Value;
        string? whereSql = null;
        var onSql = m.Groups["on"].Value.Trim();
        if (usingSyntax)
            onSql = ExtractJoinConditionFromWhere(m.Groups["where"].Value, aAlias, sAlias, out whereSql);

        if (!connection.TryGetTable(tableName, out var target, query.Table?.DbName) || target == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);

        var onM = _regexOnSql.Match(onSql);
        if (!onM.Success)
            throw new InvalidOperationException(SqlExceptionMessages.DeleteJoinOnlySimpleEqualityOnSupported());

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
            throw new InvalidOperationException(SqlExceptionMessages.JoinOnMustReferenceTargetAndSubqueryAliases());
        }

        var executor = context.CreateExecutor();
        var q = SqlQueryParser.Parse(
            subSql,
            connection.Db,
            connection.ExecutionDialect,
            null,
            SqlCustomFunctionResolverFactory.Create(context));
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
        var resolvedWhereConds = ResolveWhereEqualsList(target, whereConds, context.DbParameters);
        int deleted = 0;
        for (int i = target.Count - 1; i >= 0; i--)
        {
            var row = target[i];
            var key = joinInfo.GetGenValue != null ? joinInfo.GetGenValue(row, target) : row[joinInfo.Index];
            if (key is null || key is DBNull) continue;
            if (!keys.Contains(key)) continue;
            if (resolvedWhereConds.Count > 0 && !MatchWhereEquals(target, row, resolvedWhereConds)) continue;
            target.RemoveAt(i);
            deleted++;
        }

        if (connection.Metrics.Enabled)
            connection.Metrics.Deletes += deleted;

        return new DmlExecutionResult
        {
            AffectedRows = deleted,
            AffectedIndexes = [],//affectedIndexes,
            AffectedRowsData = []//affectedRowsData
        };
    }

    private readonly record struct ResolvedWhereCondition(ColumnDef Column, object? Expected);

    private static string ExtractJoinConditionFromWhere(string whereSql, string targetAlias, string subAlias, out string? remainingWhere)
    {
        remainingWhere = null;
        if (string.IsNullOrWhiteSpace(whereSql))
            throw new InvalidOperationException(SqlExceptionMessages.DeleteUsingWhereMustContainJoinEqualityCondition());

        var span = whereSql.AsSpan().Trim().TrimEnd(';');
        var parts = new List<string>();
        string? joinCondition = null;
        var start = 0;
        while (start < span.Length)
        {
            var andIndex = IndexOfAndSeparator(span, start);
            var part = (andIndex >= 0 ? span[start..andIndex] : span[start..]).Trim();
            if (part.Length > 0)
            {
                if (joinCondition is null && TryParseJoinCondition(part, targetAlias, subAlias, out var parsedJoinCondition))
                    joinCondition = parsedJoinCondition;
                else
                    parts.Add(part.ToString());
            }

            if (andIndex < 0)
                break;

            start = andIndex + 3;
        }

        if (joinCondition is null)
            throw new InvalidOperationException(SqlExceptionMessages.DeleteUsingWhereMustContainJoinEqualityCondition());

        remainingWhere = parts.Count == 0 ? null : string.Join(SqlConst._AND_, parts);
        return joinCondition;
    }

    private static void TryAddWhereEqualsPart(List<(string Col, string Val)> list, ReadOnlySpan<char> part)
    {
        var eqIndex = part.IndexOf('=');
        if (eqIndex < 0)
            return;

        var col = part[..eqIndex].Trim();
        var value = part[(eqIndex + 1)..].Trim();
        var dotIndex = col.LastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < col.Length)
            col = col[(dotIndex + 1)..];

        var normalizedColumn = TrimIdentifier(col);
        if (normalizedColumn.Length == 0 || value.Length == 0)
            return;

        list.Add((normalizedColumn.ToString(), value.ToString()));
    }

    private static bool TryParseJoinCondition(
        ReadOnlySpan<char> part,
        string targetAlias,
        string subAlias,
        out string joinCondition)
    {
        joinCondition = string.Empty;

        var candidate = part;
        while (candidate.StartsWith("(") && candidate.EndsWith(")"))
            candidate = candidate[1..^1].Trim();

        var onM = _regexOnSql.Match(candidate.ToString());
        if (!onM.Success)
            return false;

        var l = onM.Groups["l"].Value;
        var r = onM.Groups["r"].Value;
        var valid = (l.Equals(targetAlias, StringComparison.OrdinalIgnoreCase) && r.Equals(subAlias, StringComparison.OrdinalIgnoreCase))
            || (l.Equals(subAlias, StringComparison.OrdinalIgnoreCase) && r.Equals(targetAlias, StringComparison.OrdinalIgnoreCase));
        if (!valid)
            return false;

        joinCondition = candidate.ToString();
        return true;
    }

    private static ReadOnlySpan<char> TrimIdentifier(ReadOnlySpan<char> value)
        => StringCompatibility.Trim(value, '`', '"', '[', ']');

    private static int IndexOfAndSeparator(ReadOnlySpan<char> span, int start)
    {
        for (var i = start; i <= span.Length - 3; i++)
        {
            if (IsAndToken(span, i))
                return i;
        }

        return -1;
    }

    private static bool IsAndToken(ReadOnlySpan<char> span, int index)
    {
        if (index < 0 || index + 2 >= span.Length)
            return false;

        if (!(span[index] is 'A' or 'a')
            || !(span[index + 1] is 'N' or 'n')
            || !(span[index + 2] is 'D' or 'd'))
        {
            return false;
        }

        var beforeOk = index == 0 || char.IsWhiteSpace(span[index - 1]) || span[index - 1] == '(';
        var afterIndex = index + 3;
        var afterOk = afterIndex >= span.Length || char.IsWhiteSpace(span[afterIndex]) || span[afterIndex] == ')';
        return beforeOk && afterOk;
    }

    private sealed class ObjectEqualityComparer : IEqualityComparer<object>
    {
        /// <summary>
        /// EN: Implements Equals.
        /// PT-br: Implementa Equals.
        /// </summary>
        public new bool Equals(object? x, object? y) => object.Equals(x, y);
        /// <summary>
        /// EN: Implements GetHashCode.
        /// PT-br: Implementa GetHashCode.
        /// </summary>
        public int GetHashCode(object obj) => obj?.GetHashCode() ?? 0;
    }
}
