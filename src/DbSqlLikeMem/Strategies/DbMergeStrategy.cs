namespace DbSqlLikeMem;

internal static class DbMergeStrategy
{
    /// <summary>
    /// EN: Implements ExecuteMerge.
    /// PT: Implementa ExecuteMerge.
    /// </summary>
    public static DmlExecutionResult ExecuteMerge(
        this DbConnectionMockBase connection,
        SqlMergeQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        DmlExecutionResult affected;
        if (!connection.Db.ThreadSafe)
            affected = ExecuteMergeImpl(connection, query, pars, dialect);
        else
        {
            lock (connection.Db.SyncRoot)
                affected = ExecuteMergeImpl(connection, query, pars, dialect);
        }

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteMergeImpl(
        DbConnectionMockBase connection,
        SqlMergeQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var sql = query.RawSql;
        var targetMatch = Regex.Match(
            sql,
            @"MERGE\s+INTO\s+(?<target>[A-Za-z0-9_#]+)(?:\s+AS)?\s+(?<alias>[A-Za-z0-9_]+)?",
            RegexOptions.IgnoreCase);
        if (!targetMatch.Success)
            throw new InvalidOperationException(SqlExceptionMessages.MergeCouldNotIdentifyTargetTable());

        var targetName = targetMatch.Groups["target"].Value;
        var targetAlias = targetMatch.Groups["alias"].Success
            ? targetMatch.Groups["alias"].Value
            : "target";

        var target = connection.GetTable(targetName, query.Table?.DbName);
        var table = (TableMock)target;

        var usingIndex = CultureInfo.InvariantCulture.CompareInfo
            .IndexOf(sql, "USING", CompareOptions.IgnoreCase);
        if (usingIndex < 0)
            throw new InvalidOperationException(SqlExceptionMessages.MergeUsingClauseNotFound());

        var selectSql = ExtractParenthesized(sql, sql.IndexOf('(', usingIndex));
        var srcAliasMatch = Regex.Match(
            sql,
            @"USING\s*\(.*?\)\s+(?:AS\s+)?(?<alias>(?!ON\b|WHEN\b)[A-Za-z0-9_]+)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var sourceAlias = srcAliasMatch.Success ? srcAliasMatch.Groups["alias"].Value : "src";

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var parsedSource = SqlQueryParser.Parse(selectSql, dialect) as SqlSelectQuery
            ?? throw new InvalidOperationException(SqlExceptionMessages.MergeSourceSelectInvalid());
        var sourceTable = executor.ExecuteSelect(parsedSource);

        var onMatch = Regex.Match(
            sql,
            @"ON\s+(?<on>.+?)\s+WHEN",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!onMatch.Success)
            throw new InvalidOperationException(SqlExceptionMessages.MergeOnClauseNotFound());

        var joinMatch = Regex.Match(
            onMatch.Groups["on"].Value,
            @"(?<talias>[A-Za-z0-9_]+)\.(?<tcol>[A-Za-z0-9_]+)\s*=\s*(?<salias>[A-Za-z0-9_]+)\.(?<scol>[A-Za-z0-9_]+)",
            RegexOptions.IgnoreCase);
        if (!joinMatch.Success)
            throw new InvalidOperationException(SqlExceptionMessages.MergeOnConditionNotSupported());

        var targetJoinColumn = joinMatch.Groups["tcol"].Value;
        var sourceJoinColumn = joinMatch.Groups["scol"].Value;

        var updateMatch = Regex.Match(
            sql,
            @"WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(?<set>.+?)(?=WHEN\s+NOT\s+MATCHED|$)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var insertMatch = Regex.Match(
            sql,
            @"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\((?<cols>[^)]*)\)\s*VALUES\s*\((?<vals>[^)]*)\)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        var updates = updateMatch.Success ? SplitByComma(updateMatch.Groups["set"].Value) : [];
        var insertCols = insertMatch.Success ? SplitByComma(insertMatch.Groups["cols"].Value) : [];
        var insertVals = insertMatch.Success ? SplitByComma(insertMatch.Groups["vals"].Value) : [];
        var targetJoinCol = table.GetColumn(targetJoinColumn);
        string[] sourceColumnNames = [.. sourceTable.Columns.Select(col => col.ColumnName)];
        var parsedUpdates = ParseMergeAssignments(table, updates);
        ColumnDef[] insertTargets = [.. insertCols.Select(table.GetColumn)];
        var pendingInsertRows = new List<Dictionary<int, object?>>();
        var pendingJoinKeys = new HashSet<object?>();

        var affected = new DmlExecutionResult();
        foreach (var srcRow in sourceTable)
        {
            var srcValues = new Dictionary<string, object?>(sourceColumnNames.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < sourceColumnNames.Length; i++)
            {
                srcValues[sourceColumnNames[i]] = srcRow.TryGetValue(i, out var v) ? v : null;
            }

            srcValues.TryGetValue(sourceJoinColumn, out var srcKeyRaw);
            var srcKey = CoerceToColumnType(srcKeyRaw, targetJoinCol.DbType);
            if (pendingJoinKeys.Contains(srcKey))
                FlushPendingMergeInsertBatch(table, pendingInsertRows, pendingJoinKeys, ref affected);

            var existingIndex = FindRowIndex(table, targetJoinCol.Index, srcKey);

            if (existingIndex >= 0)
            {
                FlushPendingMergeInsertBatch(table, pendingInsertRows, pendingJoinKeys, ref affected);

                if (parsedUpdates.Length > 0)
                {
                    var oldSnapshot = table[existingIndex].ToDictionary(_ => _.Key, _ => _.Value);
                    foreach (var assignment in parsedUpdates)
                    {
                        var value = ResolveMergeValue(assignment.ValueToken, sourceAlias, srcValues, table, assignment.TargetColumn.Name, pars);
                        table.UpdateRowColumn(existingIndex, assignment.TargetColumn.Index, value);
                    }

                    table.UpdateIndexesWithRow(existingIndex, oldSnapshot, table[existingIndex]);
                }

                affected.IncreseAffected();
                continue;
            }

            if (insertTargets.Length > 0)
            {
                var newRow = new Dictionary<int, object?>(insertTargets.Length);
                for (int i = 0; i < insertTargets.Length; i++)
                {
                    var valueToken = i < insertVals.Count ? insertVals[i] : "NULL";
                    var targetColumn = insertTargets[i];
                    var value = ResolveMergeValue(valueToken, sourceAlias, srcValues, table, targetColumn.Name, pars);
                    newRow[targetColumn.Index] = value is DBNull ? null : value;
                }

                pendingInsertRows.Add(newRow);
                pendingJoinKeys.Add(newRow.TryGetValue(targetJoinCol.Index, out var pendingJoinKey) ? pendingJoinKey : null);
            }
        }

        FlushPendingMergeInsertBatch(table, pendingInsertRows, pendingJoinKeys, ref affected);
        return affected;
    }

    private static void FlushPendingMergeInsertBatch(
        TableMock table,
        List<Dictionary<int, object?>> pendingInsertRows,
        HashSet<object?> pendingJoinKeys,
        ref DmlExecutionResult affected)
    {
        if (pendingInsertRows.Count == 0)
            return;

        table.AddBatch(pendingInsertRows);
        affected.IncreseAffected(pendingInsertRows.Count);
        pendingInsertRows.Clear();
        pendingJoinKeys.Clear();
    }

    private static MergeAssignment[] ParseMergeAssignments(TableMock table, IReadOnlyList<string> assignments)
    {
        if (assignments.Count == 0)
            return [];

        var parsed = new List<MergeAssignment>(assignments.Count);
        foreach (var assignment in assignments)
        {
            var parts = assignment.Split('=').Select(_ => _.Trim()).Take(2).ToArray();
            if (parts.Length != 2)
                continue;

            parsed.Add(new MergeAssignment(table.GetColumn(parts[0]), parts[1]));
        }

        return [.. parsed];
    }

    private static int FindRowIndex(TableMock table, int columnIndex, object? value)
    {
        if (table.PrimaryKeyIndexes.Count == 1
            && table.PrimaryKeyIndexes.Contains(columnIndex)
            && table.TryFindRowByPk(new Dictionary<int, object?> { [columnIndex] = value }, out var pkRowIndex))
        {
            return pkRowIndex;
        }

        for (int i = 0; i < table.Count; i++)
        {
            if (table[i].TryGetValue(columnIndex, out var existing) && Equals(existing, value))
                return i;
        }
        return -1;
    }

    private static object? ResolveMergeValue(
        string raw,
        string sourceAlias,
        IReadOnlyDictionary<string, object?> sourceValues,
        TableMock table,
        string columnName,
        DbParameterCollection pars)
    {
        var token = raw.Trim();
        if (token.StartsWith(sourceAlias + ".", StringComparison.OrdinalIgnoreCase))
        {
            var key = token[(sourceAlias.Length + 1)..];
            sourceValues.TryGetValue(key, out var v);
            return CoerceToColumnType(v, table.GetColumn(columnName).DbType);
        }

        if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        var col = table.GetColumn(columnName);
        table.CurrentColumn = columnName;
        var resolved = table.Resolve(token, col.DbType, col.Nullable, pars, table.Columns);
        table.CurrentColumn = null;
        return resolved is DBNull ? null : CoerceToColumnType(resolved, col.DbType);
    }

    private static object? CoerceToColumnType(object? value, DbType dbType)
    {
        if (value is null || value is DBNull)
            return null;

        try
        {
            return dbType switch
            {
                DbType.Int16 => Convert.ToInt16(value, CultureInfo.InvariantCulture),
                DbType.Int32 => Convert.ToInt32(value, CultureInfo.InvariantCulture),
                DbType.Int64 => Convert.ToInt64(value, CultureInfo.InvariantCulture),
                DbType.Decimal => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                DbType.Double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
                DbType.Single => Convert.ToSingle(value, CultureInfo.InvariantCulture),
                DbType.String => value.ToString(),
                _ => value
            };
        }
        catch
        {
            return value;
        }
    }

    private static List<string> SplitByComma(string raw)
        => [.. raw.Split(',').Select(_=>_.Trim())];

    private readonly record struct MergeAssignment(ColumnDef TargetColumn, string ValueToken);

    private static string ExtractParenthesized(string sql, int startIndex)
    {
        if (startIndex < 0 || startIndex >= sql.Length || sql[startIndex] != '(')
            throw new InvalidOperationException(SqlExceptionMessages.MergeCouldNotReadUsingSubquery());

        var depth = 0;
        var end = startIndex;
        for (; end < sql.Length; end++)
        {
            if (sql[end] == '(') depth++;
            else if (sql[end] == ')')
            {
                depth--;
                if (depth == 0)
                {
                    end++;
                    break;
                }
            }
        }

        if (depth != 0)
            throw new InvalidOperationException(SqlExceptionMessages.MergeUsingClauseUnbalancedParentheses());

        return sql[(startIndex + 1)..(end - 1)].Trim();
    }
}
