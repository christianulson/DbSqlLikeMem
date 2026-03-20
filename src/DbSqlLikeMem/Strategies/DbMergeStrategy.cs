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
        var target = connection.GetTable(targetName, query.Table?.DbName);
        var table = (TableMock)target;

        var usingIndex = CultureInfo.InvariantCulture.CompareInfo
            .IndexOf(sql, SqlConst.USING, CompareOptions.IgnoreCase);
        if (usingIndex < 0)
            throw new InvalidOperationException(SqlExceptionMessages.MergeUsingClauseNotFound());

        var selectSql = ExtractParenthesized(sql, sql.IndexOf('(', usingIndex), out var usingCloseIndex);
        var sourceTail = sql[usingCloseIndex..];
        var srcAliasMatch = Regex.Match(
            sourceTail,
            @"^\s+(?:AS\s+)?(?<alias>(?!ON\b|WHEN\b)[A-Za-z0-9_]+)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var sourceAlias = srcAliasMatch.Success ? srcAliasMatch.Groups["alias"].Value : "src";
        var srcColumnsMatch = Regex.Match(
            sourceTail,
            @"^\s+(?:AS\s+)?[A-Za-z0-9_]+\s*\((?<cols>[^)]*)\)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        List<string> sourceColumnNames = srcColumnsMatch.Success
            ? [.. SplitByComma(srcColumnsMatch.Groups["cols"].Value).Where(static col => !string.IsNullOrWhiteSpace(col))]
            : [];

        TableResultMock sourceTable;
        if (!TryBuildValuesSource(
                selectSql,
                sourceAlias,
                sourceColumnNames,
                dialect,
                pars,
                out sourceTable))
        {
            sourceTable = ExecuteMergeSourceSelect(selectSql, connection, pars, dialect);
        }

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
        string[] sourceColumnNames2 = [.. sourceTable.Columns.Select(col => col.ColumnName)];
        var parsedUpdates = ParseMergeAssignments(table, updates);
        ColumnDef[] insertTargets = [.. insertCols.Select(table.GetColumn)];
        var pendingInsertRows = new List<Dictionary<int, object?>>();
        var pendingJoinKeys = new HashSet<object?>();

        var affected = new DmlExecutionResult();
        foreach (var srcRow in sourceTable)
        {
            var srcValues = new Dictionary<string, object?>(sourceColumnNames2.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < sourceColumnNames2.Length; i++)
            {
                srcValues[sourceColumnNames2[i]] = srcRow.TryGetValue(i, out var v) ? v : null;
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
                    var valueToken = i < insertVals.Count ? insertVals[i] : SqlConst.NULL;
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

        if (token.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
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

    private static TableResultMock ExecuteMergeSourceSelect(
        string selectSql,
        DbConnectionMockBase connection,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var parsedSource = SqlQueryParser.Parse(selectSql, dialect) as SqlSelectQuery
            ?? throw new InvalidOperationException(SqlExceptionMessages.MergeSourceSelectInvalid());

        return executor.ExecuteSelect(parsedSource);
    }

    private static bool TryBuildValuesSource(
        string sourceSql,
        string sourceAlias,
        IReadOnlyList<string> sourceColumnNames,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out TableResultMock sourceTable)
    {
        sourceTable = new TableResultMock();

        if (!sourceSql.TrimStart().StartsWith(SqlConst.VALUES, StringComparison.OrdinalIgnoreCase))
            return false;

        var valuesBody = sourceSql.TrimStart();
        valuesBody = valuesBody[SqlConst.VALUES.Length..].TrimStart();
        if (string.IsNullOrWhiteSpace(valuesBody))
            throw new InvalidOperationException("MERGE USING VALUES requires at least one row.");

        var rows = SplitValuesRows(valuesBody);
        if (rows.Count == 0)
            throw new InvalidOperationException("MERGE USING VALUES requires at least one row.");

        var firstRowItems = SplitTopLevelCommaSeparated(rows[0]);
        if (firstRowItems.Count == 0)
            throw new InvalidOperationException("MERGE USING VALUES row requires at least one expression.");

        var columnNames = sourceColumnNames.Count > 0
            ? sourceColumnNames
            : [.. Enumerable.Range(0, firstRowItems.Count).Select(i => $"C{i + 1}")];

        if (columnNames.Count != firstRowItems.Count)
            throw new InvalidOperationException(
                $"MERGE USING VALUES column count ({columnNames.Count}) does not match row 1 expression count ({firstRowItems.Count}).");

        sourceTable.Columns = [
            .. columnNames.Select((columnName, index) => new TableResultColMock(
                sourceAlias,
                columnName,
                columnName,
                index,
                DbType.Object,
                true))
        ];

        for (var rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            var items = SplitTopLevelCommaSeparated(rows[rowIndex]);
            if (items.Count != columnNames.Count)
                throw new InvalidOperationException(
                    $"MERGE USING VALUES row {rowIndex + 1} expression count ({items.Count}) does not match row 1 expression count ({columnNames.Count}).");

            var row = new Dictionary<int, object?>(columnNames.Count);
            for (var columnIndex = 0; columnIndex < items.Count; columnIndex++)
            {
                var expr = SqlExpressionParser.ParseScalar(items[columnIndex], dialect, pars);
                row[columnIndex] = EvaluateValuesSourceExpression(expr, pars, dialect);
            }

            sourceTable.Add(row);
        }

        return true;
    }

    private static object? EvaluateValuesSourceExpression(
        SqlExpr expr,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        return expr switch
        {
            LiteralExpr lit => lit.Value is DBNull ? null : lit.Value,
            ParameterExpr p => TryResolveParameterValue(pars, p.Name, out var value) ? value : null,
            UnaryExpr { Op: SqlUnaryOp.Not, Expr: var inner } => !Convert.ToBoolean(EvaluateValuesSourceExpression(inner, pars, dialect) ?? false),
            BinaryExpr { Op: SqlBinaryOp.Add } b => Convert.ToDecimal(EvaluateValuesSourceExpression(b.Left, pars, dialect) ?? 0m)
                + Convert.ToDecimal(EvaluateValuesSourceExpression(b.Right, pars, dialect) ?? 0m),
            BinaryExpr { Op: SqlBinaryOp.Subtract } b => Convert.ToDecimal(EvaluateValuesSourceExpression(b.Left, pars, dialect) ?? 0m)
                - Convert.ToDecimal(EvaluateValuesSourceExpression(b.Right, pars, dialect) ?? 0m),
            BinaryExpr { Op: SqlBinaryOp.Multiply } b => Convert.ToDecimal(EvaluateValuesSourceExpression(b.Left, pars, dialect) ?? 0m)
                * Convert.ToDecimal(EvaluateValuesSourceExpression(b.Right, pars, dialect) ?? 0m),
            BinaryExpr { Op: SqlBinaryOp.Divide } b => Convert.ToDecimal(EvaluateValuesSourceExpression(b.Left, pars, dialect) ?? 0m)
                / Convert.ToDecimal(EvaluateValuesSourceExpression(b.Right, pars, dialect) ?? 0m),
            BinaryExpr { Op: SqlBinaryOp.Concat } b => string.Concat(
                EvaluateValuesSourceExpression(b.Left, pars, dialect)?.ToString() ?? string.Empty,
                EvaluateValuesSourceExpression(b.Right, pars, dialect)?.ToString() ?? string.Empty),
            CallExpr call when call.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, call.Name, out var temporal) => temporal,
            FunctionCallExpr fn when fn.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, fn.Name, out var temporal) => temporal,
            IdentifierExpr id when string.Equals(id.Name, SqlConst.NULL, StringComparison.OrdinalIgnoreCase) => null,
            IdentifierExpr id when string.Equals(id.Name, SqlConst.TRUE, StringComparison.OrdinalIgnoreCase) => true,
            IdentifierExpr id when string.Equals(id.Name, SqlConst.FALSE, StringComparison.OrdinalIgnoreCase) => false,
            _ => throw new NotSupportedException(
                $"MERGE USING VALUES expression '{expr.GetType().Name}' is not supported yet.")
        };
    }

    private static bool TryResolveParameterValue(
        DbParameterCollection? pars,
        string parameterToken,
        out object? value)
    {
        value = null;
        if (pars is null)
            return false;

        if (parameterToken == "?")
        {
            if (pars.Count <= 0 || pars[0] is not IDataParameter first)
                return false;

            value = first.Value is DBNull ? null : first.Value;
            return true;
        }

        var normalized = parameterToken.TrimStart('@', ':', '?');
        foreach (IDataParameter p in pars)
        {
            var candidate = (p.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            value = p.Value is DBNull ? null : p.Value;
            return true;
        }

        return false;
    }

    private static List<string> SplitTopLevelCommaSeparated(string raw)
    {
        var items = new List<string>();
        var start = 0;
        var depth = 0;
        var inString = false;

        for (var i = 0; i < raw.Length; i++)
        {
            var ch = raw[i];
            if (inString)
            {
                if (ch == '\'' && i + 1 < raw.Length && raw[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inString = false;

                continue;
            }

            if (ch == '\'')
            {
                inString = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                var item = raw[start..i].Trim();
                if (!string.IsNullOrWhiteSpace(item))
                    items.Add(item);
                start = i + 1;
            }
        }

        var last = raw[start..].Trim();
        if (!string.IsNullOrWhiteSpace(last))
            items.Add(last);

        return items;
    }

    private static List<string> SplitValuesRows(string raw)
    {
        var rows = new List<string>();
        var i = 0;

        while (i < raw.Length)
        {
            while (i < raw.Length && char.IsWhiteSpace(raw[i]))
                i++;

            if (i >= raw.Length)
                break;

            if (raw[i] != '(')
                throw new InvalidOperationException("MERGE USING VALUES expects row tuples enclosed in parentheses.");

            rows.Add(ReadParenthesizedContent(raw, ref i));

            while (i < raw.Length && char.IsWhiteSpace(raw[i]))
                i++;

            if (i >= raw.Length)
                break;

            if (raw[i] != ',')
                throw new InvalidOperationException("MERGE USING VALUES must separate row tuples with commas.");

            i++;
        }

        return rows;
    }

    private static string ReadParenthesizedContent(
        string raw,
        ref int index)
    {
        if (index < 0 || index >= raw.Length || raw[index] != '(')
            throw new InvalidOperationException("MERGE USING VALUES row tuple was not opened correctly.");

        var start = ++index;
        var depth = 0;
        var inString = false;
        for (; index < raw.Length; index++)
        {
            var ch = raw[index];
            if (inString)
            {
                if (ch == '\'' && index + 1 < raw.Length && raw[index + 1] == '\'')
                {
                    index++;
                    continue;
                }

                if (ch == '\'')
                    inString = false;

                continue;
            }

            if (ch == '\'')
            {
                inString = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth == 0)
                {
                    var content = raw[start..index].Trim();
                    index++;
                    return content;
                }

                depth--;
            }
        }

        throw new InvalidOperationException("MERGE USING VALUES row tuple was not closed correctly.");
    }

    private static string ExtractParenthesized(string sql, int startIndex, out int endIndex)
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

        endIndex = end;
        return sql[(startIndex + 1)..(end - 1)].Trim();
    }
}
