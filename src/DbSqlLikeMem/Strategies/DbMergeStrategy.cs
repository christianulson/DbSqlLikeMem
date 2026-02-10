using System.Globalization;

namespace DbSqlLikeMem;

internal static class DbMergeStrategy
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteMerge(
        this DbConnectionMockBase connection,
        SqlMergeQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return ExecuteMergeImpl(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
            return ExecuteMergeImpl(connection, query, pars, dialect);
    }

    private static int ExecuteMergeImpl(
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
            throw new InvalidOperationException("MERGE: não foi possível identificar a tabela alvo.");

        var targetName = targetMatch.Groups["target"].Value;
        var targetAlias = targetMatch.Groups["alias"].Success
            ? targetMatch.Groups["alias"].Value
            : "target";

        var target = connection.GetTable(targetName, query.Table?.DbName);
        var table = (TableMock)target;

        var usingIndex = CultureInfo.InvariantCulture.CompareInfo
            .IndexOf(sql, "USING", CompareOptions.IgnoreCase);
        if (usingIndex < 0)
            throw new InvalidOperationException("MERGE: cláusula USING não encontrada.");

        var selectSql = ExtractParenthesized(sql, sql.IndexOf('(', usingIndex));
        var srcAliasMatch = Regex.Match(
            sql,
            @"USING\s*\(.*?\)\s+AS\s+(?<alias>[A-Za-z0-9_]+)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var sourceAlias = srcAliasMatch.Success ? srcAliasMatch.Groups["alias"].Value : "src";

        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars);
        var parsedSource = SqlQueryParser.Parse(selectSql, dialect) as SqlSelectQuery
            ?? throw new InvalidOperationException("MERGE: source SELECT inválido.");
        var sourceTable = executor.ExecuteSelect(parsedSource);

        var onMatch = Regex.Match(
            sql,
            @"ON\s+(?<on>.+?)\s+WHEN",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!onMatch.Success)
            throw new InvalidOperationException("MERGE: cláusula ON não encontrada.");

        var joinMatch = Regex.Match(
            onMatch.Groups["on"].Value,
            @"(?<talias>[A-Za-z0-9_]+)\.(?<tcol>[A-Za-z0-9_]+)\s*=\s*(?<salias>[A-Za-z0-9_]+)\.(?<scol>[A-Za-z0-9_]+)",
            RegexOptions.IgnoreCase);
        if (!joinMatch.Success)
            throw new InvalidOperationException("MERGE: condição ON não suportada.");

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

        var affected = 0;
        foreach (var srcRow in sourceTable)
        {
            var srcValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < sourceTable.Columns.Count; i++)
            {
                var colName = sourceTable.Columns[i].ColumnName;
                srcValues[colName] = srcRow.TryGetValue(i, out var v) ? v : null;
            }

            srcValues.TryGetValue(sourceJoinColumn, out var srcKey);
            var targetCol = table.GetColumn(targetJoinColumn);
            var existingIndex = FindRowIndex(table, targetCol.Index, srcKey);

            if (existingIndex >= 0)
            {
                if (updates.Count > 0)
                {
                    foreach (var assignment in updates)
                {
                    var parts = assignment.Split('=', 2, StringSplitOptions.TrimEntries);
                    if (parts.Length != 2) continue;
                    var colName = parts[0];
                    var valueToken = parts[1];
                    var value = ResolveMergeValue(valueToken, sourceAlias, srcValues, table, colName, pars);
                    var col = table.GetColumn(colName);
                    table.UpdateRowColumn(existingIndex, col.Index, value);
                }

                    table.RebuildAllIndexes();
                }

                affected++;
                continue;
            }

            if (insertCols.Count > 0)
            {
                var newRow = new Dictionary<int, object?>();
                for (int i = 0; i < insertCols.Count; i++)
                {
                    var colName = insertCols[i];
                    var valueToken = i < insertVals.Count ? insertVals[i] : "NULL";
                    var value = ResolveMergeValue(valueToken, sourceAlias, srcValues, table, colName, pars);
                    var col = table.GetColumn(colName);
                    newRow[col.Index] = value is DBNull ? null : value;
                }

                table.Add(newRow);
                affected++;
            }
        }

        return affected;
    }

    private static int FindRowIndex(TableMock table, int columnIndex, object? value)
    {
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
        => raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();

    private static string ExtractParenthesized(string sql, int startIndex)
    {
        if (startIndex < 0 || startIndex >= sql.Length || sql[startIndex] != '(')
            throw new InvalidOperationException("MERGE: não foi possível ler a subconsulta USING.");

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
            throw new InvalidOperationException("MERGE: parênteses não fechados na cláusula USING.");

        return sql[(startIndex + 1)..(end - 1)].Trim();
    }
}
