using System.Collections.ObjectModel;

namespace DbSqlLikeMem;

internal static class DbUpdateStrategy
{
    /// <summary>
    /// EN: Implements ExecuteUpdate.
    /// PT: Implementa ExecuteUpdate.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdate(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection? pars = null)
        => connection.ExecuteUpdate(query, QueryExecutionContext.FromConnection(connection, pars!));

    /// <summary>
    /// EN: Implements ExecuteUpdate using a pre-built execution context.
    /// PT: Implementa ExecuteUpdate usando um contexto de execução pré-construído.
    /// </summary>
    public static DmlExecutionResult ExecuteUpdate(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        QueryExecutionContext context)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(context, query);
        lock (connection.Db.SyncRoot)
            return Execute(context, query);
    }

    private static DmlExecutionResult Execute(
        QueryExecutionContext context,
        SqlUpdateQuery query)
    {
        var connection = context.Connection;
        var pars = context.DbParameters;
        var capturePlans = context.CaptureExecutionPlans;
        var sw = capturePlans ? Stopwatch.StartNew() : null;
        var metricsEnabled = context.MetricsEnabled;
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        var queryTable = query.Table;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(queryTable!.Name, nameof(query.Table.Name));
        var tableName = queryTable.Name!;
        var dialect = context.Dialect;
        if (!connection.TryGetTable(tableName, out var table, queryTable.DbName) || table == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);
        var rowCountBefore = table.Count;

        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);
        var setPairs = new (string Col, string Val)[query.Set.Count];
        for (var i = 0; i < query.Set.Count; i++)
            setPairs[i] = (query.Set[i].Col, query.Set[i].ExprRaw);
        var changedCols = new string[setPairs.Length];
        for (var i = 0; i < setPairs.Length; i++)
            changedCols[i] = setPairs[i].Col;

        int updated = 0;
        var tableMock = (TableMock)table;
        var affectedIndexes = new List<int>();
        var parsedSetPairs = query.SetParsed;
        var supportsTriggers = dialect.SupportsTriggers;
        var hasBeforeUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate);
        var hasAfterUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate);
        var requiresOldSnapshotForIndex = HasIndexedKeyChanges(tableMock, changedCols);
        var requiresUniqueValidation = HasUniqueKeyChanges(tableMock, changedCols);
        var captureAffectedRowSnapshots = connection.CaptureAffectedRowSnapshots;
        var affectedRowsData = captureAffectedRowSnapshots ? new List<IReadOnlyDictionary<int, object?>>() : null;

        void ProcessRow(int rowIdx)
        {
            var row = table[rowIdx];
            if (!TableMock.IsMatchSimple(table, pars, conditions, row))
                return;

            var oldSnapshot = (hasBeforeUpdateTrigger || hasAfterUpdateTrigger || requiresOldSnapshotForIndex)
                ? TableMock.SnapshotRow(row)
                : null;

            // Simula Update (reaproveitado por validacoes/trigger)
            var simulated = TableMock.CloneRow(row);
            UpdateRowValuesInMemory(table, pars, setPairs, parsedSetPairs, simulated, context);

            // Valida Unique Constraints antes de aplicar (somente quando necessario)
            if (requiresUniqueValidation)
                tableMock.EnsureUniqueBeforeUpdate(tableName, row, simulated, rowIdx, changedCols);

            tableMock.ValidateForeignKeysOnRow(simulated);

            if (hasBeforeUpdateTrigger)
                TryExecuteTableTrigger(connection, dialect, table, tableName, queryTable.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, simulated);

            UpdateRowValues(table, pars, setPairs, parsedSetPairs, rowIdx, row, context);

            if (hasAfterUpdateTrigger)
                TryExecuteTableTrigger(connection, dialect, table, tableName, queryTable.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[rowIdx]));

            // Atualiza índices
            if (requiresOldSnapshotForIndex)
                tableMock.UpdateIndexesWithRow(rowIdx, oldSnapshot, table[rowIdx]);
            else
                tableMock.UpdateIndexesWithRow(rowIdx);
            updated++;
            affectedIndexes.Add(rowIdx);
            if (captureAffectedRowSnapshots)
                affectedRowsData!.Add(TableMock.SnapshotRow(table[rowIdx]));
        }

        if (conditions.Count > 0
            && TableMock.TryFindRowByPkConditions(table, pars, conditions, out var pkRowIndex))
        {
            ProcessRow(pkRowIndex);
        }
        else
        {
            for (var rowIdx = 0; rowIdx < table.Count; rowIdx++)
                ProcessRow(rowIdx);
        }

        if (metricsEnabled)
            connection.Metrics.Updates += updated;

        if (capturePlans)
        {
            sw!.Stop();
            var metrics = new SqlPlanRuntimeMetrics(
                InputTables: 1,
                EstimatedRowsRead: rowCountBefore,
                ActualRows: updated,
                ElapsedMs: sw.ElapsedMilliseconds);
            var plan = SqlExecutionPlanFormatter.FormatUpdate(
                query,
                metrics,
                new SqlPlanMockRuntimeContext(connection.SimulatedLatencyMs, connection.DropProbability, connection.Db.ThreadSafe));
            connection.RegisterExecutionPlan(plan);
        }
        return new DmlExecutionResult
        {
            AffectedRows = updated,
            AffectedIndexes = affectedIndexes,
            AffectedRowsData = captureAffectedRowSnapshots ? affectedRowsData! : new List<IReadOnlyDictionary<int, object?>>()
        };
    }
    private static bool HasIndexedKeyChanges(ITableMock table, IReadOnlyList<string> changedCols)
    {
        var changedCount = changedCols.Count;
        if (changedCount == 0)
            return false;

        if (table is TableMock tableMock)
        {
            foreach (var pkIndex in tableMock.PkIndexArray)
            {
                if (tableMock.ColumnsByIndex.TryGetValue(pkIndex, out var pkColumnName)
                    && ContainsChangedColumn(changedCols, pkColumnName))
                {
                    return true;
                }
            }

            foreach (var index in tableMock.Indexes.Values)
            {
                foreach (var keyCol in index.KeyCols)
                {
                    if (ContainsChangedColumn(changedCols, keyCol))
                        return true;
                }
            }

            return false;
        }

        foreach (var pkIndex in table.PrimaryKeyIndexes)
        {
            foreach (var column in table.Columns.Values)
            {
                if (column.Index == pkIndex && ContainsChangedColumn(changedCols, column.Name))
                    return true;
            }
        }

        foreach (var index in table.Indexes.Values)
        {
            foreach (var keyCol in index.KeyCols)
            {
                if (ContainsChangedColumn(changedCols, keyCol))
                    return true;
            }
        }

        return false;
    }

    private static bool HasUniqueKeyChanges(TableMock table, IReadOnlyList<string> changedCols)
    {
        if (changedCols.Count == 0)
            return false;

        foreach (var index in table.Indexes.Values)
        {
            if (!index.Unique)
                continue;

            foreach (var keyCol in index.KeyCols)
            {
                if (ContainsChangedColumn(changedCols, keyCol))
                    return true;
            }
        }

        return false;
    }

    private static bool ContainsChangedColumn(IReadOnlyList<string> changedCols, string columnName)
    {
        for (var i = 0; i < changedCols.Count; i++)
        {
            if (string.Equals(changedCols[i], columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static void UpdateRowValues(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        IReadOnlyList<SqlAssignment> parsedSetPairs,
        int rowIdx,
        IReadOnlyDictionary<int, object?> row,
        QueryExecutionContext context)
    {
        for (var i = 0; i < setPairs.Length; i++)
        {
            var (Col, Val) = setPairs[i];
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue;

            var parsedExpr = i < parsedSetPairs.Count ? parsedSetPairs[i].ValueExpr : null;
            var raw = ResolveSetValue(table, pars, row, info, Col, Val, parsedExpr, context);
            table.UpdateRowColumn(rowIdx, info.Index, raw);
        }
    }

    private static void UpdateRowValuesInMemory(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        IReadOnlyList<SqlAssignment> parsedSetPairs,
        IDictionary<int, object?> row,
        QueryExecutionContext context)
    {
        var readOnlyRow = row as IReadOnlyDictionary<int, object?> ?? new ReadOnlyDictionary<int, object?>(row);
        for (var i = 0; i < setPairs.Length; i++)
        {
            var (Col, Val) = setPairs[i];
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue;
            var parsedExpr = i < parsedSetPairs.Count ? parsedSetPairs[i].ValueExpr : null;
            var raw = ResolveSetValue(table, pars, readOnlyRow, info, Col, Val, parsedExpr, context);
            row[info.Index] = raw;
        }
    }

    private static object? ResolveSetValue(
        ITableMock table,
        DbParameterCollection? pars,
        IReadOnlyDictionary<int, object?> row,
        ColumnDef info,
        string colName,
        string exprRaw,
        SqlExpr? parsedExpr,
        QueryExecutionContext context)
    {
        table.CurrentColumn = colName;
        try
        {
            if (TryResolveCastAsJsonValue(parsedExpr, pars, out var castJsonValue))
                return castJsonValue;

            if (TryEvalArithmeticSetValue(exprRaw, table, row, pars, info.DbType, info.Nullable, out var arith))
                return arith;

            if (TryResolveTemporalSetValue(exprRaw, context, out var temporalValue))
                return temporalValue;

            var raw = table.Resolve(exprRaw, info.DbType, info.Nullable, pars, table.Columns);
            return raw is DBNull ? null : raw;
        }
        finally
        {
            table.CurrentColumn = null;
        }
    }

    private static bool TryResolveCastAsJsonValue(
        SqlExpr? parsedExpr,
        DbParameterCollection? pars,
        out object? value)
    {
        value = null;
        if (parsedExpr is not CallExpr cast
            || !cast.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || cast.Args.Count < 2
            || !IsJsonCastType(cast.Args[1]))
            return false;

        if (!TryResolveCastOperandValue(cast.Args[0], pars, out var operand))
            return false;

        value = NormalizeJsonCastValue(operand);
        return true;
    }

    private static bool IsJsonCastType(SqlExpr expr)
    {
        if (expr is RawSqlExpr raw)
        {
            var sqlType = raw.Sql.Trim();
            return sqlType.Equals("JSON", StringComparison.OrdinalIgnoreCase)
                || sqlType.Equals("JSONB", StringComparison.OrdinalIgnoreCase);
        }

        if (expr is LiteralExpr { Value: string s })
        {
            var sqlType = s.Trim();
            return sqlType.Equals("JSON", StringComparison.OrdinalIgnoreCase)
                || sqlType.Equals("JSONB", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool TryResolveCastOperandValue(
        SqlExpr expr,
        DbParameterCollection? pars,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                value = lit.Value;
                return true;
            case ParameterExpr p:
                return TryResolveParameterValue(pars, p.Name, out value);
            default:
                value = null;
                return false;
        }
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

    private static object? NormalizeJsonCastValue(object? operand)
    {
        if (operand is null || operand is DBNull)
            return null;

        if (operand is System.Text.Json.JsonDocument || operand is System.Text.Json.JsonElement)
            return operand;

        if (operand is string)
            return operand;

        return System.Text.Json.JsonSerializer.Serialize(operand);
    }

    private static bool TryResolveTemporalSetValue(string exprRaw, QueryExecutionContext context, out object? value)
    {
        var trimmed = exprRaw.Trim();
        if (context.TryEvaluateZeroArgIdentifier(trimmed, out value))
            return true;

        var openParen = trimmed.IndexOf('(');
        var closeParen = trimmed.LastIndexOf(')');
        if (openParen <= 0 || closeParen <= openParen)
            return false;

        var functionName = trimmed[..openParen].Trim();
        var argsRaw = trimmed[(openParen + 1)..closeParen].Trim();
        if (argsRaw.Length > 0)
            return false;

        return context.TryEvaluateZeroArgCall(functionName, out value);
    }

    private static bool TryEvalArithmeticSetValue(
        string exprRaw,
        ITableMock table,
        IReadOnlyDictionary<int, object?> row,
        DbParameterCollection? pars,
        DbType dbType,
        bool isNullable,
        out object? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(exprRaw))
            return false;

        var expr = exprRaw.Trim();
        int opIdx = -1;
        char op = '\0';
        bool inSingle = false;
        bool inDouble = false;

        for (int i = 0; i < expr.Length; i++)
        {
            var ch = expr[i];
            if (ch == '\'' && !inDouble) inSingle = !inSingle;
            else if (ch == '"' && !inSingle) inDouble = !inDouble;

            if (inSingle || inDouble) continue;

            if (ch == '+' || ch == '-')
            {
                if (i == 0) continue;
                opIdx = i;
                op = ch;
                break;
            }
        }

        if (opIdx < 0)
            return false;

        var leftTok = expr[..opIdx].Trim();
        var rightTok = expr[(opIdx + 1)..].Trim();
        if (leftTok.Length == 0 || rightTok.Length == 0)
            return false;

        object? leftVal = ResolveOperand(leftTok, table, row, pars, dbType, isNullable);
        object? rightVal = ResolveOperand(rightTok, table, row, pars, dbType, isNullable);

        if (leftVal is null || leftVal is DBNull || rightVal is null || rightVal is DBNull)
        {
            value = null;
            return true;
        }

        try
        {
            switch (dbType)
            {
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.Byte:
                case DbType.SByte:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                    var li = Convert.ToInt64(leftVal);
                    var ri = Convert.ToInt64(rightVal);
                    var resI = op == '+' ? li + ri : li - ri;
                    value = dbType switch
                    {
                        DbType.Int16 => (short)resI,
                        DbType.Int32 => (int)resI,
                        DbType.Byte => (byte)resI,
                        DbType.SByte => (sbyte)resI,
                        DbType.UInt16 => (ushort)resI,
                        DbType.UInt32 => (uint)resI,
                        DbType.UInt64 => (ulong)resI,
                        _ => resI
                    };
                    return true;

                case DbType.Decimal:
                case DbType.Currency:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    var ld = Convert.ToDecimal(leftVal);
                    var rd = Convert.ToDecimal(rightVal);
                    value = op == '+' ? ld + rd : ld - rd;
                    return true;

                default:
                    return false;
            }
        }
        catch
        {
            return false;
        }
    }

    private static object? ResolveOperand(
        string token,
        ITableMock table,
        IReadOnlyDictionary<int, object?> row,
        DbParameterCollection? pars,
        DbType dbType,
        bool isNullable)
    {
        if (table.Columns.TryGetValue(token, out var col))
            return row.TryGetValue(col.Index, out var v) ? v : null;

        var unquoted = token.Trim();
        if (unquoted.Length >= 2 && (unquoted[0] == '`' && unquoted[^1] == '`'))
        {
            var name = unquoted[1..^1];
            if (table.Columns.TryGetValue(name, out var col2))
                return row.TryGetValue(col2.Index, out var v2) ? v2 : null;
        }

        table.CurrentColumn = null;
        var raw = table.Resolve(token, dbType, isNullable, pars, table.Columns);
        return raw is DBNull ? null : raw;
    }

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
}
