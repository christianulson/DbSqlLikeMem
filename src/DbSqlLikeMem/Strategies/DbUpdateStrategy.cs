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
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars);
    }

    private static DmlExecutionResult Execute(
        DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection? pars)
    {
        var sw = Stopwatch.StartNew();
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        var queryTable = query.Table;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(queryTable!.Name, nameof(query.Table.Name));
        var tableName = queryTable.Name!;
        var dialect = connection.ExecutionDialect;
        if (!connection.TryGetTable(tableName, out var table, queryTable.DbName) || table == null)
            throw SqlUnsupported.ForTableDoesNotExist(tableName);
        var rowCountBefore = table.Count;

        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);
        var setPairs = query.Set.Select(s => (s.Col, Val: s.ExprRaw)).ToArray();

        int updated = 0;
        var tableMock = (TableMock)table;
        var affectedIndexes = new List<int>();
        var affectedRowsData = new List<IReadOnlyDictionary<int, object?>>();
        foreach (var rowIdx in GetCandidateRowIndexes(table, pars, conditions))
        {
            var row = table[rowIdx];
            if (!TableMock.IsMatchSimple(table, pars, conditions, row)) continue;

            var oldSnapshot = TableMock.SnapshotRow(row);

            // Valida Unique Constraints antes de aplicar
            var changedCols = setPairs.Select(sp => sp.Col).ToList();
            ValidateUniqueBeforeUpdate(tableName, tableMock, pars, setPairs, rowIdx, row, changedCols, dialect);

            // Aplica Update
            var simulated = row.ToDictionary(_ => _.Key, _ => _.Value);
            UpdateRowValuesInMemory(table, pars, setPairs, simulated, dialect);
            tableMock.ValidateForeignKeysOnRow(new ReadOnlyDictionary<int, object?>(simulated));
            
            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate))
                TryExecuteTableTrigger(connection, dialect, table, tableName, queryTable.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, simulated);
            
            UpdateRowValues(table, pars, setPairs, rowIdx, row, dialect);
            
            if (dialect.SupportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate))
                TryExecuteTableTrigger(connection, dialect, table, tableName, queryTable.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, TableMock.SnapshotRow(table[rowIdx]));

            // Atualiza índices
            tableMock.UpdateIndexesWithRow(rowIdx, oldSnapshot, table[rowIdx]);
            updated++;
            affectedIndexes.Add(rowIdx);
            if (connection.CaptureAffectedRowSnapshots)
                affectedRowsData.Add(TableMock.SnapshotRow(table[rowIdx]));
        }

        connection.Metrics.Updates += updated;
        sw.Stop();

        var metrics = new SqlPlanRuntimeMetrics(
            InputTables: 1,
            EstimatedRowsRead: rowCountBefore,
            ActualRows: updated,
            ElapsedMs: sw.ElapsedMilliseconds);
        if (connection.Db.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatUpdate(
                query,
                metrics,
                new SqlPlanMockRuntimeContext(connection.SimulatedLatencyMs, connection.DropProbability, connection.Db.ThreadSafe));
            connection.RegisterExecutionPlan(plan);
        }
        return new DmlExecutionResult
        {
            AffectedRows= updated,
            AffectedIndexes = affectedIndexes,
            AffectedRowsData = affectedRowsData
        };
    }

    private static IEnumerable<int> GetCandidateRowIndexes(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions)
    {
        if (conditions.Count > 0
            && TableMock.TryFindRowByPkConditions(table, pars, conditions, out var rowIndex))
        {
            yield return rowIndex;
            yield break;
        }

        for (int rowIdx = 0; rowIdx < table.Count; rowIdx++)
            yield return rowIdx;
    }

    private static void ValidateUniqueBeforeUpdate(
        string tableName,
        TableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        int rowIdx,
        IReadOnlyDictionary<int, object?> row,
        List<string> changedCols,
        ISqlDialect dialect)
    {
        var simulated = row.ToDictionary(_=>_.Key, _=>_.Value);
        UpdateRowValuesInMemory(table, pars, setPairs, simulated, dialect); 
        table.EnsureUniqueBeforeUpdate(tableName, row, simulated, rowIdx, changedCols);
    }

    private static void UpdateRowValues(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        int rowIdx,
        IReadOnlyDictionary<int, object?> row,
        ISqlDialect dialect)
    {
        foreach (var (Col, Val) in setPairs)
        {
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue; 

            var raw = ResolveSetValue(table, pars, row, info, Col, Val, dialect);
            table.UpdateRowColumn(rowIdx, info.Index, raw);
        }
    }

    private static void UpdateRowValuesInMemory(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        IDictionary<int, object?> row,
        ISqlDialect dialect)
    {
        foreach (var (Col, Val) in setPairs)
        {
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue; 
            var raw = ResolveSetValue(table, pars, new ReadOnlyDictionary<int, object?>(row), info, Col, Val, dialect);
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
        ISqlDialect dialect)
    {
        table.CurrentColumn = colName;
        try
        {
            if (TryEvalArithmeticSetValue(exprRaw, table, row, pars, info.DbType, info.Nullable, out var arith))
                return arith;

            if (TryResolveTemporalSetValue(exprRaw, dialect, out var temporalValue))
                return temporalValue;

            var raw = table.Resolve(exprRaw, info.DbType, info.Nullable, pars, table.Columns);
            return raw is DBNull ? null : raw;
        }
        finally
        {
            table.CurrentColumn = null;
        }
    }

    private static bool TryResolveTemporalSetValue(string exprRaw, ISqlDialect dialect, out object? value)
    {
        var trimmed = exprRaw.Trim();
        if (SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, trimmed, out value))
            return true;

        var openParen = trimmed.IndexOf('(');
        var closeParen = trimmed.LastIndexOf(')');
        if (openParen <= 0 || closeParen <= openParen)
            return false;

        var functionName = trimmed[..openParen].Trim();
        var argsRaw = trimmed[(openParen + 1)..closeParen].Trim();
        if (argsRaw.Length > 0)
            return false;

        return SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, functionName, out value);
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
