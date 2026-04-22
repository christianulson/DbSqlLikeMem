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
        => connection.Db.ExecuteWithLock(() => Execute(context, query));

    private static DmlExecutionResult Execute(
        QueryExecutionContext context,
        SqlUpdateQuery query)
    {
        var connection = context.Connection;
        context.ResetPositionalParameterCursor();
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
        var parsedSetPairs = query.SetParsed;
        var changedCols = new string[setPairs.Length];
        for (var i = 0; i < setPairs.Length; i++)
            changedCols[i] = setPairs[i].Col;
        var changedColsLookup = new HashSet<string>(changedCols, StringComparer.OrdinalIgnoreCase);
        var positionalSetParameterCount = CountPositionalParameters(parsedSetPairs);
        var whereHasPositionalParameters = HasPositionalParameters(conditions);
        var whereContextBase = context.Fork();
        whereContextBase.AdvancePositionalParameterCursor(positionalSetParameterCount);
        var setContextBase = context.Fork();
        QueryExecutionContext CreateWhereContext()
            => whereHasPositionalParameters ? whereContextBase.Fork() : whereContextBase;

        QueryExecutionContext CreateSetContext()
            => positionalSetParameterCount > 0 ? setContextBase.Fork() : setContextBase;

        int updated = 0;
        var tableMock = (TableMock)table;
        var affectedIndexes = new List<int>(4);
        var supportsTriggers = dialect.SupportsTriggers;
        var hasBeforeUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.BeforeUpdate);
        var hasAfterUpdateTrigger = supportsTriggers && table.HasTriggers(TableTriggerEvent.AfterUpdate);
        var requiresOldSnapshotForIndex = HasIndexedKeyChanges(tableMock, changedColsLookup);
        var requiresUniqueValidation = HasUniqueKeyChanges(tableMock, changedColsLookup);
        var captureAffectedRowSnapshots = connection.CaptureAffectedRowSnapshots;
        var affectedRowsData = captureAffectedRowSnapshots ? new List<IReadOnlyDictionary<int, object?>>() : null;

        void ProcessMatchedRow(int rowIdx)
        {
            var row = table[rowIdx];
            var simulatedSetContext = CreateSetContext();
            var rowSetContext = CreateSetContext();

            var oldSnapshot = (hasBeforeUpdateTrigger || hasAfterUpdateTrigger || requiresOldSnapshotForIndex)
                ? TableMock.SnapshotRow(row)
                : null;

            // Simula Update (reaproveitado por validacoes/trigger)
            var simulated = TableMock.CloneRow(row);
            UpdateRowValuesInMemory(table, setPairs, parsedSetPairs, simulated, simulatedSetContext);

            // Valida Unique Constraints antes de aplicar (somente quando necessario)
            if (requiresUniqueValidation)
                tableMock.EnsureUniqueBeforeUpdate(tableName, row, simulated, rowIdx, changedCols);

            tableMock.ValidateForeignKeysOnRow(simulated);

            if (hasBeforeUpdateTrigger)
                TryExecuteTableTrigger(connection, dialect, table, tableName, queryTable.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, simulated);

            UpdateRowValues(table, setPairs, parsedSetPairs, rowIdx, row, rowSetContext);

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
            && TableMock.TryFindRowByPkConditions(table, CreateWhereContext(), context.DbParameters, conditions, out var pkRowIndex))
        {
            ProcessMatchedRow(pkRowIndex);
        }
        else
        {
            for (var rowIdx = 0; rowIdx < table.Count; rowIdx++)
            {
                var row = table[rowIdx];
                if (!TableMock.IsMatchSimple(table, CreateWhereContext(), context.DbParameters, conditions, row))
                    continue;

                ProcessMatchedRow(rowIdx);
            }
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

    private static int CountPositionalParameters(IReadOnlyList<SqlAssignment> parsedSetPairs)
    {
        var count = 0;
        for (var i = 0; i < parsedSetPairs.Count; i++)
            count += CountPositionalParameters(parsedSetPairs[i].ValueExpr);

        return count;
    }

    private static int CountPositionalParameters(SqlExpr? expr)
    {
        if (expr is null)
            return 0;

        return expr switch
        {
            ParameterExpr parameter => string.Equals(parameter.Name.Trim(), "?", StringComparison.Ordinal) ? 1 : 0,
            UnaryExpr unary => CountPositionalParameters(unary.Expr),
            BinaryExpr binary => CountPositionalParameters(binary.Left) + CountPositionalParameters(binary.Right),
            InExpr inExpr => CountPositionalParameters(inExpr.Left) + CountPositionalParameters(inExpr.Items),
            LikeExpr likeExpr => CountPositionalParameters(likeExpr.Left)
                + CountPositionalParameters(likeExpr.Pattern)
                + CountPositionalParameters(likeExpr.Escape),
            IsNullExpr isNullExpr => CountPositionalParameters(isNullExpr.Expr),
            SubqueryExpr => 0,
            RowExpr rowExpr => CountPositionalParameters(rowExpr.Items),
            ExistsExpr => 0,
            QuantifiedComparisonExpr quantified => CountPositionalParameters(quantified.Left),
            FunctionCallExpr functionCall => CountPositionalParameters(functionCall.Args),
            JsonAccessExpr jsonAccess => CountPositionalParameters(jsonAccess.Target) + CountPositionalParameters(jsonAccess.Path),
            CallExpr call => CountPositionalParameters(call.Args) + CountPositionalParameters(call.Filter),
            WindowFunctionExpr windowFunction => CountPositionalParameters(windowFunction.Args) + CountPositionalParameters(windowFunction.Spec),
            BetweenExpr between => CountPositionalParameters(between.Expr) + CountPositionalParameters(between.Low) + CountPositionalParameters(between.High),
            CaseExpr caseExpr => CountPositionalParameters(caseExpr.BaseExpr)
                + CountPositionalParameters(caseExpr.Whens)
                + CountPositionalParameters(caseExpr.ElseExpr),
            RawSqlExpr or IdentifierExpr or ColumnExpr or LiteralExpr or StarExpr => 0,
            _ => 0
        };
    }

    private static int CountPositionalParameters(IReadOnlyList<SqlExpr> exprs)
    {
        var count = 0;
        for (var i = 0; i < exprs.Count; i++)
            count += CountPositionalParameters(exprs[i]);

        return count;
    }

    private static int CountPositionalParameters(IReadOnlyList<CaseWhenThen> whenThens)
    {
        var count = 0;
        for (var i = 0; i < whenThens.Count; i++)
            count += CountPositionalParameters(whenThens[i].When) + CountPositionalParameters(whenThens[i].Then);

        return count;
    }

    private static int CountPositionalParameters(WindowSpec spec)
        => CountPositionalParameters(spec.PartitionBy)
            + CountPositionalParameters(spec.OrderBy)
            + CountPositionalParameters(spec.Frame);

    private static int CountPositionalParameters(IReadOnlyList<WindowOrderItem> items)
    {
        var count = 0;
        for (var i = 0; i < items.Count; i++)
            count += CountPositionalParameters(items[i].Expr);

        return count;
    }

    private static int CountPositionalParameters(WindowFrameSpec? frame)
    {
        if (frame is null)
            return 0;

        return CountPositionalParameters(frame.Start) + CountPositionalParameters(frame.End);
    }

    private static int CountPositionalParameters(WindowFrameBound bound)
        => 0;
    private static bool HasPositionalParameters(List<(string C, string Op, string V)> conditions)
    {
        for (var i = 0; i < conditions.Count; i++)
        {
            if (conditions[i].V.IndexOf('?') >= 0)
                return true;
        }

        return false;
    }
    private static bool HasIndexedKeyChanges(ITableMock table, HashSet<string> changedCols)
    {
        var changedCount = changedCols.Count;
        if (changedCount == 0)
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

    private static bool HasUniqueKeyChanges(TableMock table, HashSet<string> changedCols)
    {
        if (changedCols.Count == 0)
            return false;

        foreach (var index in table.UniqueIndexes)
        {
            foreach (var keyCol in index.KeyCols)
            {
                if (changedCols.Contains(keyCol))
                    return true;
            }
        }

        return false;
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

    private static void UpdateRowValues(
        ITableMock table,
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
            var raw = ResolveSetValue(table, row, info, Col, Val, parsedExpr, context);
            table.UpdateRowColumn(rowIdx, info.Index, raw);
        }
    }

    private static void UpdateRowValuesInMemory(
        ITableMock table,
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
            var raw = ResolveSetValue(table, readOnlyRow, info, Col, Val, parsedExpr, context);
            row[info.Index] = raw;
        }
    }

    private static object? ResolveSetValue(
        ITableMock table,
        IReadOnlyDictionary<int, object?> row,
        ColumnDef info,
        string colName,
        string exprRaw,
        SqlExpr? parsedExpr,
        QueryExecutionContext context)
    {
        table.CurrentColumn = colName;
        var trimmedExprRaw = exprRaw;
        if (trimmedExprRaw.Length > 0
            && (char.IsWhiteSpace(trimmedExprRaw[0]) || char.IsWhiteSpace(trimmedExprRaw[^1])))
        {
            trimmedExprRaw = trimmedExprRaw.Trim();
        }

        try
        {
            if (TryResolveCastAsJsonValue(parsedExpr, context, out var castJsonValue))
                return castJsonValue;

            if (TryEvalArithmeticSetValue(trimmedExprRaw, table, row, context, info.DbType, info.Nullable, out var arith))
                return arith;

            if (TryResolveTemporalSetValue(trimmedExprRaw, context, out var temporalValue))
                return temporalValue;

            if (context.TryResolveParameter(trimmedExprRaw, out var parameterValue))
                return parameterValue;

            var raw = table.Resolve(trimmedExprRaw, info.DbType, info.Nullable, context.DbParameters, table.Columns);
            return context.NormalizeResolvedValue(raw is DBNull ? null : raw);
        }
        finally
        {
            table.CurrentColumn = null;
        }
    }

    private static bool TryResolveCastAsJsonValue(
        SqlExpr? parsedExpr,
        QueryExecutionContext context,
        out object? value)
    {
        value = null;
        if (parsedExpr is not CallExpr cast
            || !cast.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || cast.Args.Count < 2
            || !IsJsonCastType(cast.Args[1]))
            return false;

        if (!TryResolveCastOperandValue(cast.Args[0], context, out var operand))
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
        QueryExecutionContext context,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                value = lit.Value;
                return true;
            case ParameterExpr p:
                return context.TryResolveParameter(p.Name, out value);
            default:
                value = null;
                return false;
        }
    }

    private static object? NormalizeJsonCastValue(object? operand)
    {
        if (operand is null || operand is DBNull)
            return null;

        if (operand is System.Text.Json.JsonDocument || operand is System.Text.Json.JsonElement)
            return operand;

        if (operand is string)
            return operand;

        return JsonSerializer.Serialize(operand);
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
        QueryExecutionContext context,
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

        object? leftVal = ResolveOperand(leftTok, table, row, context, dbType, isNullable);
        object? rightVal = ResolveOperand(rightTok, table, row, context, dbType, isNullable);

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
        QueryExecutionContext context,
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
        if (context.TryResolveParameter(token, out var parameterValue))
            return parameterValue;

        var raw = table.Resolve(token, dbType, isNullable, context.DbParameters, table.Columns);
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
