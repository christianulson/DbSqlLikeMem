using System.Collections.ObjectModel;

namespace DbSqlLikeMem;

internal static class DbUpdateStrategy
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteUpdate(
        this DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection? pars = null)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars);
    }

    private static int Execute(
        DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection? pars)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
            ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query.Table!.Name, nameof(query.Table.Name));
        var tableName = query.Table.Name!;
        var dialect = connection.Db.Dialect;
        if (!connection.TryGetTable(tableName, out var table, query.Table?.DbName) || table == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // JOIN updates ainda não suportados plenamente no Parser simples, 
        // mas se o AST viesse com UpdateFromSelect, trataríamos aqui.
        // O Parser atual foca em UPDATE simples com WHERE.

        // 1. Parse do WHERE (raw string do AST)
        // Em alguns cenários (ex: camadas que reescrevem SQL/parametrização), o parser pode
        // acabar não preenchendo WhereRaw. Como fallback, extraímos do RawSql.
        var whereRaw = TableMock.ResolveWhereRaw(query.WhereRaw, query.RawSql);
        var conditions = TableMock.ParseWhereSimple(whereRaw);

        // 2. Prepara os SET pairs do AST
        var setPairs = query.Set.Select(s => (s.Col, Val: s.ExprRaw)).ToArray();

        int updated = 0;
        var tableMock = (TableMock)table;
        for (int rowIdx = 0; rowIdx < table.Count; rowIdx++)
        {
            var row = table[rowIdx];

            // Match Where
            if (!TableMock.IsMatchSimple(table, pars, conditions, row)) continue;

            var oldSnapshot = SnapshotRow(row);

            // Valida Unique Constraints antes de aplicar
            var changedCols = setPairs.Select(sp => sp.Col).ToList();
            ValidateUniqueBeforeUpdate(tableName, tableMock, pars, setPairs, rowIdx, row, changedCols);

            // Aplica Update
            var simulated = row.ToDictionary(_ => _.Key, _ => _.Value);
            UpdateRowValuesInMemory(table, pars, setPairs, simulated);
            TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, SnapshotRow(new System.Collections.ObjectModel.ReadOnlyDictionary<int, object?>(simulated)));
            UpdateRowValues(table, pars, setPairs, rowIdx, row);
            TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, SnapshotRow(table[rowIdx]));

            // Atualiza índices
            table.UpdateIndexesWithRow(rowIdx);
            updated++;
        }

        connection.Metrics.Updates += updated;
        return updated;
    }

    // --- Helpers de Lógica ---

    private static void ValidateUniqueBeforeUpdate(
        string tableName,
        TableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        int rowIdx,
        IReadOnlyDictionary<int, object?> row,
        List<string> changedCols)
    {
        // Simula linha nova sem mutar a tabela
        var simulated = row.ToDictionary(_=>_.Key, _=>_.Value);
        UpdateRowValuesInMemory(table, pars, setPairs, simulated); // aplica na simulação

        table.EnsureUniqueBeforeUpdate(tableName, row, simulated, rowIdx, changedCols);
    }

    private static void UpdateRowValues(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        int rowIdx,
        IReadOnlyDictionary<int, object?> row)
    {
        foreach (var (Col, Val) in setPairs)
        {
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue; // Coluna gerada não se update

            var raw = ResolveSetValue(table, pars, row, info, Col, Val);
            table.UpdateRowColumn(rowIdx, info.Index, raw);
        }
    }

    private static void UpdateRowValuesInMemory(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        IDictionary<int, object?> row)
    {
        foreach (var (Col, Val) in setPairs)
        {
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue; // Coluna gerada não se update
            var raw = ResolveSetValue(table, pars, new ReadOnlyDictionary<int, object?>(row), info, Col, Val);
            row[info.Index] = raw;
        }
    }

    private static object? ResolveSetValue(
        ITableMock table,
        DbParameterCollection? pars,
        IReadOnlyDictionary<int, object?> row,
        ColumnDef info,
        string colName,
        string exprRaw)
    {
        table.CurrentColumn = colName;
        try
        {
            if (TryEvalArithmeticSetValue(exprRaw, table, row, pars, info.DbType, info.Nullable, out var arith))
                return arith;

            var raw = table.Resolve(exprRaw, info.DbType, info.Nullable, pars, table.Columns);
            return raw is DBNull ? null : raw;
        }
        finally
        {
            table.CurrentColumn = null;
        }
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

        // We only support very simple arithmetic used in UPDATE SET, e.g.:
        //   col = col + 1
        //   col = col - @p
        // This is enough to cover common "increment" patterns.
        var expr = exprRaw.Trim();

        // Find + or - at top-level (ignore quoted strings).
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
                // avoid treating leading sign as an operator: "-1"
                // and avoid "e-3" inside scientific notation (rare here).
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

        // MySQL: arithmetic with NULL yields NULL
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
                    // unsupported type for arithmetic
                    return false;
            }
        }
        catch
        {
            // fall back to normal Resolve (will likely throw with a clearer message)
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
        // Column reference
        if (table.Columns.TryGetValue(token, out var col))
        {
            return row.TryGetValue(col.Index, out var v) ? v : null;
        }

        // Quoted identifiers like `counter`
        var unquoted = token.Trim();
        if (unquoted.Length >= 2 && (unquoted[0] == '`' && unquoted[^1] == '`'))
        {
            var name = unquoted[1..^1];
            if (table.Columns.TryGetValue(name, out var col2))
                return row.TryGetValue(col2.Index, out var v2) ? v2 : null;
        }

        // Literal or parameter
        table.CurrentColumn = null;
        var raw = table.Resolve(token, dbType, isNullable, pars, table.Columns);
        return raw is DBNull ? null : raw;
    }


    private static IReadOnlyDictionary<int, object?> SnapshotRow(IReadOnlyDictionary<int, object?> row)
        => row.ToDictionary(_ => _.Key, _ => _.Value);

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
