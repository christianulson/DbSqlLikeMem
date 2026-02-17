namespace DbSqlLikeMem;

internal static class DbInsertStrategy
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static int ExecuteInsert(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars, dialect);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars, dialect);
    }

    private static int Execute(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Table, nameof(query.Table));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(query.Table!.Name, nameof(query.Table.Name));

        var tableName = query.Table.Name!; // Nome vindo do Parser
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName) || table == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // Identifica linhas a inserir (seja via VALUES ou SELECT)
        List<Dictionary<int, object?>> newRows;

        if (query.InsertSelect != null)
        {
            // Caso: INSERT INTO ... SELECT ...
            newRows = CreateRowsFromSelect(query, table, connection, pars, dialect);
        }
        else
        {
            // Caso: INSERT INTO ... VALUES ...
            newRows = CreateRowsFromValues(query, table, pars);
        }

        int insertedCount = 0;
        int updatedCount = 0;

        var tableMock = (TableMock)table;

        foreach (var newRow in newRows)
        {
            if (!query.HasOnDuplicateKeyUpdate)
            {
                // Inserção normal
                tableMock.ValidateForeignKeysOnRow(newRow);
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, SnapshotRow(newRow));
                table.Add(newRow);
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, SnapshotRow(table[table.Count - 1]));
                insertedCount++;
                continue;
            }

            // Lógica ON DUPLICATE KEY UPDATE
            var conflictIdx = tableMock.FindConflictingRowIndex(newRow, out _, out _);
            if (conflictIdx is null)
            {
                // Sem conflito -> Insere
                tableMock.ValidateForeignKeysOnRow(newRow);
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeInsert, null, SnapshotRow(newRow));
                table.Add(newRow);
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterInsert, null, SnapshotRow(table[table.Count - 1]));
                insertedCount++;
            }
            else
            {
                // Conflito -> Update
                var oldSnapshot = SnapshotRow(table[conflictIdx.Value]);
                var simulatedUpdated = table[conflictIdx.Value].ToDictionary(_ => _.Key, _ => _.Value);
                ApplyOnDuplicateUpdateAstInMemory(
                    table,
                    conflictIdx.Value,
                    newRow,
                    query.OnDupAssigns,
                    pars,
                    dialect,
                    simulatedUpdated);
                tableMock.ValidateForeignKeysOnRow(new System.Collections.ObjectModel.ReadOnlyDictionary<int, object?>(simulatedUpdated));

                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.BeforeUpdate, oldSnapshot, SnapshotRow(newRow));
                ApplyOnDuplicateUpdateAst(
                    table,
                    conflictIdx.Value,
                    newRow,
                    query.OnDupAssigns,
                    pars,
                    dialect);
                TryExecuteTableTrigger(connection, dialect, table, tableName, query.Table.DbName, TableTriggerEvent.AfterUpdate, oldSnapshot, SnapshotRow(table[conflictIdx.Value]));

                // Rebuild índices afetados (simplificado: rebuild all)
                table.RebuildAllIndexes();
                updatedCount++;
            }
        }

        connection.Metrics.Inserts += insertedCount;
        connection.Metrics.Updates += updatedCount;

        if (string.Equals(dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase))
            return insertedCount + updatedCount;

        // MySQL retorna: 1 para insert, 2 para update em conflito
        return insertedCount + (updatedCount * 2);
    }

    // --- Helpers de Criação de Linhas ---

    private static List<Dictionary<int, object?>> CreateRowsFromValues(
        SqlInsertQuery query,
        ITableMock table,
        DbParameterCollection? pars)
    {
        var rows = new List<Dictionary<int, object?>>();
        var colNames = query.Columns; // Lista de colunas do Insert

        foreach (var valueBlock in query.ValuesRaw) // List<string> (tokens raw)
        {
            // Validação de count
            if (colNames.Count > 0 && colNames.Count != valueBlock.Count)
                throw new InvalidOperationException($"Column count ({colNames.Count}) does not match value count ({valueBlock.Count}).");

            var newRow = new Dictionary<int, object?>();

            // Se colNames estiver vazio, assume ordem das colunas da tabela? 
            // O Mock original assumia que se não tem colunas, tem que bater com tudo ou ser default.
            // Aqui vamos assumir que o Parser preencheu corretamente ou usamos lógica posicional se Columns vazio.

            if (colNames.Count == 0 && valueBlock.Count > 0)
            {
                // Insert implicito: INSERT INTO t VALUES (1, 2)
                // Mapeia por index da tabela
                var tableCols = table.Columns.Values.OrderBy(c => c.Index).ToList();

                // Fallback defensivo: se o parser não trouxe a lista explícita de colunas
                // e a quantidade de valores bate apenas com colunas não-identity,
                // mapeia para as não-identity (caso comum: INSERT INTO t(name) VALUES(...)).
                // Isso evita jogar o primeiro valor em uma identity e deixar colunas obrigatórias nulas.
                var nonIdentityCols = tableCols.Where(c => !c.Identity).ToList();
                var targetCols = valueBlock.Count == tableCols.Count
                    ? tableCols
                    : (valueBlock.Count == nonIdentityCols.Count ? nonIdentityCols : tableCols);

                for (int i = 0; i < valueBlock.Count; i++)
                {
                    if (i >= targetCols.Count) break;
                    SetColValue(table, pars, targetCols[i].Index, valueBlock[i], newRow);
                }
            }
            else if (colNames.Count > 0)
            {
                // Insert explícito: INSERT INTO t (a,b) VALUES (1, 2)
                for (int i = 0; i < colNames.Count; i++)
                {
                    var colInfo = table.GetColumn(colNames[i]);
                    SetColValue(table, pars, colInfo.Index, valueBlock[i], newRow);
                }
            }

            rows.Add(newRow);
        }
        return rows;
    }

    private static List<Dictionary<int, object?>> CreateRowsFromSelect(
        SqlInsertQuery query,
        ITableMock targetTable,
        DbConnectionMockBase connection,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        // Executa o SELECT interno
        var executor = AstQueryExecutorFactory.Create(dialect, connection, pars ?? connection.CreateCommand().Parameters);
        var res = executor.ExecuteSelect(query.InsertSelect!);

        var rows = new List<Dictionary<int, object?>>();
        var colNames = query.Columns;

        if (colNames.Count > 0 && colNames.Count != res.Columns.Count)
            throw new InvalidOperationException("Column count does not match SELECT list.");

        foreach (var srcRow in res)
        {
            var newRow = new Dictionary<int, object?>();

            if (colNames.Count > 0)
            {
                // Mapeamento por nome explícito
                for (int i = 0; i < colNames.Count; i++)
                {
                    var colInfo = targetTable.GetColumn(colNames[i]);
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[colInfo.Index] = (val is DBNull) ? null : val;
                }
            }
            else
            {
                // Mapeamento posicional implícito
                var tableCols = targetTable.Columns.Values.OrderBy(c => c.Index).ToList();
                for (int i = 0; i < res.Columns.Count; i++)
                {
                    if (i >= tableCols.Count) break;
                    var val = srcRow.TryGetValue(i, out var v) ? v : null;
                    newRow[tableCols[i].Index] = (val is DBNull) ? null : val;
                }
            }

            rows.Add(newRow);
        }
        return rows;
    }

    private static void SetColValue(
        ITableMock table,
        DbParameterCollection? pars,
        int colIndex,
        string rawValue,
        Dictionary<int, object?> row)
    {
        // Encontra definição da coluna para saber tipo
        var colDef = table.Columns.Values.First(c => c.Index == colIndex);

        // Resolve valor
        table.CurrentColumn = table.Columns.First(c => c.Value.Index == colIndex).Key;
        var resolved = table.Resolve(rawValue, colDef.DbType, colDef.Nullable, pars, table.Columns);
        table.CurrentColumn = null;

        var val = (resolved is DBNull) ? null : resolved;
        if (val == null && !colDef.Nullable)
            throw table.ColumnCannotBeNull("Idx:" + colIndex);

        row[colIndex] = val;
    }

    // --- Helpers de ON DUPLICATE ---

    private static void ApplyOnDuplicateUpdateAstInMemory(
        ITableMock table,
        int existinIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        IReadOnlyList<(string Col, string ExprRaw)> assigns,
        DbParameterCollection? pars,
        ISqlDialect dialect,
        IDictionary<int, object?> targetRow)
    {
        object? GetParamValue(string rawName)
        {
            if (pars is null) return null;
            var n = rawName.Trim();
            if (n.Length > 0 && (n[0] == '@' || n[0] == ':' || n[0] == '?')) n = n[1..];

            foreach (DbParameter p in pars)
            {
                var pn = p.ParameterName?.TrimStart('@', ':', '?') ?? "";
                if (string.Equals(pn, n, StringComparison.OrdinalIgnoreCase))
                    return p.Value is DBNull ? null : p.Value;
            }
            return null;
        }

        object? GetInsertedColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return insertedRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        object? GetExistingColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return targetRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        static object? Coerce(DbType dbType, object? value)
        {
            if (value is null || value is DBNull) return null;
            try
            {
                return dbType switch
                {
                    DbType.String => value.ToString(),
                    DbType.Int16 => Convert.ToInt16(value),
                    DbType.Int32 => Convert.ToInt32(value),
                    DbType.Int64 => Convert.ToInt64(value),
                    DbType.Byte => Convert.ToByte(value),
                    DbType.Boolean => value is bool b ? b : Convert.ToInt32(value) != 0,
                    DbType.Decimal => Convert.ToDecimal(value),
                    DbType.Double => Convert.ToDouble(value),
                    DbType.Single => Convert.ToSingle(value),
                    DbType.DateTime => value is DateTime dt ? dt : Convert.ToDateTime(value),
                    _ => value
                };
            }
            catch { return value; }
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => GetParamValue(p.Name),
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : GetExistingColumnValue(id.Name.Contains('.') ? id.Name.Split('.').Last() : id.Name),
                ColumnExpr c => string.Equals(c.Qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
                    ? GetInsertedColumnValue(c.Name)
                    : GetExistingColumnValue(c.Name),
                UnaryExpr u when u.Op == SqlUnaryOp.Not => !(Convert.ToBoolean(Eval(u.Expr) ?? false)),
                IsNullExpr n => (Eval(n.Expr) is null) ^ n.Negated,
                BinaryExpr b => b.Op switch
                {
                    SqlBinaryOp.Add => (Convert.ToDecimal(Eval(b.Left) ?? 0m) + Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Subtract => (Convert.ToDecimal(Eval(b.Left) ?? 0m) - Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Multiply => (Convert.ToDecimal(Eval(b.Left) ?? 0m) * Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Divide => (Convert.ToDecimal(Eval(b.Left) ?? 0m) / Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Eq => Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Neq => !Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.And => Convert.ToBoolean(Eval(b.Left) ?? false) && Convert.ToBoolean(Eval(b.Right) ?? false),
                    SqlBinaryOp.Or => Convert.ToBoolean(Eval(b.Left) ?? false) || Convert.ToBoolean(Eval(b.Right) ?? false),
                    _ => throw new NotSupportedException($"Operador não suportado em ON DUPLICATE: {b.Op}")
                },
                _ => throw new NotSupportedException($"Expressão não suportada em ON DUPLICATE: {expr.GetType().Name}")
            };
        }

        bool TryGetExcludedValueFromName(string rawName, out object? val)
        {
            val = null;
            var n = rawName.Trim();
            int dot = n.IndexOf('.');
            if (dot <= 0) return false;

            var qualifier = n[..dot];
            var col = n[(dot + 1)..];

            if (!(string.Equals(qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
               || string.Equals(qualifier, "values", StringComparison.OrdinalIgnoreCase)))
                return false;

            val = GetInsertedColumnValue(col);
            return true;
        }

        foreach (var (col, exprRaw) in assigns)
        {
            var colInfo = table.GetColumn(col);
            if (colInfo.GetGenValue != null) continue;
            var expr = SqlExpressionParser.ParseScalar(exprRaw, dialect);
            var resolved = Eval(expr);
            var coerced = Coerce(colInfo.DbType, resolved);
            targetRow[colInfo.Index] = coerced;
        }
    }

    private static void ApplyOnDuplicateUpdateAst(
        ITableMock table,
        int existinIndex,
        IReadOnlyDictionary<int, object?> insertedRow,
        IReadOnlyList<(string Col, string ExprRaw)> assigns,
        DbParameterCollection? pars,
        ISqlDialect dialect)
    {
        object? GetParamValue(string rawName)
        {
            if (pars is null) return null;
            var n = rawName.Trim();
            // remove common prefixes
            if (n.Length > 0 && (n[0] == '@' || n[0] == ':' || n[0] == '?')) n = n[1..];

            foreach (DbParameter p in pars)
            {
                var pn = p.ParameterName?.TrimStart('@', ':', '?') ?? "";
                if (string.Equals(pn, n, StringComparison.OrdinalIgnoreCase))
                    return p.Value is DBNull ? null : p.Value;
            }
            return null;
        }

        object? GetInsertedColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return insertedRow.TryGetValue(info.Index, out var v) ? v : null;
        }

        object? GetExistingColumnValue(string col)
        {
            var info = table.GetColumn(col);
            return table[existinIndex].TryGetValue(info.Index, out var v) ? v : null;
        }

        static object? Coerce(DbType dbType, object? value)
        {
            if (value is null || value is DBNull) return null;

            try
            {
                return dbType switch
                {
                    DbType.String => value.ToString(),
                    DbType.Int16 => Convert.ToInt16(value),
                    DbType.Int32 => Convert.ToInt32(value),
                    DbType.Int64 => Convert.ToInt64(value),
                    DbType.Byte => Convert.ToByte(value),
                    DbType.Boolean => value is bool b ? b : Convert.ToInt32(value) != 0,
                    DbType.Decimal => Convert.ToDecimal(value),
                    DbType.Double => Convert.ToDouble(value),
                    DbType.Single => Convert.ToSingle(value),
                    DbType.DateTime => value is DateTime dt ? dt : Convert.ToDateTime(value),
                    _ => value
                };
            }
            catch
            {
                // Fallback: se for string e o convert falhou, devolve string (vai falhar mais tarde se realmente precisar)
                return value;
            }
        }

        object? Eval(SqlExpr expr)
        {
            return expr switch
            {
                LiteralExpr lit => lit.Value,
                ParameterExpr p => GetParamValue(p.Name),
                IdentifierExpr id => TryGetExcludedValueFromName(id.Name, out var excluded)
                    ? excluded
                    : GetExistingColumnValue(id.Name.Contains('.') ? id.Name.Split('.').Last() : id.Name),
                ColumnExpr c => string.Equals(c.Qualifier, "excluded", StringComparison.OrdinalIgnoreCase)
                    ? GetInsertedColumnValue(c.Name)
                    : GetExistingColumnValue(c.Name),
                UnaryExpr u when u.Op == SqlUnaryOp.Not => !(Convert.ToBoolean(Eval(u.Expr) ?? false)),
                IsNullExpr n => (Eval(n.Expr) is null) ^ n.Negated,
                BinaryExpr b => b.Op switch
                {
                    SqlBinaryOp.Add => (Convert.ToDecimal(Eval(b.Left) ?? 0m) + Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Subtract => (Convert.ToDecimal(Eval(b.Left) ?? 0m) - Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Multiply => (Convert.ToDecimal(Eval(b.Left) ?? 0m) * Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Divide => (Convert.ToDecimal(Eval(b.Left) ?? 0m) / Convert.ToDecimal(Eval(b.Right) ?? 0m)),
                    SqlBinaryOp.Eq => Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.Neq => !Equals(Eval(b.Left), Eval(b.Right)),
                    SqlBinaryOp.And => Convert.ToBoolean(Eval(b.Left) ?? false) && Convert.ToBoolean(Eval(b.Right) ?? false),
                    SqlBinaryOp.Or => Convert.ToBoolean(Eval(b.Left) ?? false) || Convert.ToBoolean(Eval(b.Right) ?? false),
                    _ => throw new InvalidOperationException($"Operador não suportado no ON DUPLICATE: {b.Op}")
                },
                CallExpr call => EvalCall(call),
                FunctionCallExpr fn => EvalFunction(fn),
                RawSqlExpr raw => throw new InvalidOperationException($"Expressão não suportada no ON DUPLICATE: {raw.Sql}"),
                _ => throw new InvalidOperationException($"Expressão não suportada no ON DUPLICATE: {expr.GetType().Name}")
            };
        }

        bool TryGetExcludedValueFromName(string rawName, out object? value)
        {
            value = null;
            if (string.IsNullOrWhiteSpace(rawName))
                return false;

            var parts = rawName.Split('.').Select(_=>_.Trim()).Take(2).ToArray();
            if (parts.Length == 2 && string.Equals(parts[0], "excluded", StringComparison.OrdinalIgnoreCase))
            {
                value = GetInsertedColumnValue(parts[1]);
                return true;
            }

            return false;
        }

        object? EvalFunction(FunctionCallExpr fn)
        {
            // compat: alguns parsers usam FunctionCallExpr
            var name = fn.Name;
            if (name.Equals("NOW", StringComparison.OrdinalIgnoreCase) ||
                name.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
                return DateTime.UtcNow;

            // se vier algo simples tipo VALUES(...) cair aqui por engano, tenta tratar:
            if (name.Equals("VALUES", StringComparison.OrdinalIgnoreCase) && fn.Args.Count == 1)
            {
                var col = fn.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };
                if (!string.IsNullOrWhiteSpace(col))
                    return GetInsertedColumnValue(col!);
            }

            throw new InvalidOperationException($"Função não suportada no ON DUPLICATE: {fn.Name}()");
        }

        object? EvalCall(CallExpr call)
        {
            var name = call.Name;

            // MySQL: VALUES(col)
            if (name.Equals("VALUES", StringComparison.OrdinalIgnoreCase) && call.Args.Count == 1)
            {
                var col = call.Args[0] switch
                {
                    IdentifierExpr id => id.Name,
                    ColumnExpr c => c.Name,
                    _ => null
                };
                if (string.IsNullOrWhiteSpace(col))
                    throw new InvalidOperationException("VALUES() espera 1 coluna");

                return GetInsertedColumnValue(col!);
            }

            // NOW()
            if ((name.Equals("NOW", StringComparison.OrdinalIgnoreCase) ||
                 name.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) && call.Args.Count == 0)
                return DateTime.UtcNow;

            throw new InvalidOperationException($"CALL não suportado no ON DUPLICATE: {call.Name}");
        }

        foreach (var (colName, exprRaw) in assigns)
        {
            var colInfo = table.GetColumn(colName);

            // Parseia e avalia a expressão (suporta VALUES(col), users.col, col, aritmética)
            var ast = SqlExpressionParser.ParseScalar(exprRaw, dialect);
            var value = Eval(ast);

            table.UpdateRowColumn(
                existinIndex, 
                colInfo.Index, 
                Coerce(colInfo.DbType, value));
        }
    }

    // --- Helpers Genéricos (Mantidos ou Levemente Adaptados) ---

    // Defaults and uniqueness are handled by TableMock.

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
