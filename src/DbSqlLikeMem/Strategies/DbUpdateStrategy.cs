namespace DbSqlLikeMem;

internal static class DbUpdateStrategy
{
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
        ArgumentNullException.ThrowIfNull(query.Table);
            ArgumentException.ThrowIfNullOrWhiteSpace(query.Table.Name);
        var tableName = query.Table.Name;
        if (!connection.TryGetTable(tableName, out var table, query.Table?.DbName) || table == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // JOIN updates ainda não suportados plenamente no Parser simples, 
        // mas se o AST viesse com UpdateFromSelect, trataríamos aqui.
        // O Parser atual foca em UPDATE simples com WHERE.

        // 1. Parse do WHERE (raw string do AST)
        // Em alguns cenários (ex: camadas que reescrevem SQL/parametrização), o parser pode
        // acabar não preenchendo WhereRaw. Como fallback, extraímos do RawSql.
        var whereRaw = query.WhereRaw;
        if (string.IsNullOrWhiteSpace(whereRaw) && !string.IsNullOrWhiteSpace(query.RawSql))
        {
            whereRaw = TryExtractWhereRaw(query.RawSql);
        }

        var conditions = ParseWhereSimple(whereRaw);

        // 2. Prepara os SET pairs do AST
        var setPairs = query.Set.Select(s => (s.Col, Val: s.ExprRaw)).ToArray();

        int updated = 0;
        for (int rowIdx = 0; rowIdx < table.Count; rowIdx++)
        {
            var row = table[rowIdx];

            // Match Where
            if (!IsMatch(table, pars, conditions, row)) continue;

            // Valida Unique Constraints antes de aplicar
            var changedCols = setPairs.Select(sp => sp.Col).ToList();
            ValidateUniqueBeforeUpdate(tableName, table, pars, setPairs, rowIdx, row, changedCols);

            // Aplica Update
            UpdateRowValues(table, pars, setPairs, row);

            // Atualiza índices
            table.UpdateIndexesWithRow(rowIdx);
            updated++;
        }

        connection.Metrics.Updates += updated;
        return updated;
    }

    // --- Helpers de Lógica ---

    internal static string BuildIndexKey(
        ITableMock table,
        IndexDef idx,
        Dictionary<int, object?> row)
    {
        return string.Join("|", idx.KeyCols.Select(colName =>
        {
            var ci = table.Columns[colName];
            if (ci.GetGenValue != null)
                return ci.GetGenValue(row, table)?.ToString() ?? "<null>";
            return row.TryGetValue(ci.Index, out var v) ? (v?.ToString() ?? "<null>") : "<null>";
        }));
    }

    private static void ValidateUniqueBeforeUpdate(
        string tableName,
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        int rowIdx,
        Dictionary<int, object?> row,
        List<string> changedCols)
    {
        foreach (var ix in table.Indexes.GetUnique())
        {
            // Se o update não toca nas colunas desse índice, ignora
            if (!ix.KeyCols.Intersect(changedCols, StringComparer.OrdinalIgnoreCase).Any()) continue;

            string oldKey = BuildIndexKey(table, ix, row);

            // Simula linha nova
            var simulated = new Dictionary<int, object?>(row);
            UpdateRowValues(table, pars, setPairs, simulated); // aplica na simulação

            string newKey = BuildIndexKey(table, ix, simulated);

            if (!oldKey.Equals(newKey, StringComparison.Ordinal) &&
                table.Lookup(ix, newKey)?.Any(i => i != rowIdx) == true)
            {
                throw table.DuplicateKey(tableName, ix.Name, newKey);
            }
        }
    }

    private static void UpdateRowValues(
        ITableMock table,
        DbParameterCollection? pars,
        (string Col, string Val)[] setPairs,
        Dictionary<int, object?> row)
    {
        foreach (var (Col, Val) in setPairs)
        {
            var info = table.GetColumn(Col);
            if (info.GetGenValue != null) continue; // Coluna gerada não se update

            table.CurrentColumn = Col;

            object? raw;
            if (TryEvalArithmeticSetValue(Val, table, row, pars, info.DbType, info.Nullable, out var arith))
            {
                raw = arith;
            }
            else
            {
                raw = table.Resolve(Val, info.DbType, info.Nullable, pars, table.Columns);
                raw = (raw is DBNull) ? null : raw;
            }

            table.CurrentColumn = null;
            row[info.Index] = raw;
        }
    }


    private static bool TryEvalArithmeticSetValue(
        string exprRaw,
        ITableMock table,
        Dictionary<int, object?> row,
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
        Dictionary<int, object?> row,
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


    private static bool IsMatch(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        Dictionary<int, object?> row)
    => conditions.All(cond =>
        {
            var info = table.GetColumn(cond.C);
            var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];

            if (cond.Op.Equals("=", StringComparison.OrdinalIgnoreCase))
            {
                table.CurrentColumn = cond.C;
                var exp = table.Resolve(cond.V, info.DbType, info.Nullable, pars, table.Columns);
                table.CurrentColumn = null;
                return Equals(actual, exp is DBNull ? null : exp);
            }

            if (cond.Op.Equals("IN", StringComparison.OrdinalIgnoreCase))
            {
                var rhs = cond.V.Trim();

                IEnumerable<object?> candidates;

                if (rhs.StartsWith("(", StringComparison.Ordinal) && rhs.EndsWith(")", StringComparison.Ordinal))
                {
                    var inner = rhs[1..^1];
                    var parts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                    var tmp = new List<object?>();
                    foreach (var part in parts)
                    {
                        table.CurrentColumn = cond.C;
                        var val = table.Resolve(part, info.DbType, info.Nullable, pars, table.Columns);
                        table.CurrentColumn = null;
                        val = val is DBNull ? null : val;

                        // Dapper-style: IN (@ids) onde @ids é IEnumerable.
                        // Mesmo estando dentro de parênteses, queremos expandir a lista.
                        if (val is System.Collections.IEnumerable ie && val is not string)
                        {
                            foreach (var v in ie) tmp.Add(v);
                        }
                        else
                        {
                            tmp.Add(val);
                        }
                    }
                    candidates = tmp;
                }
                else
                {
                    table.CurrentColumn = cond.C;
                    var resolved = table.Resolve(rhs, info.DbType, info.Nullable, pars, table.Columns);
                    table.CurrentColumn = null;

                    resolved = resolved is DBNull ? null : resolved;

                    if (resolved is System.Collections.IEnumerable ie && resolved is not string)
                    {
                        var tmp = new List<object?>();
                        foreach (var v in ie) tmp.Add(v);
                        candidates = tmp;
                    }
                    else
                    {
                        candidates = new object?[] { resolved };
                    }
                }

                foreach (var cand in candidates)
                {
                    if (Equals(actual, cand))
                        return true;
                }

                return false;
            }

            return false;
        });

    private static List<(string C, string Op, string V)> ParseWhereSimple(string? w)
    {
        var list = new List<(string C, string Op, string V)>();
        if (string.IsNullOrWhiteSpace(w)) return list;

        // Defensive: some call sites may include the keyword.
        w = w.Trim();
        if (w.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
            w = w[6..].Trim();

        // Split por AND (case-insensitive) - evita bugs do tipo "aNd".
        // (Sem parênteses) - suficiente para os testes atuais.
        var parts = Regex.Split(w, @"\s+AND\s+", RegexOptions.IgnoreCase)
            .Where(p => !string.IsNullOrWhiteSpace(p));

        foreach (var p in parts)
        {
            var s = p.Trim();

            // IN
            // NOTE: o SqlQueryParser reconstrói tokens sem espaço antes de '(' (ex: "IN(@ids)").
            // Então o matcher precisa aceitar tanto "IN (@ids)" quanto "IN(@ids)".
            var min = Regex.Match(s, @"^(?<c>[\w`\.]+)\s+IN\s*(?<v>.+)$", RegexOptions.IgnoreCase);
            if (min.Success)
            {
                list.Add((min.Groups["c"].Value.Trim(), "IN", min.Groups["v"].Value.Trim()));
                continue;
            }

            // =
            var kv = s.Split('=', 2);
            if (kv.Length == 2)
            {
                list.Add((kv[0].Trim(), "=", kv[1].Trim()));
            }
        }

        return list;
    }

    private static string? TryExtractWhereRaw(string sql)
    {
        // Very small fallback extractor: take everything after WHERE, stop at ORDER/LIMIT/OFFSET/FETCH/;.
        var norm = sql.NormalizeString();
        var whereIdx = norm.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIdx < 0)
        {
            // handle "...WHERE" without leading space
            whereIdx = norm.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
            if (whereIdx < 0) return null;
        }

        var w = norm[(whereIdx + (norm[whereIdx] == ' ' ? 7 : 6))..];
        var stops = new[] { " ORDER ", " LIMIT ", " OFFSET ", " FETCH ", ";" };
        var cut = w.Length;
        foreach (var stop in stops)
        {
            var idx = w.IndexOf(stop, StringComparison.OrdinalIgnoreCase);
            if (idx >= 0) cut = Math.Min(cut, idx);
        }
        return w[..cut].Trim();
    }
}