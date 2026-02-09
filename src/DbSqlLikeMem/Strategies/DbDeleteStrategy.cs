namespace DbSqlLikeMem;

internal static class DbDeleteStrategy
{
    public static int ExecuteDelete(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars = null)
    {
        if (!connection.Db.ThreadSafe)
            return Execute(connection, query, pars);
        lock (connection.Db.SyncRoot)
            return Execute(connection, query, pars);
    }

    private static int Execute(
        this DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection? pars)
    {
        ArgumentNullException.ThrowIfNull(query.Table);
        ArgumentException.ThrowIfNullOrWhiteSpace(query.Table.Name);
        var tableName = query.Table.Name;
        if (!connection.TryGetTable(tableName, out var table, query.Table.DbName)
            || table == null)
            throw new InvalidOperationException($"Table {tableName} does not exist.");

        // 1. Filtrar linhas
        // Usa a mesma lógica simplificada de WHERE do UpdateStrategy.
        // Fallback: se WhereRaw não foi preenchido, extraímos do RawSql.
        var whereRaw = query.WhereRaw;
        if (string.IsNullOrWhiteSpace(whereRaw) && !string.IsNullOrWhiteSpace(query.RawSql))
        {
            whereRaw = TryExtractWhereRaw(query.RawSql);
        }

        var conditions = ParseWhereSimple(whereRaw);

        var rowsToDelete = new List<IReadOnlyDictionary<int, object?>>();
        var indexesToDelete = new List<int>();

        for (int i = 0; i < table.Count; i++)
        {
            var row = table[i];
            if (IsMatch(table, pars, conditions, row))
            {
                rowsToDelete.Add(row);
                indexesToDelete.Add(i);
            }
        }

        // 2. FK Validation
        ValidateForeignKeys(connection, tableName, table, rowsToDelete, query.Table.DbName);

        // 3. Remover (ordem inversa para não quebrar índices durante loop)
        foreach (var idx in indexesToDelete.OrderByDescending(x => x))
        {
            table.RemoveAt(idx);
        }

        // Rebuild índices (necessário pois posições mudaram)
        table.RebuildAllIndexes();

        connection.Metrics.Deletes += rowsToDelete.Count;
        return rowsToDelete.Count;
    }

    private static void ValidateForeignKeys(
        this DbConnectionMockBase connection,
        string tableName,
        ITableMock table,
        List<IReadOnlyDictionary<int, object?>> rowsToDelete,
        string? dbName)
    {
        // Verifica se alguma tabela filha referencia as linhas deletadas
        foreach (var parentRow in rowsToDelete)
        {
            foreach (var childTable in connection.ListTables(dbName))
            {
                foreach (var (Col, _, RefCol) in childTable.ForeignKeys.Where(f => f.RefTable.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                {
                    var parentInfo = table.GetColumn(RefCol);
                    var childInfo = childTable.GetColumn(Col);

                    var keyVal = parentRow[parentInfo.Index];

                    // Se encontrar qualquer linha na filha com esse valor de FK
                    if (childTable.Any(childRow => Equals(childRow[childInfo.Index], keyVal)))
                    {
                        throw table.ReferencedRow(tableName);
                    }
                }
            }
        }
    }

    // Reutilizando lógica do UpdateStrategy para consistência
    private static bool IsMatch(
        ITableMock table,
        DbParameterCollection? p,
        List<(string C, string Op, string V)> c,
        IReadOnlyDictionary<int, object?> r)
    => c.All(cond =>
    // WHERE simples com suporte a "=" e "IN" (inclusive IN @ids com lista/array)
        {
            var info = table.GetColumn(cond.C);
            var actual = info.GetGenValue != null ? info.GetGenValue(r, table) : r[info.Index];

            if (cond.Op.Equals("=", StringComparison.OrdinalIgnoreCase))
            {
                table.CurrentColumn = cond.C;
                var exp = table.Resolve(cond.V, info.DbType, info.Nullable, p, table.Columns);
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
                        var val = table.Resolve(part, info.DbType, info.Nullable, p, table.Columns);
                        table.CurrentColumn = null;

                        val = val is DBNull ? null : val;

                        // Dapper-style: IN (@ids) onde @ids é IEnumerable.
                        // O SqlQueryParser tende a reconstruir como IN(@ids) (sem espaço),
                        // e o usuário pode envolver em parênteses. Precisamos expandir aqui também.
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
                    var resolved = table.Resolve(rhs, info.DbType, info.Nullable, p, table.Columns);
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

        // Defensive: some layers may include the keyword.
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
            var min = Regex.Match(s, @"^(?<c>[\w`\.]+)\s+IN\s+(?<v>.+)$", RegexOptions.IgnoreCase);
            if (min.Success)
            {
                list.Add((min.Groups["c"].Value.Trim('`', ' '), "IN", min.Groups["v"].Value.Trim()));
                continue;
            }

            // =
            var kv = s.Split('=', 2);
            if (kv.Length == 2)
                list.Add((kv[0].Trim('`', ' '), "=", kv[1].Trim()));
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