namespace DbSqlLikeMem;

/// <summary>
/// New query engine that executes the new Pratt-based AST (<see cref="SqlSelectQuery"/>) directly
/// against <see cref="TableMock"/> tables.
///
/// Scope: SELECT/WITH only (as per SqlQueryParser).
/// </summary>
internal abstract class AstQueryExecutorBase(
    DbConnectionMockBase cnn,
    IDataParameterCollection pars,
    object dialect)
    : IAstQueryExecutor
{
    private readonly DbConnectionMockBase _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly IDataParameterCollection _pars = pars ?? throw new ArgumentNullException(nameof(pars));
    private readonly object _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    private ISqlDialect? Dialect => _dialect as ISqlDialect;


    private static readonly HashSet<string> _aggFns = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG"
    };

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // We resolve SqlExpressionParser.ParseWhere(string, <dialectType>) via reflection so the base
    // can be reused by SqlServer/Postgre/Oracle dialects.
    private SqlExpr ParseExpr(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            throw new ArgumentException("Expressão vazia.", nameof(raw));

        var dialectType = _dialect.GetType();
        var mi = typeof(SqlExpressionParser)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .FirstOrDefault(m =>
            {
                if (m.Name != nameof(SqlExpressionParser.ParseWhere)) return false;
                var ps = m.GetParameters();
                if (ps.Length != 2) return false;
                if (ps[0].ParameterType != typeof(string)) return false;
                return ps[1].ParameterType.IsAssignableFrom(dialectType);
            });

        if (mi is null)
            throw new MissingMethodException(
                $"{nameof(SqlExpressionParser)}.{nameof(SqlExpressionParser.ParseWhere)}(string, {dialectType.Name}) não encontrado.");

        try
        {
            return (SqlExpr)mi.Invoke(null, new object[] { raw, _dialect })!;
        }
        catch (System.Reflection.TargetInvocationException tie) when (tie.InnerException is not null)
        {
            throw tie.InnerException;
        }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public TableResultMock ExecuteUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        string? sqlContextForErrors = null)
    {
        if (parts is null || parts.Count == 0)
            throw new InvalidOperationException("UNION: nenhuma query.");

        if (allFlags is null)
            throw new InvalidOperationException("UNION: allFlags null.");

        if (allFlags.Count != Math.Max(0, parts.Count - 1))
            throw new InvalidOperationException($"UNION: allFlags.Count inválido. parts={parts.Count}, allFlags={allFlags.Count}");

        // Executa cada SELECT
        var tables = new List<TableResultMock>(parts.Count);
        foreach (var q in parts)
            tables.Add(ExecuteSelect(q));

        // Base do resultado
        var result = new TableResultMock
        {
            Columns = tables[0].Columns,
            JoinFields = tables[0].JoinFields
        };

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para UNION.");

        // valida colunas compatíveis
        for (int i = 0; i < tables.Count; i++)
        {
            if (tables[i].Columns.Count != result.Columns.Count)
            {
                var msg =
                    $"UNION: número de colunas incompatível. " +
                    $"Primeiro={result.Columns.Count}, SELECT[{i}]={tables[i].Columns.Count}.";
                if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                    msg += "\nSQL: " + sqlContextForErrors;

                throw new InvalidOperationException(msg);
            }

            ValidateUnionColumnTypes(result.Columns, tables[i].Columns, i, sqlContextForErrors, dialect);
        }

        // merge
        // - entre 0 e 1 usa allFlags[0]
        // - entre 1 e 2 usa allFlags[1]
        // etc.
        // Começa com o primeiro
        foreach (var row in tables[0])
            result.Add(row);

        for (int i = 1; i < tables.Count; i++)
        {
            var isUnionAll = allFlags[i - 1];

            if (isUnionAll)
            {
                foreach (var row in tables[i])
                    result.Add(row);
            }
            else
            {
                // UNION => DISTINCT
                foreach (var row in tables[i])
                {
                    if (!ContainsRow(result, row, dialect))
                        result.Add(row);
                }
            }
        }

        // ORDER BY / LIMIT devem aplicar no resultado final
        // No seu parse atual, ORDER BY fica no último SELECT do UNION.
        var finalQ = parts[^1];
        var ctes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
        result = ApplyOrderAndLimit(result, finalQ, ctes);

        return result;
    }

    private static bool ContainsRow(
        TableResultMock table,
        Dictionary<int, object?> candidate,
        ISqlDialect dialect)
    {
        // comparação simples: mesmas chaves + mesmos valores
        // (se precisar mais forte depois, você troca aqui)
        foreach (var row in table)
        {
            if (row.Count != candidate.Count) continue;

            var ok = true;
            foreach (var kv in candidate)
            {
                if (!row.TryGetValue(kv.Key, out var v)) { ok = false; break; }
                if (!v.EqualsSql(kv.Value, dialect)) { ok = false; break; }
            }

            if (ok) return true;
        }
        return false;
    }

    private static void ValidateUnionColumnTypes(
        IList<TableResultColMock> expected,
        IList<TableResultColMock> current,
        int currentIndex,
        string? sqlContextForErrors,
        ISqlDialect dialect)
    {
        for (int i = 0; i < expected.Count; i++)
        {
            if (dialect.AreUnionColumnTypesCompatible(expected[i].DbType, current[i].DbType))
                continue;

            var msg =
                "UNION: tipo de coluna incompatível. " +
                $"Coluna[{i}] Primeiro={expected[i].DbType}, SELECT[{currentIndex}]={current[i].DbType}.";
            if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                msg += "\nSQL: " + sqlContextForErrors;

            throw new InvalidOperationException(msg);
        }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public TableResultMock ExecuteSelect(SqlSelectQuery q)
        => ExecuteSelect(q, null, null);

    private TableResultMock ExecuteSelect(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(selectQuery, nameof(selectQuery));

        // 0) Build CTE materializations (simple: materialize each CTE into a temp source)
        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var res = ExecuteSelect(cte.Query, ctes, outerRow);
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
        }

        // 1) FROM
        var rows = BuildFrom(selectQuery.Table, ctes, selectQuery.Where);

        // 2) JOINS
        foreach (var j in selectQuery.Joins)
            rows = ApplyJoin(rows, j, ctes);

        // 2.5) Correlated subquery: expose outer row fields/sources to subquery evaluation (EXISTS, IN subselect, etc.)
        if (outerRow is not null)
            rows = rows.Select(r => AttachOuterRow(r, outerRow));

        // 3) WHERE
        if (selectQuery.Where is not null)
            rows = rows.Where(r => Eval(selectQuery.Where, r, group: null, ctes).ToBool());

        // 4) GROUP BY / HAVING / SELECT projection
        bool needsGrouping = selectQuery.GroupBy.Count > 0 || selectQuery.Having is not null || ContainsAggregate(selectQuery);

        if (needsGrouping)
            return ExecuteGroup(selectQuery, ctes, rows);

        // 5) Project non-grouped
        var projected = ProjectRows(selectQuery, [.. rows], ctes);

        // 6) DISTINCT
        if (selectQuery.Distinct)
            projected = ApplyDistinct(projected, Dialect);

        // 7) ORDER BY / LIMIT
        projected = ApplyOrderAndLimit(projected, selectQuery, ctes);
        return projected;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows)
    {
        var keyExprs = q.GroupBy.Select(_ => ParseExpr(_)).ToArray();

        var grouped = rows.GroupBy(
            r => new GroupKey([.. keyExprs.Select(e => Eval(e, r, group: null, ctes))]),
            GroupKey.Comparer);

        // HAVING filter (MySQL: HAVING pode referenciar alias do SELECT)
        if (q.Having is null)
        {
            // Project grouped
            return ProjectGrouped(q, grouped, ctes);
        }

        // pré-parse das expressões do SELECT que têm Alias (ex: COUNT(val) AS C)
        var aliasExprs = q.SelectItems
            .Select(si =>
            {
                // pega alias mesmo se o parser não preencheu si.Alias
                var (exprRaw, alias) = SplitTrailingAsAlias(si.Raw, si.Alias);
                if (string.IsNullOrWhiteSpace(alias))
                    return (Alias: (string?)null, Ast: (SqlExpr?)null);

                SqlExpr ast;
#pragma warning disable CA1031 // Do not catch general exception types
                try { ast = ParseExpr(exprRaw); }
                catch (Exception e)
                {
#pragma warning disable CA1303
                    Console.WriteLine($"{GetType().Name}.{nameof(ExecuteSelect)}");
#pragma warning restore CA1303
                    Console.WriteLine(e);
                    ast = new RawSqlExpr(exprRaw);
                }
#pragma warning restore CA1031

                return (Alias: alias, Ast: (SqlExpr?)ast);
            })
            .Where(x => x.Alias is not null && x.Ast is not null)
            .Select(x => (Alias: x.Alias!, Ast: x.Ast!))
            .ToList();

        var havingExpr = q.Having;

        grouped = grouped.Where(g =>
        {
            var rows = g.ToList();
            var eg = new EvalGroup(rows);
            var first = rows[0];

            // clona a row e injeta aliases projetados (C, etc.)
            var ctx = first.CloneRow();
            foreach (var (alias, ast) in aliasExprs)
                ctx.Fields[alias] = Eval(ast, first, eg, ctes);

            return Eval(havingExpr, ctx, eg, ctes).ToBool();
        });

        // Project grouped
        return ProjectGrouped(q, grouped, ctes);
    }

    // ---------------- FROM/JOIN ----------------

    private IEnumerable<EvalRow> BuildFrom(
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where)
    {
        if (from is null)
        {
            // SELECT without FROM => one synthetic row
            yield return new EvalRow(
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));
            yield break;
        }

        var src = ResolveSource(from, ctes);
        var sourceRows = TryRowsFromIndex(src, where) ?? src.Rows();
        foreach (var r in sourceRows)
        {
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var it in r)
                fields[it.Key] = it.Value;

            var sources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            {
                [src.Alias] = src
            };

            yield return new EvalRow(fields, sources);
        }
    }

    private IEnumerable<Dictionary<string, object?>>? TryRowsFromIndex(
        Source src,
        SqlExpr? where)
    {
        if (where is null || src.Physical is null || src.Physical.Indexes.Count == 0)
            return null;

        if (!TryCollectColumnEqualities(where, src, out var equalsByColumn)
            || equalsByColumn.Count == 0)
            return null;

        IndexDef? best = null;
        foreach (var ix in src.Physical.Indexes.Values)
        {
            if (ix.KeyCols.Count == 0)
                continue;

            var coversAll = ix.KeyCols.All(col => equalsByColumn.ContainsKey(col.NormalizeName()));
            if (!coversAll)
                continue;

            if (best is null || ix.KeyCols.Count > best.KeyCols.Count)
                best = ix;
        }

        if (best is null)
            return null;

        var key = src.Physical is TableMock physicalTable
            ? physicalTable.BuildIndexKeyFromValues(best, equalsByColumn)
            : string.Join("|", best.KeyCols.Select(col =>
            {
                var norm = col.NormalizeName();
                var value = equalsByColumn[norm];
                return value?.ToString() ?? "<null>";
            }));

        var positions = LookupIndexWithMetrics(src.Physical, best, key);
        if (positions is null)
            return [];

        return src.RowsByIndexes(positions);
    }


    private IEnumerable<int>? LookupIndexWithMetrics(
        ITableMock table,
        IndexDef indexDef,
        string key)
    {
        _cnn.Metrics.IndexLookups++;
        _cnn.Metrics.IncrementIndexHint(indexDef.Name.NormalizeName());
        return table.Lookup(indexDef, key);
    }

    private bool TryCollectColumnEqualities(
        SqlExpr where,
        Source src,
        out Dictionary<string, object?> equalsByColumn)
    {
        equalsByColumn = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        return Walk(where, ref equalsByColumn);

        bool Walk(SqlExpr expr, ref Dictionary<string, object?> eqCol)
        {
            if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
                return Walk(andExpr.Left, ref eqCol) && Walk(andExpr.Right, ref eqCol);

            if (expr is not BinaryExpr eq || eq.Op != SqlBinaryOp.Eq)
                return false;

            if (TryGetColumnAndValue(eq.Left, eq.Right, src, out var column, out var value)
                || TryGetColumnAndValue(eq.Right, eq.Left, src, out column, out value))
            {
                eqCol[column] = value;
                return true;
            }

            return false;
        }
    }

    private bool TryGetColumnAndValue(
        SqlExpr maybeColumn,
        SqlExpr maybeValue,
        Source src,
        out string column,
        out object? value)
    {
        column = "";
        value = null;

        if (!TryResolveColumnName(maybeColumn, src, out var resolvedColumn))
            return false;

        if (!TryResolveConstantValue(maybeValue, out value))
            return false;

        column = resolvedColumn;
        return true;
    }

    private static bool TryResolveColumnName(
        SqlExpr expr,
        Source src,
        out string column)
    {
        column = "";

        switch (expr)
        {
            case IdentifierExpr id:
                {
                    var dot = id.Name.IndexOf('.');
                    if (dot < 0)
                    {
                        column = id.Name.NormalizeName();
                        return true;
                    }

                    var qualifier = id.Name[..dot].NormalizeName();
                    var sourceAlias = src.Alias.NormalizeName();
                    var sourceName = src.Name.NormalizeName();
                    if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                        && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                        return false;

                    column = id.Name[(dot + 1)..].NormalizeName();
                    return true;
                }

            case ColumnExpr col:
                {
                    if (!string.IsNullOrWhiteSpace(col.Qualifier))
                    {
                        var qualifier = col.Qualifier.NormalizeName();
                        var sourceAlias = src.Alias.NormalizeName();
                        var sourceName = src.Name.NormalizeName();
                        if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                            && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }

                    column = col.Name.NormalizeName();
                    return true;
                }

            default:
                return false;
        }
    }

    private bool TryResolveConstantValue(
        SqlExpr expr,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr l:
                value = l.Value;
                return true;
            case ParameterExpr p:
                value = ResolveParam(p.Name);
                return true;
            default:
                value = null;
                return false;
        }
    }

    private IEnumerable<EvalRow> ApplyJoin(
        IEnumerable<EvalRow> leftRows,
        SqlJoin join,
        IDictionary<string, Source> ctes)
    {
        var rightSrc = ResolveSource(join.Table, ctes);

        // FULL is not MySQL; accept as INNER for test/mock purposes
        var jt = //join.Type == SqlJoinType.Full ? SqlJoinType.Inner : 
            join.Type;

        if (jt == SqlJoinType.Cross)
        {
            foreach (var l in leftRows)
                foreach (var rr in rightSrc.Rows())
                {
                    var merged = l.CloneRow();
                    merged.AddSource(rightSrc);
                    merged.AddFields(rr);
                    yield return merged;
                }
            yield break;
        }

        if (jt == SqlJoinType.Right)
        {
            // Implement RIGHT JOIN by swapping and treating as LEFT
            var swapped = new SqlJoin(SqlJoinType.Left, join.Table, join.On);
            foreach (var r in ApplyJoinRight(leftRows, swapped, rightSrc, ctes))
                yield return r;
            yield break;
        }

        bool isLeft = jt == SqlJoinType.Left;

        foreach (var l in leftRows)
            foreach (var li in ApplyLeftJoin(join, ctes, rightSrc, isLeft, l))
                yield return li;
    }

    private IEnumerable<EvalRow> ApplyLeftJoin(
        SqlJoin join,
        IDictionary<string, Source> ctes,
        Source rightSrc,
        bool isLeft, EvalRow l)
    {
        bool matched = false;

        foreach (var rr in rightSrc.Rows())
        {
            var merged = l.CloneRow();
            merged.AddSource(rightSrc);
            merged.AddFields(rr);

            if (!Eval(join.On, merged, group: null, ctes).ToBool())
                continue;
            matched = true;
            yield return merged;
        }

        if (isLeft && !matched)
        {
            var merged = l.CloneRow();
            merged.AddSource(rightSrc);
            // add nulls for right columns
            foreach (var c in rightSrc.ColumnNames)
                merged.Fields[$"{rightSrc.Alias}.{c}"] = null;
            yield return merged;
        }
    }

    private IEnumerable<EvalRow> ApplyJoinRight(
        IEnumerable<EvalRow> leftRows,
        SqlJoin leftJoin,
        Source rightSrc,
        IDictionary<string, Source> ctes)
    {
        // RIGHT JOIN: produce all right rows, optionally matched with left
        var leftList = leftRows.ToList();

        foreach (var rr in rightSrc.Rows())
        {
            bool matched = false;
            foreach (var l in leftList)
            {
                var merged = l.CloneRow();
                merged.AddSource(rightSrc);
                merged.AddFields(rr);

                if (Eval(leftJoin.On, merged, group: null, ctes).ToBool())
                {
                    matched = true;
                    yield return merged;
                }
            }

            if (matched)
                continue;

            // left side nulls (best effort: keep existing left sources, but blank their fields)
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var sources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            {
                // no left sources/fields
                [rightSrc.Alias] = rightSrc
            };

            var row = new EvalRow(fields, sources);
            row.AddFields(rr);
            yield return row;
        }
    }

    private Source ResolveSource(
        SqlTableSource ts,
        IDictionary<string, Source> ctes)
    {
        var alias = ts.Alias ?? ts.Name ?? ts.DbName ?? "t";

        if (ts.DerivedUnion is not null)
        {
            var res = ExecuteUnion(
                [.. ts.DerivedUnion.Parts
                    .Select(_=>_ as SqlSelectQuery)
                    .Where(_=>_!= null)
                    .Select(_=>_!)],
                ts.DerivedUnion.AllFlags,
                ts.DerivedSql ?? "(derived)"
            );
            return Source.FromResult(alias, res);
        }

        if (ts.Derived is not null)
        {
            var res = ExecuteSelect(ts.Derived);
            return Source.FromResult(alias, res);
        }

        if (!string.IsNullOrWhiteSpace(ts.Name)
            && ctes.TryGetValue(ts.Name!, out var cteSrc))
        {
            // alias may differ from CTE name
            return cteSrc.WithAlias(alias);
        }

        if (string.IsNullOrWhiteSpace(ts.Name))
            throw new InvalidOperationException("FROM sem nome de tabela/CTE/derived não suportado.");

        var tableName = ts.Name!.NormalizeName();

        // Non-materialized VIEW: expand definition at execution time
        if (_cnn.TryGetView(tableName, out var viewSelect, ts.DbName)
            && viewSelect!= null)
        {
            var viewRes = ExecuteSelect(viewSelect, ctes, outerRow: null);
            return Source.FromResult(alias, viewRes);
        }

        // ✅ MySQL allows SELECT without FROM; parser may materialize it as FROM DUAL.
        // Treat DUAL as a single dummy row source.
        if (tableName.Equals("DUAL", StringComparison.OrdinalIgnoreCase))
        {
            var one = new TableResultMock();
            one.Add(new Dictionary<int, object?>());
            return Source.FromResult("DUAL", alias, one);
        }

        _cnn.Metrics.IncrementTableHint(tableName);
        var tb = _cnn.GetTable(tableName, ts.DbName);
        return Source.FromPhysical(tableName, alias, tb);
    }

    // ---------------- PROJECTION ----------------

    private TableResultMock ProjectRows(
        SqlSelectQuery q,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        var res = new TableResultMock();
        var selectPlan = BuildSelectPlan(q, rows, ctes);

        ComputeWindowSlots(selectPlan.WindowSlots, rows, ctes);

        // columns
        for (int i = 0; i < selectPlan.Columns.Count; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        // rows
        foreach (var r in rows)
        {
            var outRow = new Dictionary<int, object?>();
            for (int i = 0; i < selectPlan.Evaluators.Count; i++)
                outRow[i] = selectPlan.Evaluators[i](r, null);

            res.Add(outRow);
        }

        return res;
    }

    private TableResultMock ProjectGrouped(
        SqlSelectQuery q,
        IEnumerable<IGrouping<GroupKey, EvalRow>> groups,
        IDictionary<string, Source> ctes)
    {
        var res = new TableResultMock();
        var groupsList = groups.Select(g => new { g.Key, Rows = g.ToList() }).ToList();
        var selectPlan = BuildSelectPlan(q, groupsList.ConvertAll(g => g.Rows[0]), ctes);

        // columns
        for (int i = 0; i < selectPlan.Columns.Count; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        // rows
        foreach (var g in groupsList)
        {
            var eg = new EvalGroup(g.Rows);
            var outRow = new Dictionary<int, object?>();

            var first = g.Rows[0];
            for (int i = 0; i < selectPlan.Evaluators.Count; i++)
                outRow[i] = selectPlan.Evaluators[i](first, eg);

            res.Add(outRow);
        }

        if (q.Distinct)
            res = ApplyDistinct(res, Dialect);

        // ORDER / LIMIT
        res = ApplyOrderAndLimit(res, q, ctes);
        return res;
    }

    private sealed class SelectPlan
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required List<TableResultColMock> Columns { get; init; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required List<Func<EvalRow, EvalGroup?, object?>> Evaluators { get; init; }

        // Window functions computed over the current rowset (e.g. ROW_NUMBER() OVER (...))
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required List<WindowSlot> WindowSlots { get; init; }
    }

    private sealed class WindowSlot
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required WindowFunctionExpr Expr { get; init; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required Dictionary<EvalRow, object?> Map { get; init; }
    }

    private sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
        where T : class
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static readonly ReferenceEqualityComparer<T> Instance = new();
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int GetHashCode(T obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }


    private static string MakeUniqueAlias(
        List<TableResultColMock> cols,
        string preferred,
        string tableAlias)
    {
        if (!cols.Any(c => c.ColumnAlias.Equals(preferred, StringComparison.OrdinalIgnoreCase)))
            return preferred;

        var alt = $"{tableAlias}_{preferred}";
        if (!cols.Any(c => c.ColumnAlias.Equals(alt, StringComparison.OrdinalIgnoreCase)))
            return alt;

        int n = 2;
        while (true)
        {
            var a = $"{alt}_{n}";
            if (!cols.Any(c => c.ColumnAlias.Equals(a, StringComparison.OrdinalIgnoreCase)))
                return a;
            n++;
        }
    }


    private void ComputeWindowSlots(
        List<WindowSlot> slots,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        if (slots.Count == 0 || rows.Count == 0)
            return;

        foreach (var slot in slots)
        {
            var w = slot.Expr;

            // Only what we need for tests right now
            if (!w.Name.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase))
                continue;

            // Partitioning
            var partitions = new Dictionary<string, List<EvalRow>>(StringComparer.Ordinal);

            foreach (var r in rows)
            {
                string key;

                if (w.Spec.PartitionBy.Count == 0)
                {
                    key = "__all__";
                }
                else
                {
                    var parts = w.Spec.PartitionBy
                        .Select(e => NormalizeDistinctKey(Eval(e, r, null, ctes)))
                        .ToArray();
                    key = string.Join("\u001F", parts);
                }

                if (!partitions.TryGetValue(key, out var list))
                {
                    list = new List<EvalRow>();
                    partitions[key] = list;
                }
                list.Add(r);
            }

            foreach (var part in partitions.Values)
            {
                // ORDER BY inside OVER
                if (w.Spec.OrderBy.Count > 0)
                {
                    part.Sort((a, b) =>
                    {
                        foreach (var oi in w.Spec.OrderBy)
                        {
                            var av = Eval(oi.Expr, a, null, ctes);
                            var bv = Eval(oi.Expr, b, null, ctes);

                            var cmp = CompareSql(av, bv);
                            if (cmp != 0)
                                return oi.Desc ? -cmp : cmp;
                        }
                        return 0;
                    });
                }

                long rn = 1;
                foreach (var r in part)
                {
                    slot.Map[r] = rn;
                    rn++;
                }
            }
        }
    }

    private int CompareSql(object? a, object? b)
    {
        if (IsNullish(a) && IsNullish(b)) return 0;
        if (IsNullish(a)) return -1;
        if (IsNullish(b)) return 1;

        return a!.Compare(b!, Dialect);
    }

    private SelectPlan BuildSelectPlan(
            SqlSelectQuery q,
            List<EvalRow> sampleRows,
            IDictionary<string, Source> ctes)
    {
        var cols = new List<TableResultColMock>();
        var evals = new List<Func<EvalRow, EvalGroup?, object?>>();

        var windowSlots = new List<WindowSlot>();

        var sampleFirst = sampleRows.FirstOrDefault();

        foreach (var si in q.SelectItems)
        {
            Console.WriteLine($"[SELECT ITEM RAW] '{si.Raw}'  Alias='{si.Alias}'");
            var raw0 = si.Raw.Trim();

            // ✅ separa alias mesmo se o parser não preencheu si.Alias
            var (raw, asAlias) = SplitTrailingAsAlias(raw0, si.Alias);

            // Expand SELECT *
            if (!ExpandSelectAsterisc(cols, evals, sampleFirst, raw))
                continue;

            if (!IncludExtraColumns(sampleRows, cols, evals, raw))
                continue;

            // Default: parse as expression (best-effort)
            SqlExpr exprAst;
#pragma warning disable CA1031
            try
            {
                exprAst = ParseExpr(raw);
            }
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(BuildSelectPlan)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"[SELECT-ITEM] Raw0='{raw0}' RawExpr='{raw}' AliasParsed='{asAlias ?? "null"}' AliasSi='{si.Alias ?? "null"}'");
                Console.WriteLine(e);
                exprAst = new RawSqlExpr(raw);
            }
#pragma warning restore CA1031

            // ✅ alias preferido: (1) parser do SELECT (si.Alias), (2) AS extraído do raw, (3) infer
            var preferred = si.Alias ?? asAlias ?? InferColumnAlias(raw);
            var tableAl = q.Table?.Alias ?? q.Table?.Name ?? "";
            var colAlias = MakeUniqueAlias(cols, preferred, tableAl);
            var inferredDbType = InferDbTypeFromExpression(exprAst, sampleRows, ctes);

            cols.Add(new TableResultColMock(
                tableAlias: q.Table?.Alias ?? q.Table?.Name ?? "",
                columnAlias: colAlias,
                columnName: colAlias,
                columIndex: cols.Count,
                dbType: inferredDbType,
                isNullable: true));

            if (exprAst is WindowFunctionExpr w)
            {
                // slot.Map preenchido depois (quando tivermos todas as rows)
                var slot = new WindowSlot
                {
                    Expr = w,
                    Map = new Dictionary<EvalRow, object?>(ReferenceEqualityComparer<EvalRow>.Instance)
                };
                windowSlots.Add(slot);

                evals.Add((r, g) => slot.Map.TryGetValue(r, out var v) ? v : null);
            }
            else
            {
                evals.Add((r, g) => Eval(exprAst, r, g, ctes));
            }
        }

#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine("RESULT COLUMNS:");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        foreach (var c in cols)
            Console.WriteLine($" - {c.ColumnAlias}");

        return new SelectPlan { Columns = cols, Evaluators = evals, WindowSlots = windowSlots };
    }

    private DbType InferDbTypeFromExpression(
        SqlExpr exprAst,
        List<EvalRow> sampleRows,
        IDictionary<string, Source> ctes)
    {
        foreach (var row in sampleRows)
        {
            var value = Eval(exprAst, row, group: null, ctes);
            if (value is null || value is DBNull)
                continue;

            var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
            try
            {
                return type.ConvertTypeToDbType();
            }
            catch (ArgumentException)
            {
                return DbType.Object;
            }
        }

        return DbType.Object;
    }

    private static bool IncludExtraColumns(
        List<EvalRow> sampleRows,
        List<TableResultColMock> cols,
        List<Func<EvalRow, EvalGroup?, object?>> evals,
        string raw)
    {
        // tableAlias.*  (robusto: aceita espaços e crases)
        // casa "algo .*" com espaços opcionais:  <prefix> . *
        var mStar = Regex.Match(raw, @"^(?<p>.+?)\s*\.\s*\*\s*$");
        if (!mStar.Success)
            return true;

        var prefix = mStar.Groups["p"].Value.Trim();

        var sample = sampleRows.FirstOrDefault();
        if (sample is null)
            return true;

        // remove crases (caso: `t2`.*)
        prefix = prefix.Trim('`');

        // tenta achar por alias diretamente
        if (!sample.Sources.TryGetValue(prefix, out var src))
        {
            // tenta case-insensitive na mão (Sources é OrdinalIgnoreCase, mas não custa)
            var hit = sample.Sources.Keys.FirstOrDefault(k => k.Equals(prefix, StringComparison.OrdinalIgnoreCase));
            if (hit is not null)
                src = sample.Sources[hit];
        }

        if (src is null)
            return true;

        foreach (var colName in src.ColumnNames)
        {
            var alias = MakeUniqueAlias(cols, colName, src.Alias);
            cols.Add(new TableResultColMock(src.Alias, alias, colName, cols.Count, DbType.Object, true));
            evals.Add((r, g) => ResolveColumn(src.Alias, colName, r));
        }
        return false; // ✅ importante: não cai pro parser de expressão
    }

    private static bool ExpandSelectAsterisc(
        List<TableResultColMock> cols,
        List<Func<EvalRow, EvalGroup?, object?>> evals,
        EvalRow? sampleFirst, string raw)
    {
        if (raw != "*")
            return true;

        if (sampleFirst is null)
            return false;

        foreach (var src in sampleFirst.Sources.Values)
        {
            foreach (var colName in src.ColumnNames)
            {
                var alias = MakeUniqueAlias(cols, colName, src.Alias);
                cols.Add(new TableResultColMock(src.Alias, alias, colName, cols.Count, DbType.Object, true));
                evals.Add((r, g) => ResolveColumn(src.Alias, colName, r));
            }
        }
        return false;
    }

    // Remove "AS alias" somente quando:
    // - está no FINAL do select item
    // - e esse "AS" está fora de parênteses (pra não quebrar CAST(x AS CHAR))
    private static (string expr, string? alias) SplitTrailingAsAlias(
        string raw,
        string? alreadyAlias)
    {
        raw = raw.Trim();
        if (!string.IsNullOrWhiteSpace(alreadyAlias))
            return (raw, alreadyAlias);

        // varre de trás pra frente, mantendo depth de parênteses
        int depth = GetDepthAlias(raw);

        // regex final (fora de parênteses garantido pelo scanner acima? não 100%)
        // então fazemos um scanner que acha o último AS fora de parênteses.
        int asPos = -1;
        FindPositionOfAS(raw, ref depth, ref asPos);

        if (asPos < 0)
        {
            // MySQL permite alias sem AS:  SELECT COUNT(*) c1, col c2 FROM ...
            // Precisamos separar o último identificador (fora de parênteses) como alias,
            // sem quebrar expressões como "t2.*" ou "CAST(x AS CHAR)".
            if (TrySplitTrailingImplicitAlias(raw, out var expr0, out var alias0))
                return (expr0, alias0);

            return (raw, null);
        }

        // pega o sufixo depois do AS
        var after = raw[(asPos + 2)..].Trim();
        if (after.Length == 0)
            return (raw, null);

        // alias pode vir como `C` ou C
        // (aqui, só aceitamos identificador simples, que é o caso do seu teste)
        var m = Regex.Match(after, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?\s*$");
        if (!m.Success)
            return (raw, null);

        var alias = m.Groups["a"].Value;

        // expr é tudo antes do AS
        var expr = raw[..asPos].TrimEnd();
        if (expr.Length == 0)
            return (raw, null);

        return (expr, alias);
    }

    private static bool TrySplitTrailingImplicitAlias(
        string raw,
        out string expr,
        out string alias)
    {
        expr = raw;
        alias = string.Empty;

        // scanner reverso, ignorando o que está dentro de parênteses
        int depth = 0;
        int i = raw.Length - 1;
        while (i >= 0 && char.IsWhiteSpace(raw[i])) i--;
        if (i < 0) return false;

        int end = i;
        while (i >= 0)
        {
            var ch = raw[i];
            if (ch == ')') { depth++; i--; continue; }
            if (ch == '(') { depth = Math.Max(0, depth - 1); i--; continue; }
            if (depth == 0 && char.IsWhiteSpace(ch)) break;
            i--;
        }

        int start = i + 1;
        if (start > end) return false;

        var token = raw[start..(end + 1)].Trim();
        if (token.Length == 0) return false;

        // Alias tem que ser identificador simples: não aceita "t.col", "*", "?", etc.
        var m = Regex.Match(token, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?$", RegexOptions.CultureInvariant);
        if (!m.Success) return false;

        var a = m.Groups["a"].Value;
        if (IsLikelyKeyword(a)) return false;

        var before = raw[..start].TrimEnd();
        if (before.Length == 0) return false;

        // Evita pegar alias em "t2." (ex: "t2.*" já falha pelo regex acima)
        if (before.EndsWith(".")) return false;

        expr = before;
        alias = a;
        return true;
    }

    private static bool IsLikelyKeyword(string s)
    {
        // lista pequena e pragmática: só pra não confundir com sintaxe
        return s.Equals("FROM", StringComparison.OrdinalIgnoreCase)
            || s.Equals("WHERE", StringComparison.OrdinalIgnoreCase)
            || s.Equals("JOIN", StringComparison.OrdinalIgnoreCase)
            || s.Equals("LEFT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("RIGHT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("INNER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("OUTER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ON", StringComparison.OrdinalIgnoreCase)
            || s.Equals("GROUP", StringComparison.OrdinalIgnoreCase)
            || s.Equals("BY", StringComparison.OrdinalIgnoreCase)
            || s.Equals("HAVING", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ORDER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("LIMIT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("UNION", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ALL", StringComparison.OrdinalIgnoreCase)
            || s.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ASC", StringComparison.OrdinalIgnoreCase)
            || s.Equals("DESC", StringComparison.OrdinalIgnoreCase);
    }

    private static void FindPositionOfAS(string raw, ref int depth, ref int asPos)
    {
        for (int i = 0; i < raw.Length - 1; i++)
        {
            UpdateParenDepth(raw[i], ref depth);
            if (depth != 0) continue;

            if (!IsAsAt(raw, i)) continue;
            if (!IsWordBoundary(raw, i, 2)) continue;

            asPos = i;
        }
    }

    private static void UpdateParenDepth(char ch, ref int depth)
    {
        if (ch == '(') { depth++; return; }
        if (ch == ')') depth = Math.Max(0, depth - 1);
    }

    private static bool IsAsAt(string s, int i)
    {
        return (s[i] == 'A' || s[i] == 'a') &&
               (s[i + 1] == 'S' || s[i + 1] == 's');
    }

    // length=2 para "AS"
    private static bool IsWordBoundary(string s, int start, int length)
    {
        int left = start - 1;
        int right = start + length;

        bool leftOk = left < 0 || !IsIdentChar(s[left]);
        bool rightOk = right >= s.Length || !IsIdentChar(s[right]);

        return leftOk && rightOk;
    }

    private static bool IsIdentChar(char c) =>
        char.IsLetterOrDigit(c) || c == '_';

    private static int GetDepthAlias(string raw)
    {
        int depth = 0;
        for (int i = raw.Length - 1; i >= 0; i--)
        {
            char ch = raw[i];
            if (ch == ')') depth++;
            else if (ch == '(') depth = Math.Max(0, depth - 1);

            // só tenta achar " AS " quando estiver fora de parênteses
            if (depth != 0) continue;

            // procura um " AS " (case-insensitive) perto do fim.
            // Ex: "COUNT(val) AS C"
            //      ^ aqui
            if (i >= 1 && raw[i] == 'S' || raw[i] == 's')
            {
                // checa "...A S..." com espaço antes e depois
                // vamos achar o padrão usando regex no sufixo por segurança
            }
        }

        return depth;
    }

    private static string InferColumnAlias(string raw)
    {
        raw = raw.Trim();

        // For identifiers/qualified columns, we want the last part, but the token printer
        // may produce spaces around dots (e.g. "u . id"). Normalize just for alias inference.
        var norm = Regex.Replace(raw, @"\s*\.\s*", ".");

        var dot = norm.LastIndexOf('.');
        if (dot >= 0 && dot + 1 < norm.Length)
            return norm[(dot + 1)..].Trim().Trim('`');

        return norm.Trim().Trim('`');
    }

    // ---------------- ORDER / LIMIT ----------------

    private TableResultMock ApplyOrderAndLimit(
        TableResultMock res,
        SqlSelectQuery q,
        IDictionary<string, Source> ctes)
    {
        // ORDER BY (aliases/ordinals/expressions) + LIMIT/OFFSET

        // LIMIT/OFFSET sem ORDER BY ainda precisa aplicar
        if (q.OrderBy.Count == 0)
        {
            ApplyLimit(res, q);
            return res;
        }

        // Pre-parse ORDER BY keys once
        var keys = new List<(Func<Dictionary<int, object?>, object?> Get, bool Desc, string Raw)>();

        foreach (var it in q.OrderBy)
        {
            var raw = (it.Raw ?? string.Empty).Trim();
            if (raw.Length == 0)
                continue;

            // ordinal: ORDER BY 1,2,...
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: ORDER BY ordinal must be >= 1");

                var colIdx = ord - 1;
                if (colIdx >= res.Columns.Count)
                    throw new InvalidOperationException($"invalid: ORDER BY ordinal {ord} out of range");

                keys.Add((r => r.TryGetValue(colIdx, out var v) ? v : null, it.Desc, raw));
                continue;
            }

            // column/alias fast-path
            var col = res.Columns.FirstOrDefault(c => c.ColumnName.Equals(raw, StringComparison.OrdinalIgnoreCase));
            if (col is not null)
            {
                var colIdx = col.ColumIndex;
                keys.Add((r => r.TryGetValue(colIdx, out var v) ? v : null, it.Desc, raw));
                continue;
            }

            var aliasToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            // 1) Se você tiver nomes de colunas no res (ex: res.Columns / res.ColumnNames / etc), use isso.
            // Vou assumir que res.Columns é uma lista/dicionário com ordem e Nome. Ajuste a fonte conforme seu res.
            for (int i = 0; i < res.Columns.Count; i++)
            {
                var colName = res.Columns[i].ColumnName; // <-- AJUSTE AQUI conforme seu tipo real
                if (!string.IsNullOrWhiteSpace(colName) && !aliasToIndex.ContainsKey(colName))
                    aliasToIndex[colName] = i;
            }

            // 2) Fallback: se sua estrutura já tem esse mapa pronto, use ela e delete o loop acima.
            // Ex: aliasToIndex = res.AliasToIndex;  (se existir)

            var expr = ParseExpr(raw);

            Func<Dictionary<int, object?>, object?> get = r =>
            {
                var fake = EvalRow.FromProjected(res, r, aliasToIndex);
                return Eval(expr, fake, group: null, ctes);
            };

            keys.Add((Get: get, it.Desc, Raw: raw));
        }

        if (keys.Count == 0)
        {
            ApplyLimit(res, q);
            return res;
        }

        int CompareObj(object? a, object? b)
        {
            if (a is null && b is null) return 0;
            if (a is null) return -1;
            if (b is null) return 1;

            return a.Compare(b, Dialect);
        }

        int CompareRows(Dictionary<int, object?> ra, Dictionary<int, object?> rb)
        {
            foreach (var (Get, Desc, Raw) in keys)
            {
                var ka = Get(ra);
                var kb = Get(rb);

                var cmp = CompareObj(ka, kb);

                // MySQL: NULLS FIRST for ASC, NULLS LAST for DESC
                if (Desc)
                {
                    // invert, but keep nulls last
                    if (ka is null && kb is null) cmp = 0;
                    else if (ka is null) cmp = 1;
                    else if (kb is null) cmp = -1;
                    else cmp = -cmp;
                }

                if (cmp != 0) return cmp;
            }
            return 0;
        }

        var sorted = res.OrderBy(r => r, Comparer<Dictionary<int, object?>>.Create(CompareRows)).ToList();
        res.Clear();
        foreach (var r in sorted) res.Add(r);

        ApplyLimit(res, q);
        return res;
    }

    private static void ApplyLimit(
        TableResultMock res,
        SqlSelectQuery q)
    {
        int? offset = null;
        int take;
        switch (q.RowLimit)
        {
            case SqlLimitOffset l:
                offset = l.Offset;
                take = l.Count;
                break;
            case SqlFetch f:
                offset = f.Offset;
                take = f.Count;
                break;
            default:
                return;
        }

        var skip = offset ?? 0;
        var sliced = res.Skip(skip).Take(take).ToList();
        res.Clear();
        foreach (var r in sliced) res.Add(r);
    }


    // ---------------- DIALECT HOOKS ----------------

    /// <summary>
    /// EN: Dialect mapping for JSON access operators (-> / ->> etc).
    /// Default implementation matches current MySQL best-effort behavior.
    /// SqlServer/Postgre/Oracle should override.
    /// PT: Mapeamento de dialeto para operadores de acesso JSON (-> / ->> etc).
    /// A implementação padrão segue o comportamento best-effort do MySQL.
    /// SqlServer/Postgre/Oracle devem sobrescrever.
    /// </summary>
    protected virtual SqlExpr MapJsonAccess(JsonAccessExpr ja)
    {
        // MySQL semantics (best-effort):
        //  a ->  '$.x'  => JSON_EXTRACT(a, '$.x')
        //  a ->> '$.x'  => JSON_UNQUOTE(JSON_EXTRACT(a, '$.x'))
        var extract = new FunctionCallExpr("JSON_EXTRACT", new[] { ja.Target, ja.Path });
        return ja.Unquote
            ? new FunctionCallExpr("JSON_UNQUOTE", new[] { extract })
            : extract;
    }

    /// <summary>
    /// EN: Dialect mapping for scalar subquery evaluation.
    /// Default is MySQL-like: first cell of first row.
    /// PT: Mapeamento de dialeto para avaliação de subconsulta escalar.
    /// O padrão é semelhante ao MySQL: primeira célula da primeira linha.
    /// </summary>
    protected virtual object? EvalScalarSubquery(
        SubqueryExpr sq,
        IDictionary<string, Source> ctes,
        EvalRow row)
    {
        var r = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "EVAL subquery"), ctes, row);
        return r.Count > 0 && r[0].TryGetValue(0, out var v) ? v : null;
    }

    // ---------------- EXPRESSION EVAL ----------------

    private object? Eval(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        switch (expr)
        {
            case LiteralExpr l:
                return l.Value;

            case ParameterExpr p:
                return ResolveParam(p.Name);

            case IdentifierExpr id:
                return ResolveIdentifier(id.Name, row);

            case ColumnExpr col:
                return ResolveColumn(col.Qualifier, col.Name, row);

            case StarExpr:
                // only meaningful inside COUNT(*)
                return "*";

            case IsNullExpr isn:
                {
                    var v1 = Eval(isn.Expr, row, group, ctes);
                    var isNull = v1 is null || v1 is DBNull;
                    return isn.Negated ? !isNull : isNull;
                }

            case LikeExpr like:
                {
                    var left = Eval(like.Left, row, group, ctes)?.ToString() ?? "";
                    var pat = Eval(like.Pattern, row, group, ctes)?.ToString() ?? "";
                    return left.Like(pat, Dialect);
                }

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
                return !Eval(u.Expr, row, group, ctes).ToBool();

            case BinaryExpr b:
                return EvalBinary(b, row, group, ctes);

            case InExpr i:
                return EvalIn(i, row, group, ctes);

            case ExistsExpr ex:
                return EvalExists(ex, row, ctes);


            case CaseExpr c:
                return EvalCase(c, row, group, ctes);

            case JsonAccessExpr ja:
                var mapped = MapJsonAccess(ja);
                return Eval(mapped, row, group, ctes);
            case FunctionCallExpr fn:
                return EvalFunction(fn, row, group, ctes);
            case CallExpr ce:
                return EvalCall(ce, row, group, ctes);
            case BetweenExpr b:
                return EvalBetween(b, row, group, ctes);
            case SubqueryExpr sq:
                return EvalScalarSubquery(sq, ctes, row);
            case RowExpr re:
                return re.Items.Select(it => Eval(it, row, group, ctes)).ToArray();

            case RawSqlExpr:
                // unsupported expression (e.g. CAST(x AS CHAR)): best-effort: null
                return null;

            default:
                throw new InvalidOperationException($"Expr não suportada no executor: {expr.GetType().Name}");
        }
    }

    private object? EvalBetween(
        BetweenExpr b,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // desugar em runtime (não no parser): (x >= low AND x <= high)
        var ge = new BinaryExpr(SqlBinaryOp.GreaterOrEqual, b.Expr, b.Low);
        var le = new BinaryExpr(SqlBinaryOp.LessOrEqual, b.Expr, b.High);
        var and = new BinaryExpr(SqlBinaryOp.And, ge, le);

        var res = Eval(and, row, group, ctes);

        if (b.Negated)
            return res is null ? null : !(bool)res;

        return res;
    }

    private object? EvalBinary(
        BinaryExpr b,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (b.Op == SqlBinaryOp.And)
            return Eval(b.Left, row, group, ctes).ToBool() && Eval(b.Right, row, group, ctes).ToBool();

        if (b.Op == SqlBinaryOp.Or)
            return Eval(b.Left, row, group, ctes).ToBool()
                || Eval(b.Right, row, group, ctes).ToBool();

        var l = Eval(b.Left, row, group, ctes);
        var r = Eval(b.Right, row, group, ctes);

        // arithmetic: NULL propagates (MySQL)
        if (b.Op is SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide)
        {
            if (l is null || r is null) return null;

            if (l is DateTime dt && r is IntervalValue interval)
            {
                return b.Op switch
                {
                    SqlBinaryOp.Add => dt.Add(interval.Span),
                    SqlBinaryOp.Subtract => dt.Subtract(interval.Span),
                    _ => throw new InvalidOperationException("op aritmético inválido")
                };
            }

            decimal ToDec(object v)
            {
                if (v is decimal dd) return dd;
                if (v is byte or sbyte or short or ushort or int or uint or long or ulong)
                    return Convert.ToDecimal(v, CultureInfo.InvariantCulture);
                if (v is float ff) return Convert.ToDecimal(ff, CultureInfo.InvariantCulture);
                if (v is double db) return Convert.ToDecimal(db, CultureInfo.InvariantCulture);
                if (v is string s && decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                    return parsed;
                throw new InvalidOperationException($"Não consigo converter '{v}' para número.");
            }

            var a = ToDec(l);
            var b2 = ToDec(r);

            return b.Op switch
            {
                SqlBinaryOp.Add => a + b2,
                SqlBinaryOp.Subtract => a - b2,
                SqlBinaryOp.Multiply => a * b2,
                SqlBinaryOp.Divide => b2 == 0m ? null : a / b2,
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
        }

        // NULL-safe equality
        if (b.Op == SqlBinaryOp.NullSafeEq)
        {
            if (l is null && r is null) return true;
            if (l is null || r is null) return false;
            return l.Compare(r, Dialect) == 0;
        }

        if (l is null || l is DBNull || r is null || r is DBNull)
        {
            // SQL: comparisons with NULL => false (except IS NULL handled elsewhere)
            return false;
        }

        var cmp = l.Compare(r, Dialect);

        return b.Op switch
        {
            SqlBinaryOp.Eq => cmp == 0,
            SqlBinaryOp.Neq => cmp != 0,
            SqlBinaryOp.Greater => cmp > 0,
            SqlBinaryOp.GreaterOrEqual => cmp >= 0,
            SqlBinaryOp.Less => cmp < 0,
            SqlBinaryOp.LessOrEqual => cmp <= 0,
            SqlBinaryOp.Regexp => EvalRegexp(l, r, Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para REGEXP.")),
            _ => throw new InvalidOperationException($"Binary op não suportado: {b.Op}")
        };
    }

    private static bool EvalRegexp(object l, object r, ISqlDialect dialect)
    {
        try
        {
            return Regex.IsMatch(l.ToString() ?? "", r.ToString() ?? "");
        }
        catch (ArgumentException)
        {
            if (dialect.RegexInvalidPatternEvaluatesToFalse)
                return false;
            throw;
        }
    }

    private bool EvalIn(
        InExpr i,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var leftVal = Eval(i.Left, row, group, ctes);

        if (leftVal is null)
            return false;

        // IN (subquery)
        if (i.Items.Count == 1 && i.Items[0] is SubqueryExpr sq)
        {
            // ✅ passa ctes + outerRow pra suportar correlated subquery (u.Id etc.)
            var sub = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "IN"), ctes, row);

            // MySQL: IN(subquery) considera a 1ª coluna retornada
            foreach (var sr in sub)
            {
                var v = sr.TryGetValue(0, out var cell) ? cell : null;
                if (leftVal.EqualsSql(v, Dialect))
                    return true;
            }

            return false;
        }

        // IN (item1, item2, ...)
        foreach (var it in i.Items)
        {
            var v = Eval(it, row, group, ctes);

            // ✅ Caso especial: IN @ids onde @ids é lista/array
            // Expande IEnumerable (mas não string) como múltiplos candidatos.
            if (v is System.Collections.IEnumerable ie && v is not string)
            {
                foreach (var item in ie)
                {
                    var cand = item;

                    // Row IN Row (quando o parametro é lista de tuples/rows)
                    if (leftVal is object?[] la2 && cand is object?[] ra2)
                    {
                        if (la2.Length == ra2.Length && !la2.Where((t, idx) => !t.EqualsSql(ra2[idx], Dialect)).Any())
                            return true;
                        continue;
                    }

                    if (leftVal.EqualsSql(cand, Dialect))
                        return true;
                }

                continue; // não cai no EqualsSql(v) do enumerable inteiro
            }

            // Row IN Row (normal)
            if (leftVal is object?[] la && v is object?[] ra)
            {
                if (la.Length == ra.Length && !la.Where((t, idx) => !t.EqualsSql(ra[idx], Dialect)).Any())
                    return true;
                continue;
            }

            if (leftVal.EqualsSql(v, Dialect))
                return true;
        }

        return false;
    }

    private bool EvalExists(
        ExistsExpr ex,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var sq = ex.Subquery;
        var sub = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "EXISTS"), ctes, row); // ✅ AST + correlated
        return sub.Count > 0;
    }


    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Searched CASE: CASE WHEN cond THEN x ...
        if (c.BaseExpr is null)
        {
            foreach (var wt in c.Whens)
            {
                if (Eval(wt.When, row, group, ctes).ToBool())
                    return Eval(wt.Then, row, group, ctes);
            }

            return c.ElseExpr is not null
                ? Eval(c.ElseExpr, row, group, ctes)
                : null;
        }

        // Simple CASE: CASE base WHEN val THEN x ...
        var baseVal = Eval(c.BaseExpr, row, group, ctes);

        foreach (var wt in c.Whens)
        {
            var whenVal = Eval(wt.When, row, group, ctes);

            // MySQL uses '=' comparison semantics here: NULL never matches (even NULL)
            if (baseVal is null || baseVal is DBNull || whenVal is null || whenVal is DBNull)
                continue;

            if (baseVal.Compare(whenVal, Dialect) == 0)
                return Eval(wt.Then, row, group, ctes);
        }

        return c.ElseExpr is not null
            ? Eval(c.ElseExpr, row, group, ctes)
            : null;
    }

    private object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Aggregate?
        if (group is not null && _aggFns.Contains(fn.Name))
            return EvalAggregate(fn, group, ctes);

        // Scalar functions (best-effort)
        if (fn.Name.Equals("FIND_IN_SET", StringComparison.OrdinalIgnoreCase))
        {
            var needle = EvalArg(0)?.ToString() ?? "";
            var hay = EvalArg(1)?.ToString() ?? "";
            var parts = hay.Split(',').Select(_=>_.Trim()).ToArray();
            var idx = Array.FindIndex(parts, p => string.Equals(p, needle, StringComparison.OrdinalIgnoreCase));
            return idx >= 0 ? idx + 1 : 0;
        }

        if (fn.Name.Equals("IF", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("IIF", StringComparison.OrdinalIgnoreCase))
        {
            var cond = EvalArg(0).ToBool();
            return cond ? EvalArg(1) : EvalArg(2);
        }

        if (fn.Name.Equals("IFNULL", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NVL", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? EvalArg(1) : v;
        }

        if (fn.Name.Equals("ISNULL", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? EvalArg(1) : v;
        }

        if (fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase))
        {
            for (int i = 0; i < fn.Args.Count; i++)
            {
                var v = EvalArg(i);
                if (!IsNullish(v)) return v;
            }
            return null;
        }


        // JSON_EXTRACT(json, '$.path') / JSON_VALUE(json, '$.path') (best-effort)
        if (fn.Name.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            object? json = EvalArg(0);
            var path = EvalArg(1)?.ToString();

            if (IsNullish(json) || string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return TryReadJsonPathValue(json!, path!);
            }
#pragma warning disable CA1031
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine(e);
                return null;
            }
#pragma warning restore CA1031
        }

        // JSON_UNQUOTE(x) (best-effort)
        if (fn.Name.Equals("JSON_UNQUOTE", StringComparison.OrdinalIgnoreCase))
        {
            object? v = EvalArg(0);
            if (IsNullish(v)) return null;
            var s = v!.ToString() ?? "";
            if (s.Length >= 2 && ((s[0] == '"' && s[^1] == '"') || (s[0] == '\'' && s[^1] == '\'')))
                return s[1..^1];
            return s;
        }

        if (fn.Name.Equals("TO_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            if (IsNullish(v)) return null;

            if (v is byte or sbyte or short or ushort or int or uint or long or ulong)
                return Convert.ToInt64(v, CultureInfo.InvariantCulture);

            if (v is decimal or double or float)
                return Convert.ToDecimal(v, CultureInfo.InvariantCulture);

            var text = v?.ToString();
            if (string.IsNullOrWhiteSpace(text))
                return null;

            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var li))
                return li;

            if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec))
                return dec;

            return null;
        }

        // TRY_CAST(x AS TYPE) - similar ao CAST, mas retorna null em falha
        if (fn.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2) return null;

            var v = EvalArg(0);
            var type = fn.Args[1] is RawSqlExpr trx ? trx.Sql : (EvalArg(1)?.ToString() ?? "");
            type = type.Trim();

            if (IsNullish(v)) return null;

            try
            {
                if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
                {
                    if (v is long l) return (int)l;
                    if (v is int i) return i;
                    if (v is decimal d) return (int)d;
                    if (int.TryParse(v!.ToString(), out var ix)) return ix;
                    if (long.TryParse(v!.ToString(), out var lx)) return (int)lx;
                    return null;
                }

                if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
                {
                    if (v is decimal dd) return dd;
                    if (decimal.TryParse(v!.ToString(), out var dx)) return dx;
                    return null;
                }

                if (type.StartsWith("CHAR", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("VARCHAR", StringComparison.OrdinalIgnoreCase))
                    return v!.ToString();

                return v!.ToString();
            }
            catch
            {
                return null;
            }
        }

        // CAST(x AS TYPE) - aqui chega como CallExpr("CAST", [expr, RawSqlExpr("SIGNED")]) via parser
        if (fn.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2) return null;

            var v = EvalArg(0);
            var type = fn.Args[1] is RawSqlExpr rx ? rx.Sql : (EvalArg(1)?.ToString() ?? "");
            type = type.Trim();

            if (IsNullish(v)) return null;

            try
            {
                if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
                {
                    if (v is long l) return (int)l;
                    if (v is int i) return i;
                    if (v is decimal d) return (int)d;
                    var text = v!.ToString()?.Trim() ?? string.Empty;
                    if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ix)) return ix;
                    if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lx)) return (int)lx;
                    if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return (int)dx;
                    return 0;
                }

                if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
                {
                    if (v is decimal dd) return dd;
                    if (decimal.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return dx;
                    return 0m;
                }

                if (type.StartsWith("CHAR", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("VARCHAR", StringComparison.OrdinalIgnoreCase))
                    return v!.ToString();

                // desconhecido: best-effort string
                return v!.ToString();
            }
#pragma warning disable CA1031
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine(e);
                return null;
            }
#pragma warning restore CA1031
        }

        if (fn.Name.Equals("CONCAT", StringComparison.OrdinalIgnoreCase))
        {
            // MySQL: CONCAT returns NULL if any argument is NULL
            var parts = new string[fn.Args.Count];
            for (int i = 0; i < fn.Args.Count; i++)
            {
                var v = EvalArg(i);
                if (IsNullish(v)) return null;
#pragma warning disable CA1508 // Avoid dead conditional code
                parts[i] = v?.ToString() ?? "";
#pragma warning restore CA1508 // Avoid dead conditional code
            }
            return string.Concat(parts);
        }

        if (fn.Name.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase))
        {
            // CONCAT_WS(sep, a, b, ...) ignores NULL values (except sep)
            var sep = EvalArg(0);
            if (IsNullish(sep)) return null;
#pragma warning disable CA1508 // Avoid dead conditional code
            var s = sep?.ToString() ?? "";
#pragma warning restore CA1508 // Avoid dead conditional code

            var parts = new List<string>();
            for (int i = 1; i < fn.Args.Count; i++)
            {
                var v = EvalArg(i);
                if (IsNullish(v)) continue;
#pragma warning disable CA1508 // Avoid dead conditional code
                parts.Add(v?.ToString() ?? "");
#pragma warning restore CA1508 // Avoid dead conditional code
            }
            return string.Join(s, parts);
        }

        if (fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LCASE", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
#pragma warning disable CA1308 // Normalize strings to uppercase
            return IsNullish(v) ? null : v!.ToString()!.ToLowerInvariant();
#pragma warning restore CA1308 // Normalize strings to uppercase
        }

        if (fn.Name.Equals("UPPER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UCASE", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : v!.ToString()!.ToUpperInvariant();
        }

        if (fn.Name.Equals("TRIM", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : v!.ToString()!.Trim();
        }

        if (fn.Name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("CHAR_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : (long)(v!.ToString()!.Length);
        }

        if (fn.Name.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase))
        {
            var s = EvalArg(0);
            if (IsNullish(s)) return null;
            var str = s!.ToString() ?? "";

            var pos = EvalArg(1);
            if (IsNullish(pos)) return null;

            var start = Convert.ToInt32(pos.ToDec()) - 1; // MySQL is 1-based
            if (start < 0) start = 0;
            if (start >= str.Length) return "";

            var lenObj = EvalArg(2);
            if (IsNullish(lenObj))
                return str[start..];

            var len = Convert.ToInt32(lenObj.ToDec());
            if (len <= 0) return "";
            if (start + len > str.Length) len = str.Length - start;

            return str.Substring(start, len);
        }

        if (fn.Name.Equals("REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            var s = EvalArg(0);
            var from = EvalArg(1);
            var to = EvalArg(2);

            if (IsNullish(s) || IsNullish(from) || IsNullish(to))
                return null;

            return (s!.ToString() ?? "")
                .Replace(from!.ToString() ?? "", to!.ToString() ?? "");
        }


        if (fn.Name.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase))
        {
            var baseVal = EvalArg(0);
            if (IsNullish(baseVal)) return null;
            if (!TryCoerceDateTime(baseVal, out var dt))
                return null;

            var itExpr = fn.Args.Count > 1 ? fn.Args[1] : null;
            if (itExpr is null) return dt;

            // INTERVAL n DAY comes as CallExpr("INTERVAL", [n, RawSqlExpr("DAY")])
            // Importante: NÃO eval o CallExpr("INTERVAL") como função, senão vira null e DATE_ADD vira no-op.
            if (itExpr is CallExpr ce && ce.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase) && ce.Args.Count >= 2)
            {
                var nObj = Eval(ce.Args[0], row, group, ctes);
                var unit = ce.Args[1] is RawSqlExpr rx ? rx.Sql : Eval(ce.Args[1], row, group, ctes)?.ToString() ?? "DAY";

                var n = Convert.ToInt32((nObj ?? 0m).ToDec());
                return ApplyDateDelta(dt, unit, n);
            }

            return dt;
        }

        if (fn.Name.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                return null;

            var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
            var amountObj = EvalArg(1);
            var baseVal = EvalArg(2);
            if (IsNullish(baseVal)) return null;
            if (!TryCoerceDateTime(baseVal, out var dt))
                return null;

            var n = Convert.ToInt32((amountObj ?? 0m).ToDec());
            return ApplyDateDelta(dt, unit, n);
        }

        if (fn.Name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                return null;

            var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
            var amountObj = EvalArg(1);
            var baseVal = EvalArg(2);
            if (IsNullish(baseVal)) return null;

            if (!TryCoerceDateTime(baseVal, out var dt))
                return null;

            var n = Convert.ToInt32((amountObj ?? 0m).ToDec());
            return ApplyDateDelta(dt, unit, n);
        }

        if ((fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase))
            && fn.Args.Count >= 1)
        {
            var baseVal = EvalArg(0);
            if (IsNullish(baseVal)) return null;
            if (!TryCoerceDateTime(baseVal, out var dt))
                return null;

            // Minimal SQLite-like modifier support: '+N day|hour|minute|second|month|year'
            for (int i = 1; i < fn.Args.Count; i++)
            {
                var modifier = EvalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(modifier))
                    continue;

                if (!TryParseDateModifier(modifier!, out var unit, out var amount))
                    continue;

                dt = ApplyDateDelta(dt, unit, amount);
            }

            if (fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase))
                return dt.Date;

            return dt;
        }

        
        if (fn.Name.Equals("FIELD", StringComparison.OrdinalIgnoreCase))
        {
            // MySQL FIELD(str, str1, str2, ...) returns 1-based index of str in the list; 0 if not found
            var target = EvalArg(0);
            if (IsNullish(target)) return 0;

            for (int ai = 1; ai < fn.Args.Count; ai++)
            {
                var cand = EvalArg(ai);
                if (IsNullish(cand)) continue;
                if (target.EqualsSql(cand, Dialect))
                    return ai; // 1-based
            }
            return 0;
        }

// Unknown scalar => null (don't explode tests)
        return null;

        object? EvalArg(int i) => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null;
    }

    private static object? TryReadJsonPathValue(object json, string path)
    {
        var jsonStr = json.ToString() ?? "";
        using var doc = System.Text.Json.JsonDocument.Parse(jsonStr);

        // suporta só "$.a.b" e "$.a" (suficiente pro corpus)
        if (!path.StartsWith("$.", StringComparison.Ordinal))
            return null;

        var cur = doc.RootElement;
        var segs = path[2..].Split('.').Select(_=>_.Trim()).ToArray();
        foreach (var s in segs)
        {
            if (cur.ValueKind != System.Text.Json.JsonValueKind.Object)
                return null;

            if (!cur.TryGetProperty(s.Trim('"'), out var next))
                return null;

            cur = next;
        }

        return cur.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => cur.GetString(),
            System.Text.Json.JsonValueKind.Number => cur.TryGetInt64(out var li) ? li : cur.GetDecimal(),
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Null => null,
            _ => cur.ToString()
        };
    }

    private string GetDateAddUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var unit = expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(unit))
        {
            var eval = Eval(expr, row, group, ctes);
            unit = eval?.ToString() ?? string.Empty;
        }

        return unit!.Trim().ToUpperInvariant();
    }

    private static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
    {
        dt = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        switch (baseVal)
        {
            case DateTime d:
                dt = d;
                return true;
            case DateTimeOffset dto:
                dt = dto.DateTime;
                return true;
        }

        return DateTime.TryParse(
            baseVal.ToString(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeLocal,
            out dt);
    }

    private static DateTime ApplyDateDelta(DateTime dt, string unit, int amount)
    {
        var normalized = unit.Trim().ToUpperInvariant();
        return normalized switch
        {
            "YEAR" or "YEARS" or "YY" or "YYYY" => dt.AddYears(amount),
            "MONTH" or "MONTHS" or "MM" => dt.AddMonths(amount),
            "DAY" or "DAYS" or "DD" or "D" => dt.AddDays(amount),
            "HOUR" or "HOURS" or "HH" => dt.AddHours(amount),
            "MINUTE" or "MINUTES" or "MI" or "N" => dt.AddMinutes(amount),
            "SECOND" or "SECONDS" or "SS" or "S" => dt.AddSeconds(amount),
            _ => dt
        };
    }

    private static bool TryParseDateModifier(string modifier, out string unit, out int amount)
    {
        unit = string.Empty;
        amount = 0;

        var m = Regex.Match(
            modifier.Trim(),
            @"^(?<amount>[+-]?\d+)\s*(?<unit>\w+)s?$",
            RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

        if (!m.Success)
            return false;

        if (!int.TryParse(m.Groups["amount"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        unit = m.Groups["unit"].Value;
        return !string.IsNullOrWhiteSpace(unit);
    }

    private object? EvalCall(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Aggregate?
        if (group is not null && _aggFns.Contains(fn.Name))
            return EvalAggregate(fn, group, ctes);

        if (fn.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return ParseIntervalValue(fn, row, group, ctes);

        // se não for agregado, trata como função "normal" reaproveitando EvalFunction
        // (Distinct em função escalar não faz sentido aqui, então ignoramos)
        var shim = new FunctionCallExpr(fn.Name, fn.Args);
        return EvalFunction(shim, row, group, ctes);
    }

    private static bool IsNullish(object? v) => v is null || v is DBNull;

    private sealed record IntervalValue(TimeSpan Span);

    private IntervalValue? ParseIntervalValue(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        var raw = Eval(fn.Args[0], row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        raw = raw!.Trim();
        if (raw.Contains('\\'))
            raw = raw.Replace("\\", string.Empty);

        var match = Regex.Match(raw, @"^(?<num>-?\d+(?:\.\d+)?)\s*(?<unit>[a-zA-Z]+)$");
        if (!match.Success)
            return null;

        if (!decimal.TryParse(match.Groups["num"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
            return null;

        var unit = match.Groups["unit"].Value.ToUpperInvariant();
        var span = unit switch
        {
            "DAY" or "DAYS" => TimeSpan.FromDays((double)value),
            "HOUR" or "HOURS" => TimeSpan.FromHours((double)value),
            "MINUTE" or "MINUTES" => TimeSpan.FromMinutes((double)value),
            "SECOND" or "SECONDS" => TimeSpan.FromSeconds((double)value),
            _ => (TimeSpan?)null
        };

        return span is null ? null : new IntervalValue(span.Value);
    }

    private object? EvalAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var name = fn.Name.ToUpperInvariant();

        // COUNT -> SEMPRE long
        if (!TryEvalAggrageteCount(fn, group, ctes, name, out var value))
            return value;

        if (fn.Args.Count == 0)
            return null;

        // coleta valores (não-nulos)
        var values = GetNotNullValues(fn, group, ctes);

        if (values.Count == 0)
        {
            // MySQL: SUM/AVG/MIN/MAX sobre conjunto vazio (ou tudo NULL) => NULL
            return null;
        }

        return name switch
        {
            "SUM" =>
                values.Sum(_ => _.ToDec()),                 // decimal
            "AVG" =>
                values.Sum(_ => _.ToDec()) / values.Count,  // decimal
            "MIN" =>
                values.Min(_ => _.ToDec()),                 // decimal (consistente pro teu teste)
            "MAX" =>
                values.Max(_ => _.ToDec()),                 // decimal
            _ => null
        };
    }

    private object? EvalAggregate(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var name = fn.Name.ToUpperInvariant();

        // COUNT(DISTINCT ...)
        if (name == "COUNT" && fn.Distinct)
            return EvalCountDistinct(fn, group, ctes);

        // para os outros casos (sem DISTINCT), reaproveita o existente
        var shim = new FunctionCallExpr(fn.Name, fn.Args);
        return EvalAggregate(shim, group, ctes);
    }

    private long EvalCountDistinct(CallExpr fn, EvalGroup group, IDictionary<string, Source> ctes)
    {
        // COUNT(DISTINCT *) não faz sentido no MySQL; se acontecer, trata como COUNT(*)
        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
            return group.Rows.Count;

        // MySQL permite COUNT(DISTINCT expr) e COUNT(DISTINCT expr1, expr2)
        var set = new HashSet<string>(StringComparer.Ordinal);

        foreach (var r in group.Rows)
        {
            // avalia todos os args
            var vals = fn.Args.Select(a => Eval(a, r, null, ctes)).ToArray();

            // Regra prática compatível: se algum é NULL => ignora a linha
            // (na prática, COUNT(DISTINCT ...) ignora NULL; com múltiplas colunas,
            // qualquer NULL “mata” a tupla)
            if (vals.Any(IsNullish))
                continue;

            var key = string.Join("\u001F", vals.Select(v => NormalizeDistinctKey(v, Dialect)));
            set.Add(key);
        }

        return set.Count;
    }

    private static string NormalizeDistinctKey(object? v, ISqlDialect? dialect = null)
    {
        // chave determinística; evita variações idiotas
        if (v is null) return "NULL";
        if (v is DBNull) return "NULL";

        return v switch
        {
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            DateTime dt => dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            bool b => b ? "1" : "0",
            string s => (dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase) == StringComparison.Ordinal
                ? s
                : s.ToUpperInvariant(),
            _ => v.ToString() ?? ""
        };
    }

    private bool TryEvalAggrageteCount(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        out object? value)
    {
        if (name != "COUNT")
        {
            value = null;
            return true;
        }

        if (fn.Args.Count == 0)
        {
            value = (long)group.Rows.Count;
            return false;
        }

        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
        {
            value = (long)group.Rows.Count;
            return false;
        }

        long c = 0;
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v)) c++;
        }
        value = c;
        return false;
    }

    private List<object?> GetNotNullValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var values = new List<object?>();
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v))
                values.Add(v);
        }
        return values;
    }

    private bool ContainsAggregate(SqlSelectQuery q)
    {
        // SelectItems are raw strings; attempt to parse and walk
        foreach (var si in q.SelectItems)
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                var (exprRaw, _) = SplitTrailingAsAlias(si.Raw, si.Alias);
                var e = ParseExpr(exprRaw);
                if (WalkHasAggregate(e)) return true;
            }
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine(e);
                // if expression parser can't parse, assume non-aggregate
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }
        return q.Having is not null
            && WalkHasAggregate(q.Having);
    }

    private static bool WalkHasAggregate(SqlExpr e) => e switch
    {
        // ✅ NOVO: o parser usa CallExpr (COUNT, SUM, etc.)
        CallExpr c => _aggFnsStatic.Contains(c.Name) || c.Args.Any(WalkHasAggregate),

        // já existia
        FunctionCallExpr f => _aggFnsStatic.Contains(f.Name) || f.Args.Any(WalkHasAggregate),
        BinaryExpr b => WalkHasAggregate(b.Left) || WalkHasAggregate(b.Right),
        UnaryExpr u => WalkHasAggregate(u.Expr),
        LikeExpr l => WalkHasAggregate(l.Left) || WalkHasAggregate(l.Pattern),
        InExpr i => WalkHasAggregate(i.Left) || i.Items.Any(WalkHasAggregate),
        IsNullExpr isn => WalkHasAggregate(isn.Expr),

        // ⚠️ cuidado: isso aqui tá “agressivo”
        // EXISTS não é aggregate. Mas você provavelmente usa isso como “precisa de contexto especial”.
        // Vou deixar como está pra não quebrar outras coisas.
        ExistsExpr => true,

        RowExpr r => r.Items.Any(WalkHasAggregate),
        _ => false
    };

    private static readonly HashSet<string> _aggFnsStatic = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG"
    };

    // ---------------- RESOLUTION HELPERS ----------------

    private object? ResolveParam(
        string name)
    {
        // accept @p, :p, ?  (for ?, just take first parameter)
        if (name == "?")
        {
            return _pars.Count > 0 ? ((IDataParameter)_pars[0]!).Value : null;
        }

        var norm = name.TrimStart('@', ':');

        foreach (IDataParameter p in _pars)
        {
            var pn = p.ParameterName?.TrimStart('@', ':');
            if (string.Equals(pn, norm, StringComparison.OrdinalIgnoreCase))
                return p.Value is DBNull ? null : p.Value;
        }

        return null;
    }

    private static object? ResolveIdentifier(
        string name,
        EvalRow row)
    {
        // "a.b"
        var dot = name.IndexOf('.');
        if (dot >= 0)
            return ResolveColumn(name[..dot], name[(dot + 1)..], row);

        // unqualified: try exact match in any source (first wins)
        foreach (var src in row.Sources.Values)
        {
            var key = $"{src.Alias}.{name}";
            if (row.Fields.TryGetValue(key, out var v))
                return v;
        }

        // maybe projected alias
        if (row.Fields.TryGetValue(name, out var v2))
            return v2;

        return null;
    }

    private static object? ResolveColumn(
        string? qualifier,
        string col,
        EvalRow row)
    {
        col = col.NormalizeName();

        Source? src = null;

        if (!string.IsNullOrWhiteSpace(qualifier))
            return ResolveQualifiedColumn(qualifier!, col, row, out src);

        // sem qualifier: tenta unqualified (você já expõe no AddFields)
        if (row.Fields.TryGetValue(col, out var v2))
            return v2;

        // fallback: tenta varrer sources
        foreach (var vlr in row.Sources.Values)
        {
            var key = $"{vlr.Alias}.{col}";
            if (row.Fields.TryGetValue(key, out var v))
                return v;

            var colHit = vlr.ColumnNames.FirstOrDefault(c => c.Equals(col, StringComparison.OrdinalIgnoreCase));
            if (colHit is not null && row.Fields.TryGetValue($"{vlr.Alias}.{colHit}", out v))
                return v;
        }

        return null;


    }
    private static object? ResolveQualifiedColumn(
        string qualifier,
        string col,
        EvalRow row,
        out Source? src)
    {
        qualifier = qualifier.NormalizeName();

        // se vier db.table, pega o "table"
        var lastQual = qualifier.Contains('.')
            ? qualifier.Split('.').Last()
            : qualifier;

        // 1) tenta por alias
        if (!row.Sources.TryGetValue(qualifier, out src) &&
            !row.Sources.TryGetValue(lastQual, out src))
        {
            // 2) tenta por nome físico da tabela
            src = row.Sources.Values.FirstOrDefault(s =>
                s.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase) ||
                s.Name.Equals(lastQual, StringComparison.OrdinalIgnoreCase));
        }

        if (src is null)
        {
            // Fallback: even if Sources doesn't contain the qualifier, fields may already have qualified keys
            var directKey = $"{lastQual}.{col}";
            if (row.Fields.TryGetValue(directKey, out var dv))
                return dv;
            directKey = $"{qualifier}.{col}";
            if (row.Fields.TryGetValue(directKey, out dv))
                return dv;
            return null;
        }

        // wildcard qualificado? (U.*) -> aqui não resolve valor, quem trata é o SELECT planner
        if (col == "*") return null;

        var key = $"{src.Alias}.{col}";
        if (row.Fields.TryGetValue(key, out var v))
            return v;

        var colHit = src.ColumnNames.FirstOrDefault(c => c.Equals(col, StringComparison.OrdinalIgnoreCase));
        if (colHit is not null && row.Fields.TryGetValue($"{src.Alias}.{colHit}", out v))
            return v;

        return null;
    }

    private static TableResultMock ApplyDistinct(
        TableResultMock res,
        ISqlDialect? dialect)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outRows = new List<Dictionary<int, object?>>();

        foreach (var row in res)
        {
            var key = new string[res.Columns.Count];
            for (int i = 0; i < res.Columns.Count; i++)
                key[i] = NormalizeDistinctKey(row.TryGetValue(i, out var v) ? v : null, dialect);

            if (seen.Add(string.Join("\u001F", key)))
                outRows.Add(row);
        }

        res.Clear();
        foreach (var r in outRows) res.Add(r);
        return res;
    }

    private static SqlSelectQuery GetSingleSubqueryOrThrow(
        SubqueryExpr sq,
        string context)
    {
        if (sq.Parsed is null)
            throw new InvalidOperationException(
                $"{context}: SubqueryExpr sem AST parseado (Parsed vazio).");
        return sq.Parsed;
    }

    // ---------------- INTERNAL TYPES ----------------

    internal sealed class Source
    {
        internal ITableMock? Physical { get; }
        private readonly TableResultMock? _result;
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public string Alias { get; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public IReadOnlyList<string> ColumnNames { get; }
        private Source(string name, string alias, ITableMock physical)
        {
            Alias = alias;
            Name = name;
            Physical = physical;
            _result = null;
            ColumnNames = [.. physical.Columns.OrderBy(kv => kv.Value.Index).Select(kv => kv.Key!)];
        }
        private Source(string name, string alias, TableResultMock result)
        {
            Alias = alias;
            Name = name;
            _result = result;
            Physical = null;
            ColumnNames = [.. result.Columns.OrderBy(c => c.ColumIndex).Select(c => c.ColumnAlias)];
        }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public Source WithAlias(string alias)
        {
            if (Physical is not null)
                return FromPhysical(Name, alias, Physical);
            return FromResult(Name, alias, _result!);
        }

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public IEnumerable<Dictionary<string, object?>> Rows()
        {
            if (Physical is not null)
            {
                foreach (var row in Physical)
                {
                    var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    foreach (var col in ColumnNames)
                    {
                        var idx = Physical.Columns[col].Index;
                        dict[$"{Alias}.{col}"] = row.TryGetValue(idx, out var v)
                            ? v
                            : null;
                    }
                    yield return dict;
                }
                yield break;
            }
            if (_result is not null)
            {
                foreach (var row in _result)
                {
                    var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < _result.Columns.Count; i++)
                    {
                        var c = _result.Columns[i];
                        dict[$"{Alias}.{c.ColumnAlias}"] = row.TryGetValue(i, out var v)
                            ? v
                            : null;
                    }
                    yield return dict;
                }
            }
        }

        public IEnumerable<Dictionary<string, object?>> RowsByIndexes(
            IEnumerable<int> indexes)
        {
            if (Physical is null)
                return Rows();

            return EnumerateRowsByIndexes(indexes);
        }

        private IEnumerable<Dictionary<string, object?>> EnumerateRowsByIndexes(
            IEnumerable<int> indexes)
        {
            var emitted = new HashSet<int>();
            foreach (var raw in indexes)
            {
                if (raw < 0 || raw >= Physical!.Count)
                    continue;

                if (!emitted.Add(raw))
                    continue;

                var row = Physical[raw];
                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (var col in ColumnNames)
                {
                    var idx = Physical.Columns[col].Index;
                    dict[$"{Alias}.{col}"] = row.TryGetValue(idx, out var v)
                        ? v
                        : null;
                }

                yield return dict;
            }
        }

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static Source FromPhysical(string tableName, string alias, ITableMock physical)
            => new(tableName, alias, physical);
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static Source FromResult(string tableName, string alias, TableResultMock result)
            => new(tableName, alias, result);
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static Source FromResult(string tableName, TableResultMock result)
            => new(tableName, tableName, result);
    }

    internal sealed record EvalRow(
        Dictionary<string, object?> Fields,
        Dictionary<string, Source> Sources)
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static EvalRow FromProjected(
            TableResultMock res,
            Dictionary<int, object?> row,
            Dictionary<string, int> aliasToIndex)
        {
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in aliasToIndex)
            {
                fields[kv.Key] = row.TryGetValue(kv.Value, out var v) ? v : null;
            }
            return new EvalRow(fields, new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public EvalRow CloneRow()
            => new(new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase),
                   new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase));

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public void AddSource(Source src) => Sources[src.Alias] = src;

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public void AddFields(Dictionary<string, object?> fields)
        {
            foreach (var it in fields)
                Fields[it.Key] = it.Value;

            // also expose unqualified columns (first wins) for convenience
            foreach (var it in fields)
            {
                var dot = it.Key.IndexOf('.');
                if (dot > 0)
                {
                    var col = it.Key[(dot + 1)..];
                    if (!Fields.ContainsKey(col))
                        Fields[col] = it.Value;
                }
            }
        }
    }

    private static EvalRow AttachOuterRow(
        EvalRow inner,
        EvalRow outer)
    {
        // inner vence sempre (regra: o que o subselect produziu não deve ser sobrescrito)
        var merged = inner.CloneRow();

        // 1) Fields do outer entram só se não existirem no inner
        foreach (var it in outer.Fields)
        {
            if (!merged.Fields.ContainsKey(it.Key))
                merged.Fields[it.Key] = it.Value;
        }

        // 2) Também expõe colunas não qualificadas do outer (sem sobrescrever)
        foreach (var it in outer.Fields)
        {
            var dot = it.Key.IndexOf('.');
            if (dot > 0)
            {
                var col = it.Key[(dot + 1)..];
                if (!merged.Fields.ContainsKey(col))
                    merged.Fields[col] = it.Value;
            }
        }

        // 3) Sources: não sobrescreve alias do inner
        foreach (var it in outer.Sources)
        {
            if (!merged.Sources.ContainsKey(it.Key))
                merged.Sources[it.Key] = it.Value;
        }

        return merged;
    }

    private sealed class EvalGroup
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public List<EvalRow> Rows { get; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public EvalGroup(List<EvalRow> rows) => Rows = rows;
    }

    private readonly record struct GroupKey(object?[] Values)
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static readonly IEqualityComparer<GroupKey> Comparer = new GroupKeyComparer();

        private sealed class GroupKeyComparer : IEqualityComparer<GroupKey>
        {
            /// <summary>
            /// Auto-generated summary.
            /// </summary>
            public bool Equals(GroupKey x, GroupKey y)
            {
                if (x.Values.Length != y.Values.Length) return false;
                for (int i = 0; i < x.Values.Length; i++)
                    if (!x.Values[i].EqualsSql(y.Values[i])) return false;
                return true;
            }

            /// <summary>
            /// Auto-generated summary.
            /// </summary>
            public int GetHashCode(GroupKey obj)
            {
                var h = 17;
                foreach (var v in obj.Values)
                    h = (h * 31) + (v?.GetHashCode() ?? 0);
                return h;
            }
        }
    }

    private sealed class ArrayObjectComparer : IComparer<object?>
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int Compare(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            // se ambos são arrays: compara elemento a elemento
            if (x is object?[] xa && y is object?[] ya)
            {
                var len = Math.Min(xa.Length, ya.Length);
                for (int i = 0; i < len; i++)
                {
                    var c = Compare(xa[i], ya[i]); // recursivo: compara elementos
                    if (c != 0) return c;
                }
                return xa.Length.CompareTo(ya.Length);
            }

            // se um é array e outro não, define ordem estável
            if (x is object?[] && y is not object?[]) return 1;
            if (y is object?[] && x is not object?[]) return -1;

            // escalares comuns
            if (x is IComparable xc && y is IComparable)
            {
#pragma warning disable CA1031 // Do not catch general exception types
                try
                {
                    return xc.CompareTo(y);
                }
                catch (Exception e)
                {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                    Console.WriteLine($"{nameof(ArrayObjectComparer)}.{nameof(Compare)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                    Console.WriteLine(e);
                    // cai pro string se tipos diferentes (ex: int vs long vs decimal)
                }
#pragma warning restore CA1031 // Do not catch general exception types
            }

            // fallback estável
            return StringComparer.OrdinalIgnoreCase.Compare(x.ToString(), y.ToString());
        }
    }

    private sealed class ArrayObjectEqualityComparer : IEqualityComparer<object?[]>
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public static readonly ArrayObjectEqualityComparer Instance = new();

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public bool Equals(object?[]? x, object?[]? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x is null || y is null) return false;
            if (x.Length != y.Length) return false;

            for (int i = 0; i < x.Length; i++)
            {
                if (!Equals(x[i], y[i])) return false;
            }
            return true;
        }

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int GetHashCode(object?[] obj)
        {
            unchecked
            {
                int h = 17;
                for (int i = 0; i < obj.Length; i++)
                    h = (h * 31) + (obj[i]?.GetHashCode() ?? 0);
                return h;
            }
        }
    }
}
