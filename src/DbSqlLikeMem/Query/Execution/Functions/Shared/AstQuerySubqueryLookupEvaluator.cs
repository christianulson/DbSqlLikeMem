using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQuerySubqueryLookupEvaluator(
    AstSubqueryEvaluationCache cache,
    QueryExecutionContext context,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
    Func<string, SqlExpr> parseExpr,
    Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> buildFrom,
    Func<SqlTableSource, IDictionary<string, Source>, Source> resolveSource,
    Func<EvalRow, EvalRow, EvalRow> attachOuterRow,
    Func<SqlSelectQuery, IDictionary<string, Source>, EvalRow?, TableResultMock> executeSelect,
    AstQueryPartitionHelper? partitionHelper)
{
    private readonly AstSubqueryEvaluationCache _cache = cache ?? throw new ArgumentNullException(nameof(cache));
    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval = eval ?? throw new ArgumentNullException(nameof(eval));
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr ?? throw new ArgumentNullException(nameof(parseExpr));
    private readonly Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> _buildFrom = buildFrom ?? throw new ArgumentNullException(nameof(buildFrom));
    private readonly Func<SqlTableSource, IDictionary<string, Source>, Source> _resolveSource = resolveSource ?? throw new ArgumentNullException(nameof(resolveSource));
    private readonly Func<EvalRow, EvalRow, EvalRow> _attachOuterRow = attachOuterRow ?? throw new ArgumentNullException(nameof(attachOuterRow));
    private readonly Func<SqlSelectQuery, IDictionary<string, Source>, EvalRow?, TableResultMock> _executeSelect = executeSelect ?? throw new ArgumentNullException(nameof(executeSelect));
    private readonly AstQueryPartitionHelper? _partitionHelper = partitionHelper;

    internal void Clear() => _cache.Clear();

    internal bool TryEvaluateScalarSubqueryFast(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out object? value)
    {
        value = null;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseScalarCountAggregate(exprRaw, _parseExpr, out var countArg))
            return false;

        if (row is null
            && countArg is StarExpr
            && query.Table is not null
            && query.Where is not null
            && TryCountRowsFromSimpleEqualityScan(query, ctes, out var rawEqualityCount))
        {
            value = rawEqualityCount;
            return true;
        }

        var rows = _buildFrom(
            query.Table,
            ctes,
            query.Where,
            query.OrderBy.Count > 0,
            false);

        long count = 0;
        if (row is not null)
        {
            foreach (var candidate in rows)
            {
                var attached = _attachOuterRow(candidate, row);
                if (query.Where is not null && !_eval(query.Where, attached, null, ctes).ToBool())
                    continue;

                if (countArg is StarExpr)
                {
                    count++;
                    continue;
                }

                var evaluated = _eval(countArg, attached, null, ctes);
                if (!AstQueryExecutorBase.IsNullish(evaluated))
                    count++;
            }
        }
        else
        {
            foreach (var candidate in rows)
            {
                if (query.Where is not null && !_eval(query.Where, candidate, null, ctes).ToBool())
                    continue;

                if (countArg is StarExpr)
                {
                    count++;
                    continue;
                }

                var evaluated = _eval(countArg, candidate, null, ctes);
                if (!AstQueryExecutorBase.IsNullish(evaluated))
                    count++;
            }
        }

        value = count;
        return true;
    }

    internal List<object?>? GetOrEvaluateSubqueryFirstColumnValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(operation, sq.Sql, row);
        if (_cache.TryGetFirstColumnValues(cacheKey, out var cachedValues))
            return cachedValues!;

        return _cache.GetOrAddFirstColumnValues(
            cacheKey,
            _ => EvaluateSubqueryFirstColumnValues(sq, operation, row, ctes));
    }

    internal List<object?[]> GetOrEvaluateSubqueryRowValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(operation, sq.Sql, row);
        return _cache.GetOrAddOperationData(
            cacheKey,
            _ => EvaluateSubqueryRowValues(sq, operation, row, ctes));
    }

    internal InSubqueryLookupState GetOrEvaluateInSubqueryLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey("IN_LOOKUP", sq.Sql, row);
        if (_cache.TryGetOperationData(cacheKey, out InSubqueryLookupState? cachedState)
            && cachedState is not null)
            return cachedState;

        return _cache.GetOrAddOperationData(
            cacheKey,
            _ => BuildInSubqueryLookupState(sq, row, ctes));
    }

    internal InSubqueryLookupState GetOrEvaluateInSubqueryRowLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey("IN_ROWS_LOOKUP", sq.Sql, row);
        if (_cache.TryGetOperationData(cacheKey, out InSubqueryLookupState? cachedState)
            && cachedState is not null)
            return cachedState;

        return _cache.GetOrAddOperationData(
            cacheKey,
            _ => BuildInSubqueryRowLookupState(sq, row, ctes));
    }

    internal bool TryCountRowsFromSimpleEqualityScan(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out long count)
    {
        count = 0;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.Distinct
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null
            || query.Table is null
            || query.Where is null)
        {
            return false;
        }

        var src = _resolveSource(query.Table, ctes);
        if (_partitionHelper is null
            || !_partitionHelper.TryCollectColumnEqualities(query.Where, src, out var equalsByColumn)
            || equalsByColumn.Count == 0)
        {
            return false;
        }

        return _context.TryCountRows(src, equalsByColumn, out count);
    }

    private InSubqueryLookupState BuildInSubqueryLookupState(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var values = GetOrEvaluateSubqueryFirstColumnValuesForOperation(sq, SqlConst.IN, row, ctes);
        return AstQueryInSubqueryLookupBuilder.BuildScalarState(values);
    }

    private InSubqueryLookupState BuildInSubqueryRowLookupState(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var values = GetOrEvaluateSubqueryRowValuesForOperation(sq, "IN_ROWS", row, ctes);
        return AstQueryInSubqueryLookupBuilder.BuildRowState(values);
    }

    private List<object?> EvaluateSubqueryFirstColumnValues(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var subqueryResult = _executeSelect(
            sq.Parsed ?? throw new InvalidOperationException(
                $"{operation}: SubqueryExpr sem AST parseado (Parsed vazio)."),
            ctes,
            row);
        var values = new List<object?>(subqueryResult.Count);
        if (subqueryResult is List<Dictionary<int, object?>> rowList)
        {
            for (var i = 0; i < rowList.Count; i++)
                values.Add(rowList[i].TryGetValue(0, out var cell) ? cell : null);
        }
        else
        {
            foreach (var resultRow in subqueryResult)
                values.Add(resultRow.TryGetValue(0, out var cell) ? cell : null);
        }

        return values;
    }

    private List<object?[]> EvaluateSubqueryRowValues(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var subqueryResult = _executeSelect(
            sq.Parsed ?? throw new InvalidOperationException(
                $"{operation}: SubqueryExpr sem AST parseado (Parsed vazio)."),
            ctes,
            row);
        var values = new List<object?[]>(subqueryResult.Count);
        if (subqueryResult is List<Dictionary<int, object?>> rowList)
        {
            for (var rowIndex = 0; rowIndex < rowList.Count; rowIndex++)
            {
                var tuple = new object?[subqueryResult.Columns.Count];
                for (var i = 0; i < tuple.Length; i++)
                    tuple[i] = rowList[rowIndex].TryGetValue(i, out var cell) ? cell : null;

                values.Add(tuple);
            }
        }
        else
        {
            foreach (var resultRow in subqueryResult)
            {
                var tuple = new object?[subqueryResult.Columns.Count];
                for (var i = 0; i < tuple.Length; i++)
                    tuple[i] = resultRow.TryGetValue(i, out var cell) ? cell : null;

                values.Add(tuple);
            }
        }

        return values;
    }
}
