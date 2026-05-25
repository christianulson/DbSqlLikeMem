using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQuerySubqueryLookupEvaluator(
    AstSubqueryEvaluationCache cache,
    QueryExecutionContext context,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
    Func<string, SqlExpr> parseExpr,
    Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> buildFrom,
    Func<SqlTableSource, IDictionary<string, Source>, Source> resolveSource,
    AstQueryIndexHelper? indexHelper,
    Func<EvalRow, EvalRow, EvalRow> attachOuterRow,
    Func<SqlQueryBase, IDictionary<string, Source>?, EvalRow?, TableResultMock> executeQuery,
    AstQueryPartitionHelper? partitionHelper)
{
    private readonly AstSubqueryEvaluationCache _cache = cache ?? throw new ArgumentNullException(nameof(cache));
    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval = eval ?? throw new ArgumentNullException(nameof(eval));
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr ?? throw new ArgumentNullException(nameof(parseExpr));
    private readonly Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> _buildFrom = buildFrom ?? throw new ArgumentNullException(nameof(buildFrom));
    private readonly Func<SqlTableSource, IDictionary<string, Source>, Source> _resolveSource = resolveSource ?? throw new ArgumentNullException(nameof(resolveSource));
    private readonly AstQueryIndexHelper? _indexHelper = indexHelper;
    private readonly Func<EvalRow, EvalRow, EvalRow> _attachOuterRow = attachOuterRow ?? throw new ArgumentNullException(nameof(attachOuterRow));
    private readonly Func<SqlQueryBase, IDictionary<string, Source>?, EvalRow?, TableResultMock> _executeQuery = executeQuery ?? throw new ArgumentNullException(nameof(executeQuery));
    private readonly AstQueryPartitionHelper? _partitionHelper = partitionHelper;

    internal void Clear() => _cache.Clear();

    internal bool TryEvaluateScalarSubqueryFast(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out object? value)
    {
        value = null;

        if (!CanUseScalarCountFastPath(query))
            return false;

        if (query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseScalarCountAggregate(exprRaw, _parseExpr, out var countArg, out var isCountBig))
            return false;

        if (TryEvaluateScalarCountFastPath(query, row, ctes, countArg, isCountBig, out value))
            return true;

        var rows = _buildFrom(
            query.Table,
            ctes,
            query.Where,
            query.OrderBy.Count > 0,
            false);

        var count = CountScalarSubqueryRows(query, row, ctes, countArg, rows);
        value = AstQueryAggregateEvaluator.CreateCountAggregateResult(_context, isCountBig, count);
        return true;
    }

    private static bool CanUseScalarCountFastPath(SqlSelectQuery query)
        => query.Ctes.Count == 0
           && query.Joins.Count == 0
           && !query.HasDistinctClause()
           && query.GroupBy.Count == 0
           && query.Having is null
           && query.RowLimit is null
           && query.ForJson is null;

    private bool TryEvaluateScalarCountFastPath(
        SqlSelectQuery query,
        EvalRow? row,
        IDictionary<string, Source> ctes,
        SqlExpr countArg,
        bool isCountBig,
        out object? value)
    {
        value = null;

        if (row is not null
            || countArg is not StarExpr
            || query.Table is null
            || query.Where is null)
        {
            return false;
        }

        if (!TryCountRowsFromSimpleEqualityScan(query, ctes, out var rawEqualityCount))
        {
            return false;
        }

        value = AstQueryAggregateEvaluator.CreateCountAggregateResult(_context, isCountBig, rawEqualityCount);
        return true;
    }

    private long CountScalarSubqueryRows(
        SqlSelectQuery query,
        EvalRow? row,
        IDictionary<string, Source> ctes,
        SqlExpr countArg,
        IEnumerable<EvalRow> rows)
    {
        if (row is not null)
        {
            return CountRowsWithOuterRow(query, row, ctes, countArg, rows);
        }

        return CountRowsWithoutOuterRow(query, ctes, countArg, rows);
    }

    private long CountRowsWithOuterRow(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        SqlExpr countArg,
        IEnumerable<EvalRow> rows)
    {
        var count = 0L;
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
            if (!IsNullish(evaluated))
                count++;
        }

        return count;
    }

    private long CountRowsWithoutOuterRow(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        SqlExpr countArg,
        IEnumerable<EvalRow> rows)
    {
        var count = 0L;
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
            if (!IsNullish(evaluated))
                count++;
        }

        return count;
    }

    internal List<object?>? GetOrEvaluateSubqueryFirstColumnValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(operation, sq.Sql, row);
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
            || query.HasDistinctClause()
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

        if (_indexHelper is not null)
        {
            if (_indexHelper.TryCountRowsFromIndex(
                src,
                query.Table,
                query.Where,
                hasOrderBy: query.OrderBy.Count > 0,
                hasGroupBy: false,
                out count))
            {
                return true;
            }
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
        var query = sq.Parsed ?? throw new InvalidOperationException(
            $"{operation}: SubqueryExpr sem AST parseado (Parsed vazio).");

        if (query is SqlSelectQuery selectQuery
            && TryEvaluateSimpleFirstColumnValues(selectQuery, row, ctes, out var values))
            return values;

        var subqueryResult = _executeQuery(query, ctes, row);
        var rowCount = subqueryResult.Count;
        values = new List<object?>(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            var resultRow = subqueryResult[i];
            values.Add(resultRow.TryGetValue(0, out var cell) ? cell : null);
        }

        return values;
    }

    private bool TryEvaluateSimpleFirstColumnValues(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out List<object?> values)
    {
        if (row is not null
            || query.Table is null
            || query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.HasDistinctClause()
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null
            || query.SelectItems.Count != 1)
        {
            values = [];
            return false;
        }

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        SqlExpr selectExpr;
        try
        {
            selectExpr = _parseExpr(exprRaw);
        }
        catch
        {
            values = [];
            return false;
        }

        if (selectExpr is not ColumnExpr && selectExpr is not IdentifierExpr)
        {
            values = [];
            return false;
        }

        var rows = BuildFromResolvedSource(
            _resolveSource(query.Table, ctes),
            query.Table,
            ctes,
            query.Where,
            query.OrderBy.Count > 0,
            false);

        if (rows is ICollection<EvalRow> collection)
            values = new List<object?>(collection.Count);
        else
            values = [];

        foreach (var candidate in rows)
        {
            values.Add(_eval(selectExpr, candidate, null, ctes));
        }

        return true;
    }

    private IEnumerable<EvalRow> BuildFromResolvedSource(
        Source src,
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        var sourceRows = _indexHelper?.TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var row in sourceRows)
            yield return AstQueryRowSourceHelper.CreateSourceEvalRow(src, row);
    }

    private List<object?[]> EvaluateSubqueryRowValues(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var subqueryResult = _executeQuery(
            sq.Parsed ?? throw new InvalidOperationException(
                $"{operation}: SubqueryExpr sem AST parseado (Parsed vazio)."),
            ctes,
            row);
        var columnCount = subqueryResult.Columns.Count;
        var rowCount = subqueryResult.Count;
        var values = new List<object?[]>(rowCount);
        for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            var resultRow = subqueryResult[rowIndex];
            var tuple = new object?[columnCount];
            for (var i = 0; i < columnCount; i++)
                tuple[i] = resultRow.TryGetValue(i, out var cell) ? cell : null;

            values.Add(tuple);
        }

        return values;
    }
}
