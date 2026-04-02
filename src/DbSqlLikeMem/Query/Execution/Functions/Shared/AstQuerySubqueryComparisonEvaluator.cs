using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQuerySubqueryComparisonEvaluator(
    AstSubqueryEvaluationCache cache,
    QueryExecutionContext context,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
    Func<string, SqlExpr> parseExpr,
    Func<SqlTableSource, IDictionary<string, Source>, Source> resolveSource,
    Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> buildFrom,
    Func<EvalRow, EvalRow, EvalRow> attachOuterRow,
    Func<SqlSelectQuery, IDictionary<string, Source>, EvalRow?, TableResultMock> executeSelect,
    AstQueryPartitionHelper? partitionHelper,
    AstQueryIndexHelper indexHelper,
    Func<string, SqlTableSource, IDictionary<string, Source>, Source> buildCorrelatedExistsPatternSource,
    Func<SubqueryExpr, string, EvalRow, IDictionary<string, Source>, List<object?>?> getOrEvaluateSubqueryFirstColumnValuesForOperation)
{
    private readonly AstSubqueryEvaluationCache _cache = cache ?? throw new ArgumentNullException(nameof(cache));
    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval = eval ?? throw new ArgumentNullException(nameof(eval));
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr ?? throw new ArgumentNullException(nameof(parseExpr));
    private readonly Func<SqlTableSource, IDictionary<string, Source>, Source> _resolveSource = resolveSource ?? throw new ArgumentNullException(nameof(resolveSource));
    private readonly Func<SqlTableSource?, IDictionary<string, Source>, SqlExpr?, bool, bool, IEnumerable<EvalRow>> _buildFrom = buildFrom ?? throw new ArgumentNullException(nameof(buildFrom));
    private readonly Func<EvalRow, EvalRow, EvalRow> _attachOuterRow = attachOuterRow ?? throw new ArgumentNullException(nameof(attachOuterRow));
    private readonly Func<SqlSelectQuery, IDictionary<string, Source>, EvalRow?, TableResultMock> _executeSelect = executeSelect ?? throw new ArgumentNullException(nameof(executeSelect));
    private readonly AstQueryPartitionHelper? _partitionHelper = partitionHelper;
    private readonly AstQueryIndexHelper _indexHelper = indexHelper ?? throw new ArgumentNullException(nameof(indexHelper));
    private readonly Func<string, SqlTableSource, IDictionary<string, Source>, Source> _buildCorrelatedExistsPatternSource = buildCorrelatedExistsPatternSource ?? throw new ArgumentNullException(nameof(buildCorrelatedExistsPatternSource));
    private readonly Func<SubqueryExpr, string, EvalRow, IDictionary<string, Source>, List<object?>?> _getOrEvaluateSubqueryFirstColumnValuesForOperation = getOrEvaluateSubqueryFirstColumnValuesForOperation ?? throw new ArgumentNullException(nameof(getOrEvaluateSubqueryFirstColumnValuesForOperation));

    internal bool EvalExists(
        ExistsExpr ex,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var sq = ex.Subquery;
        var query = sq.Parsed ?? throw new InvalidOperationException(
            $"{SqlConst.EXISTS}: SubqueryExpr sem AST parseado (Parsed vazio).");

        if (TryEvaluateCorrelatedExistsPreAggregation(query, row, ctes, out var correlatedExists))
            return correlatedExists;

        var cacheKey = TryBuildCorrelatedExistsPatternCacheKey(query, row, ctes, out var correlatedCacheKey)
            ? correlatedCacheKey
            : AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(SqlConst.EXISTS, sq.Sql, row);

        return _cache.GetOrAddExists(
            cacheKey,
            _ =>
            {
                if (TryEvaluateExistsFast(query, row, ctes, out var exists))
                    return exists;

                var sub = _executeSelect(AstQuerySubqueryLookupSupport.LimitToSingleRow(query), ctes, row);
                return sub.Count > 0;
            });
    }

    internal bool TryEvaluateCorrelatedExistsPreAggregation(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out bool exists)
    {
        exists = false;

        if (query.Table is null
            || query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null
            || query.Where is null)
        {
            return false;
        }

        if (!TryBuildCorrelatedExistsLookupState(query, ctes, out var state))
            return false;

        if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                state.KeyPairs,
                row,
                ctes,
                useInnerSide: false,
                eval: _eval,
                out var outerKey))
            return false;

        exists = state.Presence.Contains(outerKey);
        return true;
    }

    internal object? EvalQuantifiedComparison(
        QuantifiedComparisonExpr quantified,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var leftVal = _eval(quantified.Left, row, group, ctes);
        var candidates = _getOrEvaluateSubqueryFirstColumnValuesForOperation(
            quantified.Subquery,
            BuildQuantifiedComparisonOperationName(quantified),
            row,
            ctes) ?? [];

        return quantified.Quantifier == SqlQuantifier.Any
            ? EvalAnyQuantifiedComparison(quantified.Op, leftVal, candidates)
            : EvalAllQuantifiedComparison(quantified.Op, leftVal, candidates);
    }

    internal bool TryEvaluateCorrelatedCountComparisonFast(
        BinaryExpr expression,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = null;

        if (expression.Op is not (SqlBinaryOp.Eq or SqlBinaryOp.Neq or SqlBinaryOp.Greater or SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less or SqlBinaryOp.LessOrEqual))
            return false;

        if (TryEvaluateCountComparisonAgainstLiteral(expression.Left, expression.Right, expression.Op, row, ctes, out result))
            return true;

        if (TryEvaluateCountComparisonAgainstLiteral(expression.Right, expression.Left, AstQueryComparisonSupport.ReverseComparisonOperator(expression.Op), row, ctes, out result))
            return true;

        return false;
    }

    private bool TryBuildCorrelatedExistsPatternCacheKey(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out string cacheKey)
    {
        cacheKey = string.Empty;

        if (query.Table is null
            || query.Where is null
            || query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (!AstQueryCorrelatedExistsSupport.TryGetCorrelatedCountLookupPattern(
                query.Where,
                AstQueryCorrelatedExistsSupport.ResolveCorrelatedExistsPatternSource(
                    query.Table,
                    ctes,
                    _buildCorrelatedExistsPatternSource,
                    _resolveSource),
                out var keyPairs,
                out var innerFilterExpr))
        {
            return false;
        }

        var canonicalSql = AstQueryCorrelatedExistsSupport.BuildCorrelatedLookupCanonicalSql(query.Table, keyPairs, innerFilterExpr);
        if (string.IsNullOrWhiteSpace(canonicalSql))
            return false;

        var cacheFields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < keyPairs.Count; i++)
        {
            var outerExpr = keyPairs[i].OuterExpr;
            var outerName = AstQueryCorrelatedExistsSupport.FormatCorrelatedLookupCacheFieldName(outerExpr);
            if (string.IsNullOrWhiteSpace(outerName))
                continue;

            cacheFields.TryAdd(outerName, _eval(outerExpr, row, null, ctes));
        }

        var syntheticRow = new EvalRow(
            cacheFields,
            new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));

        cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(SqlConst.EXISTS, canonicalSql, syntheticRow);
        return true;
    }

    private bool TryBuildCorrelatedExistsLookupState(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out CorrelatedExistsLookupState state)
    {
        state = null!;

        var resolvedSource = AstQueryCorrelatedExistsSupport.ResolveCorrelatedExistsPatternSource(
            query.Table!,
            ctes,
            _buildCorrelatedExistsPatternSource,
            _resolveSource);
        if (!AstQueryCorrelatedExistsSupport.TryGetCorrelatedCountLookupPattern(
                query.Where!,
                resolvedSource,
                out var keyPairs,
                out var innerFilterExpr))
        {
            return false;
        }

        var cacheKey = AstQueryCorrelatedExistsSupport.BuildCorrelatedLookupStateCacheKey(
            "EXISTS_PREAGG",
            query.Table!,
            keyPairs,
            innerFilterExpr);

        if (_cache.TryGetOperationData(cacheKey, out CorrelatedExistsLookupState? cachedState)
            && cachedState is not null)
        {
            state = cachedState;
            return true;
        }

        var built = BuildCorrelatedExistsLookupState(query, ctes, resolvedSource, keyPairs, innerFilterExpr);
        if (built is null)
            return false;

        var cached = _cache.GetOrAddOperationData(
            cacheKey,
            _ => built);

        if (cached is not CorrelatedExistsLookupState cachedState2)
            return false;

        state = cachedState2;
        return true;
    }

    private CorrelatedExistsLookupState? BuildCorrelatedExistsLookupState(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        Source resolvedSource,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        var rows = BuildFromResolvedSource(
            resolvedSource,
            query.Table,
            ctes,
            innerFilterExpr,
            false,
            false);

        if (innerFilterExpr is not null)
            rows = rows.Where(r => _eval(innerFilterExpr, r, null, ctes).ToBool());

        var estimatedCount = AstQueryAggregateEvaluator.GetKnownRowCount(rows);
        var presence = estimatedCount > 0
            ? new HashSet<string>(StringComparer.Ordinal)
            : new HashSet<string>(StringComparer.Ordinal);

        if (rows is List<EvalRow> rowList)
        {
            for (var i = 0; i < rowList.Count; i++)
            {
                if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                        keyPairs,
                        rowList[i],
                        ctes,
                        useInnerSide: true,
                        eval: _eval,
                        out var compositeKey))
                    return null;

                presence.Add(compositeKey);
            }
        }
        else
        {
            foreach (var candidate in rows)
            {
                if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                        keyPairs,
                        candidate,
                        ctes,
                        useInnerSide: true,
                        eval: _eval,
                        out var compositeKey))
                    return null;

                presence.Add(compositeKey);
            }
        }

        return new CorrelatedExistsLookupState(presence, keyPairs, innerFilterExpr);
    }

    private IEnumerable<EvalRow> BuildFromResolvedSource(
        Source src,
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        var sourceRows = _indexHelper.TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var r in sourceRows)
            yield return AstQueryRowSourceHelper.CreateSourceEvalRow(src, r);
    }

    private bool TryEvaluateExistsFast(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out bool exists)
    {
        exists = false;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (row is null
            && query.Table is not null
            && query.Where is not null
            && TryExistsFromSimpleEqualityScan(query, ctes, out exists))
        {
            return true;
        }

        var rows = _buildFrom(
            query.Table,
            ctes,
            query.Where,
            query.OrderBy.Count > 0,
            false);

        if (query.Where is null)
        {
            if (rows is ICollection<EvalRow> collection)
            {
                exists = collection.Count > 0;
                return true;
            }

            using var enumerator = rows.GetEnumerator();
            exists = enumerator.MoveNext();
            return true;
        }

        if (row is not null)
        {
            foreach (var candidate in rows)
            {
                if (_eval(query.Where, _attachOuterRow(candidate, row), null, ctes).ToBool())
                {
                    exists = true;
                    return true;
                }
            }

            return true;
        }

        foreach (var candidate in rows)
        {
            if (_eval(query.Where, candidate, null, ctes).ToBool())
            {
                exists = true;
                return true;
            }
        }

        return true;
    }

    private bool TryExistsFromSimpleEqualityScan(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out bool exists)
    {
        exists = false;

        var src = _resolveSource(query.Table!, ctes);
        if (_partitionHelper is null
            || !_partitionHelper.TryCollectColumnEqualities(query.Where!, src, out var equalsByColumn)
            || equalsByColumn.Count == 0)
        {
            return false;
        }

        if (equalsByColumn.Count == 1)
        {
            using var enumerator = equalsByColumn.GetEnumerator();
            if (!enumerator.MoveNext())
                return true;

            var kv = enumerator.Current;
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                return false;
            }

            var qualifiedName = qualifiedColumnName ?? string.Empty;

            foreach (var rawRow in src.Rows())
            {
                if (rawRow.TryGetValue(qualifiedName, out var actualValue)
                    && actualValue.EqualsSql(kv.Value, _context))
                {
                    exists = true;
                    return true;
                }
            }

            return true;
        }

        var resolvedEqualities = new (string QualifiedColumnName, object? Value)[equalsByColumn.Count];
        var resolvedEqualitiesCount = 0;
        foreach (var kv in equalsByColumn)
        {
            if (!src.TryGetQualifiedColumnName(kv.Key, out var qualifiedColumnName)
                || string.IsNullOrWhiteSpace(qualifiedColumnName))
            {
                return false;
            }

            resolvedEqualities[resolvedEqualitiesCount++] = (qualifiedColumnName!, kv.Value);
        }

        foreach (var rawRow in src.Rows())
        {
            var matches = true;
            for (var i = 0; i < resolvedEqualitiesCount; i++)
            {
                var equality = resolvedEqualities[i];
                if (!rawRow.TryGetValue(equality.QualifiedColumnName, out var actualValue)
                    || !actualValue.EqualsSql(equality.Value, _context))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                exists = true;
                return true;
            }
        }

        return true;
    }

    private static string BuildQuantifiedComparisonOperationName(QuantifiedComparisonExpr quantified)
        => quantified.Quantifier == SqlQuantifier.Any
            ? $"QANY_{quantified.Op}"
            : $"QALL_{quantified.Op}";

    private bool EvalAnyQuantifiedComparison(
        SqlBinaryOp op,
        object? leftValue,
        IReadOnlyList<object?> candidates)
    {
        foreach (var candidate in candidates)
        {
            if (EvaluateScalarComparisonTruthValue(op, leftValue, candidate) == SqlTruthValue.True)
                return true;
        }

        return false;
    }

    private bool EvalAllQuantifiedComparison(
        SqlBinaryOp op,
        object? leftValue,
        IReadOnlyList<object?> candidates)
    {
        if (candidates.Count == 0)
            return true;

        var hasUnknown = false;
        foreach (var candidate in candidates)
        {
            var truth = EvaluateScalarComparisonTruthValue(op, leftValue, candidate);
            if (truth == SqlTruthValue.False)
                return false;

            if (truth == SqlTruthValue.Unknown)
                hasUnknown = true;
        }

        return !hasUnknown;
    }

    private SqlTruthValue EvaluateScalarComparisonTruthValue(
        SqlBinaryOp op,
        object? left,
        object? right)
    {
        if (left is null || left is DBNull || right is null || right is DBNull)
            return SqlTruthValue.Unknown;

        var cmp = _context.Compare(left, right);
        return op switch
        {
            SqlBinaryOp.Eq => cmp == 0 ? SqlTruthValue.True : SqlTruthValue.False,
            SqlBinaryOp.Neq => cmp != 0 ? SqlTruthValue.True : SqlTruthValue.False,
            SqlBinaryOp.Greater => cmp > 0 ? SqlTruthValue.True : SqlTruthValue.False,
            SqlBinaryOp.GreaterOrEqual => cmp >= 0 ? SqlTruthValue.True : SqlTruthValue.False,
            SqlBinaryOp.Less => cmp < 0 ? SqlTruthValue.True : SqlTruthValue.False,
            SqlBinaryOp.LessOrEqual => cmp <= 0 ? SqlTruthValue.True : SqlTruthValue.False,
            _ => throw new InvalidOperationException($"Quantified comparison op não suportado: {op}")
        };
    }

    private bool TryEvaluateCountComparisonAgainstLiteral(
        SqlExpr candidate,
        SqlExpr otherSide,
        SqlBinaryOp comparisonOp,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = null;

        if (candidate is not SubqueryExpr subquery)
            return false;

        if (!AstQueryComparisonSupport.TryGetDecimalLiteral(otherSide, out var literalValue))
            return false;

        var query = subquery.Parsed ?? throw new InvalidOperationException(
            "COUNT comparison: SubqueryExpr sem AST parseado (Parsed vazio).");
        if (query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseScalarCountAggregate(exprRaw, _parseExpr, out var countArg) || countArg is not StarExpr)
            return false;

        if (literalValue < 0m)
        {
            result = comparisonOp switch
            {
                SqlBinaryOp.Eq => false,
                SqlBinaryOp.Neq => true,
                SqlBinaryOp.Greater => true,
                SqlBinaryOp.GreaterOrEqual => true,
                SqlBinaryOp.Less => false,
                SqlBinaryOp.LessOrEqual => false,
                _ => throw new InvalidOperationException($"Comparador não suportado para COUNT: {comparisonOp}")
            };

            return true;
        }

        if (literalValue == 0m
            && comparisonOp is SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less)
        {
            result = comparisonOp == SqlBinaryOp.GreaterOrEqual;
            return true;
        }

        if (literalValue == 1m
            && comparisonOp is SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less)
        {
            if (TryEvaluateCorrelatedExistsPreAggregation(query, row, ctes, out var correlatedExistsForOne))
            {
                result = comparisonOp == SqlBinaryOp.GreaterOrEqual
                    ? correlatedExistsForOne
                    : !correlatedExistsForOne;
                return true;
            }
        }

        if (literalValue == 0m
            && TryEvaluateCorrelatedExistsPreAggregation(query, row, ctes, out var correlatedExists))
        {
            result = comparisonOp switch
            {
                SqlBinaryOp.Eq => !correlatedExists,
                SqlBinaryOp.Neq => correlatedExists,
                SqlBinaryOp.Greater => correlatedExists,
                SqlBinaryOp.GreaterOrEqual => true,
                SqlBinaryOp.Less => false,
                SqlBinaryOp.LessOrEqual => !correlatedExists,
                _ => throw new InvalidOperationException($"Comparador não suportado para COUNT: {comparisonOp}")
            };

            return true;
        }

        if (literalValue != 0m)
        {
            if (TryEvaluateCorrelatedCountPreAggregation(query, row, ctes, comparisonOp, literalValue, out result))
                return true;

            return false;
        }

        if (!TryEvaluateExistsFast(query, row, ctes, out var exists))
            return false;

        result = comparisonOp switch
        {
            SqlBinaryOp.Eq => !exists,
            SqlBinaryOp.Neq => exists,
            SqlBinaryOp.Greater => exists,
            SqlBinaryOp.GreaterOrEqual => true,
            SqlBinaryOp.Less => false,
            SqlBinaryOp.LessOrEqual => !exists,
            _ => throw new InvalidOperationException($"Comparador não suportado para COUNT: {comparisonOp}")
        };

        return true;
    }

    private bool TryEvaluateCorrelatedCountPreAggregation(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        SqlBinaryOp comparisonOp,
        decimal literalValue,
        out object? result)
    {
        result = null;

        if (query.Table is null
            || query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null
            || query.Where is null)
        {
            return false;
        }

        if (!TryBuildCorrelatedCountLookupState(query, ctes, out var state))
            return false;

        if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                state.KeyPairs,
                row,
                ctes,
                useInnerSide: false,
                eval: _eval,
                out var outerKey))
            return false;

        var count = state.Counts.TryGetValue(outerKey, out var matchedCount)
            ? matchedCount
            : 0;
        var comparison = _context.CompareSql((decimal)count, literalValue);
        result = comparisonOp switch
        {
            SqlBinaryOp.Eq => comparison == 0,
            SqlBinaryOp.Neq => comparison != 0,
            SqlBinaryOp.Greater => comparison > 0,
            SqlBinaryOp.GreaterOrEqual => comparison >= 0,
            SqlBinaryOp.Less => comparison < 0,
            SqlBinaryOp.LessOrEqual => comparison <= 0,
            _ => throw new InvalidOperationException($"Comparador não suportado para COUNT: {comparisonOp}")
        };

        return true;
    }

    private bool TryBuildCorrelatedCountLookupState(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out CorrelatedCountLookupState state)
    {
        state = null!;

        if (!AstQueryCorrelatedExistsSupport.TryGetCorrelatedCountLookupPattern(
                query.Where!,
                _resolveSource(query.Table!, ctes),
                out var keyPairs,
                out var innerFilterExpr))
            return false;

        var cacheKey = AstQueryCorrelatedExistsSupport.BuildCorrelatedLookupStateCacheKey(
            "COUNT_PREAGG",
            query.Table!,
            keyPairs,
            innerFilterExpr);

        if (_cache.TryGetOperationData(cacheKey, out CorrelatedCountLookupState? cachedState)
            && cachedState is not null)
        {
            state = cachedState;
            return true;
        }

        var built = BuildCorrelatedCountLookupState(query, ctes, keyPairs, innerFilterExpr);
        if (built is null)
            return false;

        var cached = _cache.GetOrAddOperationData(
            cacheKey,
            _ => built);

        if (cached is not CorrelatedCountLookupState cachedState2)
            return false;

        state = cachedState2;
        return true;
    }

    private CorrelatedCountLookupState? BuildCorrelatedCountLookupState(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        var rows = _buildFrom(
            query.Table,
            ctes,
            innerFilterExpr,
            false,
            false);

        if (innerFilterExpr is not null)
            rows = rows.Where(r => _eval(innerFilterExpr, r, null, ctes).ToBool());

        var estimatedCount = AstQueryAggregateEvaluator.GetKnownRowCount(rows);
        var compositeCounts = estimatedCount > 0
            ? new Dictionary<string, int>(estimatedCount, StringComparer.Ordinal)
            : new Dictionary<string, int>(StringComparer.Ordinal);
        if (rows is List<EvalRow> rowList)
        {
            for (var i = 0; i < rowList.Count; i++)
            {
                if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                        keyPairs,
                        rowList[i],
                        ctes,
                        useInnerSide: true,
                        eval: _eval,
                        out var compositeKey))
                    return null;

                if (compositeCounts.TryGetValue(compositeKey, out var currentCount))
                    compositeCounts[compositeKey] = currentCount + 1;
                else
                    compositeCounts[compositeKey] = 1;
            }
        }
        else
        {
            foreach (var candidate in rows)
            {
                if (!AstQuerySubqueryLookupSupport.TryBuildCorrelatedLookupCompositeKey(
                        keyPairs,
                        candidate,
                        ctes,
                        useInnerSide: true,
                        eval: _eval,
                        out var compositeKey))
                    return null;

                if (compositeCounts.TryGetValue(compositeKey, out var currentCount))
                    compositeCounts[compositeKey] = currentCount + 1;
                else
                    compositeCounts[compositeKey] = 1;
            }
        }

        return new CorrelatedCountLookupState(compositeCounts, keyPairs, innerFilterExpr);
    }
}
