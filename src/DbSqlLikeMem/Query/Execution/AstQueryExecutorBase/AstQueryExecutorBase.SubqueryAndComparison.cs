namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private bool EvalExists(
        ExistsExpr ex,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var sq = ex.Subquery;
        var query = GetSingleSubqueryOrThrow(sq, SqlConst.EXISTS);

        if (TryEvaluateCorrelatedExistsPreAggregation(query, row, ctes, out var correlatedExists))
        {
            return correlatedExists;
        }

        var cacheKey = TryBuildCorrelatedExistsPatternCacheKey(query, row, ctes, out var correlatedCacheKey)
            ? correlatedCacheKey
            : BuildCorrelatedSubqueryCacheKey(SqlConst.EXISTS, sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddExists(
            cacheKey,
            _ =>
            {
                if (TryEvaluateExistsFast(query, row, ctes, out var exists))
                {
                    return exists;
                }

                var sub = ExecuteSelect(LimitToSingleRow(query), ctes, row);
                return sub.Count > 0;
            });
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

        if (!TryGetCorrelatedCountLookupPattern(
                query.Where,
                ResolveCorrelatedExistsPatternSource(query.Table, ctes),
                out var keyPairs,
                out var innerFilterExpr))
        {
            return false;
        }

        var canonicalSql = BuildCorrelatedLookupCanonicalSql(query.Table, keyPairs, innerFilterExpr);
        if (string.IsNullOrWhiteSpace(canonicalSql))
        {
            return false;
        }

        var cacheFields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < keyPairs.Count; i++)
        {
            var outerExpr = keyPairs[i].OuterExpr;
            var outerName = FormatCorrelatedLookupCacheFieldName(outerExpr);
            if (string.IsNullOrWhiteSpace(outerName))
                continue;

            cacheFields.TryAdd(outerName, Eval(outerExpr, row, group: null, ctes));
        }

        var syntheticRow = new EvalRow(
            cacheFields,
            new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));

        cacheKey = BuildCorrelatedSubqueryCacheKey(SqlConst.EXISTS, canonicalSql, syntheticRow);
        return true;
    }

    private static string FormatCorrelatedLookupCacheFieldName(SqlExpr expr)
        => expr switch
        {
            IdentifierExpr id => id.Name.NormalizeName(),
            ColumnExpr column when string.IsNullOrWhiteSpace(column.Qualifier)
                => column.Name.NormalizeName(),
            ColumnExpr column => $"{column.Qualifier.NormalizeName()}.{column.Name.NormalizeName()}",
            _ => SqlExprPrinter.Print(expr).NormalizeName()
        };

    private static string NormalizeCorrelatedExistsPredicateForCacheKey(
        SqlExpr predicate,
        SqlTableSource source)
    {
        var conjuncts = new List<SqlExpr>();
        FlattenConjuncts(predicate, conjuncts);
        if (conjuncts.Count == 0)
            return string.Empty;

        var segments = new List<string>(conjuncts.Count);
        for (var i = 0; i < conjuncts.Count; i++)
        {
            var segment = NormalizeCorrelatedExistsConjunctForCacheKey(conjuncts[i], source);
            if (!string.IsNullOrWhiteSpace(segment))
                segments.Add(segment);
        }

        if (segments.Count == 0)
            return string.Empty;

        segments.Sort(StringComparer.OrdinalIgnoreCase);
        return segments.Count == 1
            ? segments[0]
            : string.Join(" AND ", segments);
    }

    private static string NormalizeCorrelatedExistsConjunctForCacheKey(
        SqlExpr conjunct,
        SqlTableSource source)
    {
        if (conjunct is BinaryExpr binary && binary.Op == SqlBinaryOp.Eq)
        {
            var left = NormalizeCorrelatedExistsExpressionForCacheKey(binary.Left, source);
            var right = NormalizeCorrelatedExistsExpressionForCacheKey(binary.Right, source);
            return StringComparer.Ordinal.Compare(left, right) <= 0
                ? $"{left} = {right}"
                : $"{right} = {left}";
        }

        return NormalizeCorrelatedExistsExpressionForCacheKey(conjunct, source);
    }

    private static string NormalizeCorrelatedExistsExpressionForCacheKey(
        SqlExpr expr,
        SqlTableSource source)
    {
        var text = SqlExprPrinter.Print(expr);
        text = ReplaceIdentifierQualifierForCacheKey(text, source.Alias, "T1");
        text = ReplaceIdentifierQualifierForCacheKey(text, source.Name, "T1");
        return text;
    }

    private static string ReplaceIdentifierQualifierForCacheKey(
        string sql,
        string? qualifier,
        string replacement)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(qualifier) || string.IsNullOrWhiteSpace(replacement))
            return sql;

        var safeQualifier = qualifier!;
        var sb = new StringBuilder(sql.Length);
        for (var i = 0; i < sql.Length; i++)
        {
            if (IsIdentifierQualifierReferenceAt(sql, i, safeQualifier))
            {
                sb.Append(replacement);
                sb.Append('.');
                i += safeQualifier.Length;
                continue;
            }

            sb.Append(sql[i]);
        }

        return sb.ToString();
    }

    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    private static bool IsIdentifierQualifierReferenceAt(
        string sql,
        int startIndex,
        string qualifier)
    {
        if (startIndex < 0 || startIndex >= sql.Length || string.IsNullOrWhiteSpace(qualifier))
            return false;

        if (startIndex + qualifier.Length >= sql.Length)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        for (var i = 0; i < qualifier.Length; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(qualifier[i]))
                return false;
        }

        return sql[startIndex + qualifier.Length] == '.';
    }

    /// <summary>
    /// EN: Evaluates quantified comparison expressions (`ANY`/`ALL`) against the first column of a subquery result using SQL three-valued logic semantics.
    /// PT: Avalia expressões de comparação quantificada (`ANY`/`ALL`) contra a primeira coluna do resultado de subquery usando semântica SQL de três valores.
    /// </summary>
    private bool EvalQuantifiedComparison(
        QuantifiedComparisonExpr quantified,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var leftVal = Eval(quantified.Left, row, group, ctes);
        var candidates = GetOrEvaluateSubqueryFirstColumnValuesForOperation(
            quantified.Subquery,
            BuildQuantifiedComparisonOperationName(quantified),
            row,
            ctes);

        return quantified.Quantifier == SqlQuantifier.Any
            ? EvalAnyQuantifiedComparison(quantified.Op, leftVal, candidates)
            : EvalAllQuantifiedComparison(quantified.Op, leftVal, candidates);
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

    /// <summary>
    /// EN: Evaluates scalar comparison into SQL truth value (`TRUE`/`FALSE`/`UNKNOWN`) for quantified comparison semantics.
    /// PT: Avalia comparação escalar em valor lógico SQL (`TRUE`/`FALSE`/`UNKNOWN`) para semântica de comparação quantificada.
    /// </summary>
    private SqlTruthValue EvaluateScalarComparisonTruthValue(
        SqlBinaryOp op,
        object? left,
        object? right)
    {
        if (left is null || left is DBNull || right is null || right is DBNull)
            return SqlTruthValue.Unknown;

        var cmp = left.Compare(right, _context);
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
}
