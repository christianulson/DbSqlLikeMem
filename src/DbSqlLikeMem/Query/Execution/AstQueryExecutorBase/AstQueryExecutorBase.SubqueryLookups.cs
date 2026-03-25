namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private bool TryEvaluateScalarSubqueryFast(
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

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseScalarCountAggregate(exprRaw, out var countArg))
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

        var rows = BuildFrom(
            query.Table,
            ctes,
            query.Where,
            hasOrderBy: query.OrderBy.Count > 0,
            hasGroupBy: false);

        long count = 0;
        if (row is not null)
        {
            foreach (var candidate in rows)
            {
                var attached = AttachOuterRow(candidate, row);
                if (query.Where is not null && !Eval(query.Where, attached, group: null, ctes).ToBool())
                    continue;

                if (countArg is StarExpr)
                {
                    count++;
                    continue;
                }

                var evaluated = Eval(countArg, attached, group: null, ctes);
                if (!IsNullish(evaluated))
                    count++;
            }
        }
        else
        {
            foreach (var candidate in rows)
            {
                if (query.Where is not null && !Eval(query.Where, candidate, group: null, ctes).ToBool())
                    continue;

                if (countArg is StarExpr)
                {
                    count++;
                    continue;
                }

                var evaluated = Eval(countArg, candidate, group: null, ctes);
                if (!IsNullish(evaluated))
                    count++;
            }
        }

        value = count;
        return true;
    }

    private bool TryCountRowsFromSimpleEqualityScan(
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

        var src = ResolveSource(query.Table, ctes);
        if (!PartitionHelper.TryCollectColumnEqualities(query.Where, src, out var equalsByColumn)
            || equalsByColumn.Count == 0)
        {
            return false;
        }

        if (equalsByColumn.Count == 1)
        {
            foreach (var kv in equalsByColumn)
            {
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
                        count++;
                    }
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
                count++;
        }

        return true;
    }

    private bool TryParseScalarCountAggregate(string exprRaw, out SqlExpr countArg)
    {
        countArg = default!;
        if (string.IsNullOrWhiteSpace(exprRaw))
            return false;

        var parsed = ParseExpr(exprRaw);
        if (parsed is FunctionCallExpr fn
            && fn.Name.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            && fn.Args.Count == 1)
        {
            countArg = fn.Args[0];
            return true;
        }

        if (parsed is CallExpr call
            && call.Name.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            && call.Args.Count == 1)
        {
            countArg = call.Args[0];
            return true;
        }

        return false;
    }

    private List<object?>? GetOrEvaluateInSubqueryFirstColumnValues(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => GetOrEvaluateSubqueryFirstColumnValuesForOperation(sq, SqlConst.IN, row, ctes);

    private InSubqueryLookupState GetOrEvaluateInSubqueryLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = BuildCorrelatedSubqueryCacheKey("IN_LOOKUP", sq.Sql, row);
        if (_subqueryEvaluationCache.TryGetOperationData(cacheKey, out InSubqueryLookupState? cachedState)
            && cachedState is not null)
            return cachedState;

        return _subqueryEvaluationCache.GetOrAddOperationData(
            cacheKey,
            _ => BuildInSubqueryLookupState(sq, row, ctes));
    }

    private InSubqueryLookupState BuildInSubqueryLookupState(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var values = GetOrEvaluateInSubqueryFirstColumnValues(sq, row, ctes);
        if (values.Count == 0)
            return new InSubqueryLookupState(values, new HashSet<InLookupScalarKey>(), null, null, HasNullCandidate: false);

        var hasNullCandidate = false;
        var scalarCandidates = new HashSet<InLookupScalarKey>();
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsSqlNullLike(value))
            {
                hasNullCandidate = true;
                continue;
            }

            if (!TryCreateInLookupScalarKey(value, null, out var key))
                return new InSubqueryLookupState(values, null, null, null, hasNullCandidate);

            scalarCandidates.Add(key);
        }

        return new InSubqueryLookupState(values, scalarCandidates, null, null, hasNullCandidate);
    }

    private InSubqueryLookupState BuildInSubqueryRowLookupState(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var values = GetOrEvaluateSubqueryRowValuesForOperation(sq, "IN_ROWS", row, ctes);
        if (values.Count == 0)
            return new InSubqueryLookupState([], null, values, new HashSet<string>(StringComparer.Ordinal), HasNullCandidate: false);

        var hasNullCandidate = false;
        var rowCandidates = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (HasNullElement(value))
            {
                hasNullCandidate = true;
                continue;
            }

            if (!TryBuildInLookupCompositeKey(value, out var key))
                return new InSubqueryLookupState([], null, values, null, hasNullCandidate);

            rowCandidates.Add(key);
        }

        return new InSubqueryLookupState([], null, values, rowCandidates, hasNullCandidate);
    }

    /// <summary>
    /// EN: Resolves and caches first-column values for subquery-based operations using operation-specific correlated cache keys.
    /// PT: Resolve e cacheia valores da primeira coluna para operações baseadas em subquery usando chaves de cache correlacionado específicas por operação.
    /// </summary>
    private List<object?> GetOrEvaluateSubqueryFirstColumnValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = BuildCorrelatedSubqueryCacheKey(operation, sq.Sql, row);
        if (_subqueryEvaluationCache.TryGetFirstColumnValues(cacheKey, out var cachedValues))
            return cachedValues!;

        return _subqueryEvaluationCache.GetOrAddFirstColumnValues(
            cacheKey,
            _ => EvaluateSubqueryFirstColumnValues(sq, operation, row, ctes));
    }

    private List<object?[]> GetOrEvaluateSubqueryRowValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = BuildCorrelatedSubqueryCacheKey(operation, sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddOperationData(
            cacheKey,
            _ => EvaluateSubqueryRowValues(sq, operation, row, ctes));
    }

    private List<object?> EvaluateSubqueryFirstColumnValues(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var subqueryResult = ExecuteSelect(GetSingleSubqueryOrThrow(sq, operation), ctes, row);
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
        var subqueryResult = ExecuteSelect(GetSingleSubqueryOrThrow(sq, operation), ctes, row);
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

    /// <summary>
    /// EN: Builds a deterministic cache key for correlated subquery evaluation using operation kind, raw subquery text and normalized outer-row values.
    /// PT: Monta uma chave de cache determinística para avaliação de subquery correlacionada usando tipo de operação, texto bruto da subquery e valores normalizados da linha externa.
    /// </summary>
    private static string BuildCorrelatedSubqueryCacheKey(string operation, string? subquerySql, EvalRow row)
        => AstCorrelatedSubqueryCacheKeyBuilder.Build(operation, subquerySql, row);

    private static SqlSelectQuery LimitToSingleRow(SqlSelectQuery query)
        => query with
        {
            RowLimit = query.RowLimit switch
            {
                SqlLimitOffset limit => new SqlLimitOffset(new LiteralExpr(1m), limit.Offset),
                SqlTop => new SqlTop(new LiteralExpr(1m)),
                SqlFetch fetch => new SqlFetch(new LiteralExpr(1m), fetch.Offset),
                _ => new SqlLimitOffset(new LiteralExpr(1m), null)
            }
        };

    private static bool TryCreateInLookupScalarKey(object? value, ISqlDialect? dialect, out InLookupScalarKey key)
    {
        key = default;

        if (value is null or DBNull)
            return false;

        if (value is byte[] || value is object?[])
            return false;

        if (value is string text)
        {
            key = new InLookupScalarKey("s", NormalizeLookupText(text, dialect));
            return true;
        }

        if (value is char character)
        {
            key = new InLookupScalarKey("s", NormalizeLookupText(character.ToString(), dialect));
            return true;
        }

        if (value is bool boolean)
        {
            key = new InLookupScalarKey("b", boolean ? "1" : "0");
            return true;
        }

        if (TryConvertLookupNumericValue(value, out var numericValue))
        {
            key = new InLookupScalarKey("n", numericValue.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is DateTime dateTime)
        {
            key = new InLookupScalarKey("dt", dateTime.Ticks.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is DateTimeOffset dateTimeOffset)
        {
            key = new InLookupScalarKey("dto", dateTimeOffset.Ticks.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is Guid guid)
        {
            key = new InLookupScalarKey("g", guid.ToString("D"));
            return true;
        }

        var type = value.GetType();
        if (type.IsEnum)
        {
            key = new InLookupScalarKey("e", Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture));
            return true;
        }

        return false;
    }

    private static bool TryConvertLookupNumericValue(object value, out decimal numericValue)
    {
        try
        {
            switch (value)
            {
                case byte byteValue:
                    numericValue = byteValue;
                    return true;
                case sbyte sbyteValue:
                    numericValue = sbyteValue;
                    return true;
                case short shortValue:
                    numericValue = shortValue;
                    return true;
                case ushort ushortValue:
                    numericValue = ushortValue;
                    return true;
                case int intValue:
                    numericValue = intValue;
                    return true;
                case uint uintValue:
                    numericValue = uintValue;
                    return true;
                case long longValue:
                    numericValue = longValue;
                    return true;
                case ulong ulongValue:
                    numericValue = ulongValue;
                    return true;
                case float floatValue:
                    numericValue = Convert.ToDecimal(floatValue, CultureInfo.InvariantCulture);
                    return true;
                case double doubleValue:
                    numericValue = Convert.ToDecimal(doubleValue, CultureInfo.InvariantCulture);
                    return true;
                case decimal decimalValue:
                    numericValue = decimalValue;
                    return true;
                case string textValue when decimal.TryParse(textValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed):
                    numericValue = parsed;
                    return true;
                default:
                    numericValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                    return true;
            }
        }
        catch
        {
            numericValue = default;
            return false;
        }
    }

    private static string NormalizeLookupText(string value, ISqlDialect? dialect)
        => (dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase) == StringComparison.Ordinal
            ? value
            : value.ToUpperInvariant();

    /// <summary>
    /// EN: Clears per-query correlated subquery caches so memoized values do not leak across independent top-level executions.
    /// PT: Limpa caches de subquery correlacionada por consulta para que valores memoizados não vazem entre execuções de topo independentes.
    /// </summary>
    private void ClearSubqueryEvaluationCaches()
        => _subqueryEvaluationCache.Clear();
}
