namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private bool TryEvaluateCorrelatedExistsPreAggregation(
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

        if (!TryBuildCorrelatedLookupCompositeKey(state.KeyPairs, row, ctes, useInnerSide: false, out var outerKey))
            return false;

        exists = state.Presence.Contains(outerKey);
        return true;
    }

    private bool TryBuildCorrelatedExistsLookupState(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out CorrelatedExistsLookupState state)
    {
        state = null!;

        var resolvedSource = ResolveCorrelatedExistsPatternSource(query.Table!, ctes);
        if (!TryGetCorrelatedCountLookupPattern(
                query.Where!,
                resolvedSource,
                out var keyPairs,
                out var innerFilterExpr))
            return false;

        var cacheKey = BuildCorrelatedLookupStateCacheKey(
            "EXISTS_PREAGG",
            query.Table!,
            keyPairs,
            innerFilterExpr);

        if (_subqueryEvaluationCache.TryGetOperationData(cacheKey, out CorrelatedExistsLookupState? cachedState)
            && cachedState is not null)
        {
            state = cachedState;
            return true;
        }

        var built = BuildCorrelatedExistsLookupState(query, ctes, resolvedSource, keyPairs, innerFilterExpr);
        if (built is null)
            return false;

        var cached = _subqueryEvaluationCache.GetOrAddOperationData(
            cacheKey,
            _ => built);

        if (cached is not CorrelatedExistsLookupState cachedState2)
            return false;

        state = cachedState2;
        return true;
    }

    private Source ResolveCorrelatedExistsPatternSource(
        SqlTableSource tableSource,
        IDictionary<string, Source> ctes)
    {
        if (tableSource.Derived is null
            && tableSource.DerivedUnion is null
            && tableSource.TableFunction is null
            && tableSource.Pivot is null
            && tableSource.Unpivot is null
            && !string.IsNullOrWhiteSpace(tableSource.Name)
            && !tableSource.Name!.Equals("DUAL", StringComparison.OrdinalIgnoreCase)
            && !ctes.ContainsKey(tableSource.Name!))
        {
            var cacheKey = string.Concat(
                tableSource.DbName ?? string.Empty,
                '\u001F',
                tableSource.Name?.NormalizeName());

            return BuildCorrelatedExistsPatternSource(cacheKey, tableSource, ctes);
        }

        return ResolveSource(tableSource, ctes);
    }

    private Source BuildCorrelatedExistsPatternSource(
        string cacheKey,
        SqlTableSource tableSource,
        IDictionary<string, Source> ctes)
    {
        var physical = _resolvedBaseTableCache.GetOrAdd(
            cacheKey,
            _ =>
            {
                if (_cnn.TryGetTable(tableSource.Name!, out var table, tableSource.DbName)
                    && table is not null)
                {
                    _cnn.Metrics.IncrementTableHint(tableSource.Name!.NormalizeName());
                    return table;
                }

                return _cnn.GetTable(tableSource.Name!, tableSource.DbName);
            });

        return Source.FromPhysical(
            tableSource.Name!.NormalizeName(),
            tableSource.Alias ?? tableSource.Name!,
            physical,
            tableSource.MySqlIndexHints,
            tableSource.PartitionNames);
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
            where: innerFilterExpr,
            hasOrderBy: false,
            hasGroupBy: false);

        if (innerFilterExpr is not null)
            rows = ApplyRowPredicate(rows, innerFilterExpr, ctes);

        var estimatedCount = GetKnownRowCount(rows);
        var presence = estimatedCount > 0
            ? new HashSet<string>(StringComparer.Ordinal)
            : new HashSet<string>(StringComparer.Ordinal);

        if (rows is List<EvalRow> rowList)
        {
            for (var i = 0; i < rowList.Count; i++)
            {
                if (!TryBuildCorrelatedLookupCompositeKey(keyPairs, rowList[i], ctes, useInnerSide: true, out var compositeKey))
                    return null;

                presence.Add(compositeKey);
            }
        }
        else
        {
            foreach (var candidate in rows)
            {
                if (!TryBuildCorrelatedLookupCompositeKey(keyPairs, candidate, ctes, useInnerSide: true, out var compositeKey))
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
        var sourceRows = IndexHelper.TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var r in sourceRows)
            yield return CreateSourceEvalRow(src, r);
    }

    private static EvalRow CreateSourceEvalRow(Source source, Dictionary<string, object?> fields)
    {
        var sourceColumns = source.ColumnNames;
        var ordinalValues = new object?[sourceColumns.Count];
        var ordinalIndexes = new Dictionary<string, int>(sourceColumns.Count * 3, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < sourceColumns.Count; i++)
        {
            var columnName = sourceColumns[i];
            var qualifiedName = $"{source.Alias}.{columnName}";
            var value = fields.TryGetValue(qualifiedName, out var current) ? current : null;
            ordinalValues[i] = value;
            ordinalIndexes.TryAdd(qualifiedName, i);
            ordinalIndexes.TryAdd(columnName, i);
            if (!source.Name.Equals(source.Alias, StringComparison.OrdinalIgnoreCase))
                ordinalIndexes.TryAdd($"{source.Name}.{columnName}", i);
        }

        var rowSources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
        {
            [source.Alias] = source
        };
        if (!source.Name.Equals(source.Alias, StringComparison.OrdinalIgnoreCase))
            rowSources[source.Name] = source;

        return new EvalRow(fields, rowSources)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes,
            SingleSource = source
        };
    }

    private static string BuildCorrelatedLookupStateCacheKey(
        string operation,
        SqlTableSource table,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        var canonicalSql = BuildCorrelatedLookupCanonicalSql(table, keyPairs, innerFilterExpr);
        if (string.IsNullOrWhiteSpace(canonicalSql))
            return string.Empty;

        return string.Concat(operation, '\u001F', canonicalSql);
    }

    private static string BuildCorrelatedLookupCanonicalSql(
        SqlTableSource table,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        if (table.Name is null)
            return string.Empty;

        var sourceSql = table.Name.NormalizeName();
        if (string.IsNullOrWhiteSpace(sourceSql) || keyPairs.Count == 0)
            return string.Empty;

        var source = table;
        var fragments = new List<string>(keyPairs.Count + 4);

        foreach (var pair in keyPairs)
        {
            var left = NormalizeCorrelatedExistsExpressionForCacheKey(pair.InnerExpr, source);
            var right = NormalizeCorrelatedExistsExpressionForCacheKey(pair.OuterExpr, source);
            if (string.IsNullOrWhiteSpace(left) || string.IsNullOrWhiteSpace(right))
                continue;

            fragments.Add(StringComparer.Ordinal.Compare(left, right) <= 0
                ? $"{left} = {right}"
                : $"{right} = {left}");
        }

        if (innerFilterExpr is not null)
        {
            var conjuncts = new List<SqlExpr>();
            FlattenConjuncts(innerFilterExpr, conjuncts);
            foreach (var conjunct in conjuncts)
            {
                var normalized = NormalizeCorrelatedExistsExpressionForCacheKey(conjunct, source);
                if (!string.IsNullOrWhiteSpace(normalized))
                    fragments.Add(normalized);
            }
        }

        if (fragments.Count == 0)
            return string.Empty;

        fragments.Sort(StringComparer.OrdinalIgnoreCase);
        return $"SELECT 1 FROM {sourceSql} T1 WHERE {string.Join(" AND ", fragments)}";
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

        var rows = BuildFrom(
            query.Table,
            ctes,
            query.Where,
            hasOrderBy: query.OrderBy.Count > 0,
            hasGroupBy: false);

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
                if (Eval(query.Where, AttachOuterRow(candidate, row), group: null, ctes).ToBool())
                {
                    exists = true;
                    return true;
                }
            }

            return true;
        }

        foreach (var candidate in rows)
        {
            if (Eval(query.Where, candidate, group: null, ctes).ToBool())
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

        var src = ResolveSource(query.Table!, ctes);
        if (!PartitionHelper.TryCollectColumnEqualities(query.Where!, src, out var equalsByColumn)
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
}
