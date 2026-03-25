using static DbSqlLikeMem.AstQueryGeneralScalarFunctionEvaluator;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Executes the Pratt-based AST (<see cref="SqlSelectQuery"/>) against <see cref="TableMock"/> tables.
/// PT: Executa o AST baseado em Pratt (<see cref="SqlSelectQuery"/>) contra tabelas <see cref="TableMock"/>.
///
/// EN: The executor currently covers SELECT and WITH queries only, matching the scope of <see cref="SqlQueryParser"/>.
/// PT: O executor atualmente cobre apenas consultas SELECT e WITH, acompanhando o escopo de <see cref="SqlQueryParser"/>.
/// </summary>
internal abstract class AstQueryExecutorBase(QueryExecutionContext context)
    : IAstQueryExecutor
{
    private const int TemporalParseCacheSoftLimit = 1024;
    internal const int JsonPathParseCacheSoftLimit = 512;
    private static readonly Regex _sqlCalcFoundRowsRegex = new(
        @"\bSQL_CALC_FOUND_ROWS\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _dateModifierRegex = new(
        @"^(?<amount>[+-]?\d+)\s*(?<unit>\w+)s?$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex _intervalLiteralRegex = new(
        @"^(?<num>-?\d+(?:\.\d+)?)\s*(?<unit>[a-zA-Z]+)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    internal static readonly Random _sharedRandom = new();
    internal static readonly object _randomLock = new();
    internal static readonly DateTime _unixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTimeParseCacheEntry> _dateTimeParseCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTimeParseCacheEntry> _dateTimeExactParseCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTimeOffsetParseCacheEntry> _dateTimeOffsetParseCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTimeOffsetParseCacheEntry> _dateTimeOffsetExactParseCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, TimeSpanParseCacheEntry> _timeSpanParseCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, JsonPathTokenCacheEntry> _jsonPathTokenCache = new(StringComparer.Ordinal);
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, JsonPathTokenCacheEntry> _postgresJsonPathTokenCache = new(StringComparer.Ordinal);
    internal static readonly object _uuidShortCounterLock = new();
    internal static long _uuidShortCounter;

    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private DbConnectionMockBase _cnn => _context.Connection;
    private IDataParameterCollection _pars => _context.DbParameters;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ITableMock> _resolvedBaseTableCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly DateTime _evaluationLocalNow = DateTime.Now;
    private readonly DateTime _evaluationUtcNow = DateTime.UtcNow;
    private readonly long _evaluationUnixTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private sealed record InSubqueryLookupState(
        List<object?> Values,
        HashSet<InLookupScalarKey>? ScalarCandidates,
        List<object?[]>? RowValues,
        HashSet<string>? RowCandidates,
        bool HasNullCandidate);

    private sealed record CorrelatedCountLookupState(
        IReadOnlyDictionary<string, int> Counts,
        IReadOnlyList<CorrelatedLookupKeyPair> KeyPairs,
        SqlExpr? InnerFilterExpr);

    private sealed record CorrelatedExistsLookupState(
        HashSet<string> Presence,
        IReadOnlyList<CorrelatedLookupKeyPair> KeyPairs,
        SqlExpr? InnerFilterExpr);

    private sealed record CorrelatedLookupKeyPair(
        SqlExpr InnerExpr,
        SqlExpr OuterExpr);

    private readonly record struct InLookupScalarKey(string Kind, string Value);
    private readonly AstSubqueryEvaluationCache _subqueryEvaluationCache = new();
    private readonly Stack<IReadOnlyDictionary<string, object?>> _localParameterScopes = new();
    private AstQueryJoinService? _joinService;
    private AstQuerySourceResolver? _sourceResolver;
    private AstQueryPivotHelper? _pivotHelper;
    private AstQueryHavingHelper? _havingHelper;
    private AstQueryPartitionHelper? _partitionHelper;
    private AstQueryIndexHelper? _indexHelper;
    private AstQueryFunctionEvaluator? _functionEvaluator;
    private AstQueryCastConversionFamilyEvaluator? _castConversionFamilyEvaluator;
    private AstQueryCastStringAndDateTailEvaluator? _castStringAndDateTailEvaluator;
    private AstQueryGeneralSystemAndJsonFunctionEvaluator? _generalSystemAndJsonFunctionEvaluator;
    private AstQueryGeneralScalarFunctionEvaluator? _generalScalarFunctionEvaluator;
    private AstQuerySqlServerDatabaseFunctionEvaluator? _sqlServerDatabaseFunctionEvaluator;
    private AstQuerySqlServerIdentityFunctionEvaluator? _sqlServerIdentityFunctionEvaluator;
    private AstQuerySqlServerUtilityFunctionEvaluator? _sqlServerUtilityFunctionEvaluator;
    private AstQuerySqlServerSessionFunctionEvaluator? _sqlServerSessionFunctionEvaluator;
    private ISqlDialect? Dialect => _context.Dialect;
    private AstQueryJoinService JoinService
        => _joinService ??= new AstQueryJoinService(
            resolveSource: ResolveSource,
            buildMySqlIndexHintPlan: AstQueryIndexHelper.BuildMySqlIndexHintPlan,
            evalJoinPredicate: (expr, row, ctes) => Eval(expr, row, group: null, ctes).ToBool());
    private AstQuerySourceResolver SourceResolver
        => _sourceResolver ??= new AstQuerySourceResolver(
            _cnn,
            () => Dialect,
            evalExpression: Eval,
            executeSelect: (select, ctes, outerRow) => ExecuteSelect(select, ctes, outerRow),
            executeUnion: ExecuteUnion);

    private AstQueryPivotHelper PivotHelper
        => _pivotHelper ??= new AstQueryPivotHelper(
            () => Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para transformação de tabelas."),
            ParseExpr,
            Eval,
            CreateSourceEvalRow);

    private AstQueryHavingHelper HavingHelper
        => _havingHelper ??= new AstQueryHavingHelper(
            () => Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para HAVING."),
            ParseExpr,
            WalkHasAggregate,
            SplitTrailingAsAlias);

    private AstQueryPartitionHelper PartitionHelper
        => _partitionHelper ??= new AstQueryPartitionHelper(
            expr =>
            {
                switch (expr)
                {
                    case LiteralExpr l:
                        return (true, l.Value);
                    case ParameterExpr p:
                        return (true, ResolveParam(p.Name));
                    default:
                        return (false, null);
                }
            });

    private bool TryGetYearPartitionFunctionInfo(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out SqlExpr columnExpr)
        => PartitionHelper.TryGetYearPartitionFunctionInfo(expr, src, partitionedColumnName, out columnExpr);

    private static bool TryResolvePartitionYearConstant(object? rawValue, out int year)
        => AstQueryPartitionHelper.TryResolvePartitionYearConstant(rawValue, out year);

    private AstQueryIndexHelper IndexHelper
        => _indexHelper ??= new AstQueryIndexHelper(
            collectColumnEqualities: (where, src) => PartitionHelper.TryCollectColumnEqualities(where, src, out var equalities) ? equalities : null,
            incrementIndexLookupMetric: () =>
            {
                if (_cnn.Metrics.Enabled)
                    _cnn.Metrics.IndexLookups++;
            },
            incrementIndexHintMetric: indexName =>
            {
                if (_cnn.Metrics.Enabled)
                    _cnn.Metrics.IncrementIndexHint(indexName);
            },
            recordPrimaryKeyHintMetric: TryRecordPrimaryKeyHintMetric);

    private AstQueryFunctionEvaluator FunctionEvaluator
        => _functionEvaluator ??= new AstQueryFunctionEvaluator(
            isAggregateFunction: AggregateFunctionCatalog.Contains,
            evalAggregate: EvalAggregate,
            tryEvalUserDefinedScalarFunction: TryEvalUserDefinedScalarFunction,
            tryEvalBoundScalarFunction: TryEvalBoundScalarFunction,
            tryEvalNonSqlServerScalarFunctionFamily: TryEvalNonSqlServerScalarFunctionFamily,
            tryEvalSqlServerAndCompatibilityFunctionFamily: TryEvalSqlServerAndCompatibilityFunctionFamily,
            tryEvalGeneralScalarFunctionFamily: TryEvalGeneralScalarFunctionFamily,
            tryEvalCastStringAndDateTail: TryEvalCastStringAndDateTail);

    private AstQueryCastConversionFamilyEvaluator CastConversionFamilyEvaluator
        => _castConversionFamilyEvaluator ??= new AstQueryCastConversionFamilyEvaluator(
            tryEvalJsonAndNumberFunctions: TryEvalJsonAndNumberFunctions,
            evalTryCast: EvalTryCast,
            evalParseFunction: EvalParseFunction,
            evalCast: EvalCast);

    private AstQueryCastStringAndDateTailEvaluator CastStringAndDateTailEvaluator
        => _castStringAndDateTailEvaluator ??= new AstQueryCastStringAndDateTailEvaluator(
            tryEvalCastConversionFamily: TryEvalCastConversionFamily,
            tryEvalCastConcatAndStringTail: TryEvalCastConcatAndStringTail,
            tryEvalCastDateTail: TryEvalCastDateTail);

    private AstQueryGeneralSystemAndJsonFunctionEvaluator GeneralSystemAndJsonFunctionEvaluator
        => _generalSystemAndJsonFunctionEvaluator ??= new AstQueryGeneralSystemAndJsonFunctionEvaluator(
            tryEvalSessionContextFunction: TryEvalSessionContextFunction,
            tryEvalJsonUtilityFunctions: TryEvalJsonUtilityFunctions,
            tryEvalSqliteSystemFunctions: GeneralScalarFunctionEvaluator.TryEvalSqliteSystemFunctions,
            tryEvalSqliteJsonFunctions: GeneralScalarFunctionEvaluator.TryEvalSqliteJsonFunctions);

    private AstQueryGeneralScalarFunctionEvaluator GeneralScalarFunctionEvaluator
        => _generalScalarFunctionEvaluator ??= new AstQueryGeneralScalarFunctionEvaluator(_cnn);

    private AstQuerySqlServerDatabaseFunctionEvaluator SqlServerDatabaseFunctionEvaluator
        => _sqlServerDatabaseFunctionEvaluator ??= new AstQuerySqlServerDatabaseFunctionEvaluator(
            getDialect: () => Dialect,
            resolveDatabaseProperty: TryResolveSqlServerDatabaseProperty,
            resolveDatabasePrincipalId: TryResolveSqlServerDatabasePrincipalId,
            resolveColumnProperty: TryResolveSqlServerColumnProperty,
            resolveColumnLength: TryResolveSqlServerColumnLength,
            resolveColumnName: TryResolveSqlServerColumnName,
            resolveObjectId: TryResolveSqlServerObjectId,
            resolveObjectProperty: TryResolveSqlServerObjectProperty,
            resolveObjectName: TryResolveSqlServerObjectName,
            resolveObjectSchemaName: TryResolveSqlServerObjectSchemaName,
            resolveTypeProperty: TryResolveSqlServerTypeProperty,
            getDatabaseName: () => _cnn.Database);

    private AstQuerySqlServerIdentityFunctionEvaluator SqlServerIdentityFunctionEvaluator
        => _sqlServerIdentityFunctionEvaluator ??= new AstQuerySqlServerIdentityFunctionEvaluator(
            getDialect: () => Dialect,
            getLastInsertId: _cnn.GetLastInsertId,
            resolveSystemTypeId: TryResolveSqlServerSystemTypeId,
            resolveSystemTypeName: TryResolveSqlServerSystemTypeName);

    private AstQuerySqlServerUtilityFunctionEvaluator SqlServerUtilityFunctionEvaluator
        => _sqlServerUtilityFunctionEvaluator ??= new AstQuerySqlServerUtilityFunctionEvaluator(
            getDialect: () => Dialect,
            tryConvertNumericToDecimal: TryConvertNumericToDecimal,
            tryCoerceDateTime: TryCoerceDateTime,
            tryParseOffset: TryParseOffset,
            tryParseCachedDateTimeOffset: TryParseCachedDateTimeOffset);

    private AstQuerySqlServerSessionFunctionEvaluator SqlServerSessionFunctionEvaluator
        => _sqlServerSessionFunctionEvaluator ??= new AstQuerySqlServerSessionFunctionEvaluator(
            getDialect: () => Dialect,
            getContextInfo: _cnn.GetContextInfo,
            hasActiveTransaction: () => _cnn.HasActiveTransaction,
            tryResolveSqlServerRoleMembership: TryResolveSqlServerRoleMembership,
            tryResolveSqlServerServerRoleMembership: TryResolveSqlServerServerRoleMembership);

    private sealed class WindowPartitionExecutionContext(
        AstQueryExecutorBase owner,
        List<EvalRow> part,
        WindowSpec spec,
        IDictionary<string, Source> ctes,
        Dictionary<EvalRow, object?[]>? precomputedOrderValuesByRow)
    {
        private readonly AstQueryExecutorBase _owner = owner;
        private readonly WindowSpec _spec = spec;
        private readonly IDictionary<string, Source> _ctes = ctes;
        private Dictionary<EvalRow, object?[]>? _orderValuesByRow = precomputedOrderValuesByRow;
        private RowsFrameRange[]? _frameRangesByRow;
        private List<(int Start, int End)>? _peerGroups;
        private bool? _coversWholePartition;

        internal List<EvalRow> Part { get; } = part;

        internal Dictionary<EvalRow, object?[]> GetRequiredOrderValuesByRow()
        {
            _orderValuesByRow ??= WindowOrderValueHelper.BuildWindowOrderValuesByRow(
                Part,
                _spec.OrderBy,
                (expr, row) => _owner.Eval(expr, row, null, _ctes));
            return _orderValuesByRow;
        }

        internal RowsFrameRange GetFrameRange(int rowIndex)
        {
            if (_frameRangesByRow is null)
            {
                _frameRangesByRow = new RowsFrameRange[Part.Count];
                var needsOrderValues = _spec.Frame is not null
                    && _spec.Frame.Unit != WindowFrameUnit.Rows
                    && _spec.OrderBy.Count > 0;
                var orderValuesByRow = needsOrderValues ? GetRequiredOrderValuesByRow() : null;
                for (var i = 0; i < Part.Count; i++)
                {
                    _frameRangesByRow[i] = _owner.ResolveWindowFrameRange(
                        _spec.Frame,
                        Part,
                        i,
                        _spec.OrderBy,
                        _ctes,
                        orderValuesByRow);
                }
            }

            return _frameRangesByRow[rowIndex];
        }

        internal bool CoversWholePartition()
        {
            if (_coversWholePartition.HasValue)
                return _coversWholePartition.Value;

            if (Part.Count == 0)
            {
                _coversWholePartition = false;
                return false;
            }

            for (var i = 0; i < Part.Count; i++)
            {
                var frameRange = GetFrameRange(i);
                if (frameRange.IsEmpty || frameRange.StartIndex != 0 || frameRange.EndIndex != Part.Count - 1)
                {
                    _coversWholePartition = false;
                    return false;
                }
            }

            _coversWholePartition = true;
            return true;
        }

        internal List<(int Start, int End)> GetPeerGroups()
        {
            if (_peerGroups is not null)
                return _peerGroups;

            var peerGroups = new List<(int Start, int End)>();
            if (Part.Count == 0)
                return _peerGroups = peerGroups;

            if (Part.Count == 1)
                return _peerGroups = [(0, 0)];

            var orderValuesByRow = GetRequiredOrderValuesByRow();
            var start = 0;
            for (var i = 1; i <= Part.Count; i++)
            {
                var isBoundary = i == Part.Count
                    || !WindowOrderValueHelper.WindowOrderValuesEqual(
                        orderValuesByRow[Part[i - 1]],
                        orderValuesByRow[Part[i]],
                        _owner.CompareSql);
                if (!isBoundary)
                    continue;

                peerGroups.Add((start, i - 1));
                start = i;
            }

            _peerGroups = peerGroups;
            return _peerGroups;
        }
    }


    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // Custom schema functions are resolved through the current connection when available.
    private SqlExpr ParseExpr(string raw)
    {
        var dialectInstance = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para parse de expressão.");
        return SqlExpressionParser.ParseWhere(
            raw,
            dialectInstance,
            null,
            customFunctionSupported: name => _cnn.TryGetFunction(name, out _));
    }

    private SqlExpr ParseScalarExpr(string raw)
    {
        var dialect1 = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para parse de expressão escalar.");
        return SqlExpressionParser.ParseScalar(
            raw,
            dialect1,
            _pars,
            customFunctionSupported: name => _cnn.TryGetFunction(name, out _));
    }

    private static List<List<WindowSlot>> GroupWindowSlotsBySpec(List<WindowSlot> slots)
    {
        var groups = new Dictionary<string, List<WindowSlot>>(Math.Max(1, slots.Count), StringComparer.Ordinal);
        foreach (var slot in slots)
        {
            var key = BuildWindowSpecCacheKey(slot.Expr.Spec);
            if (!groups.TryGetValue(key, out var group))
            {
                group = new List<WindowSlot>();
                groups[key] = group;
            }

            group.Add(slot);
        }

        return [.. groups.Values];
    }

    private static string BuildWindowSpecCacheKey(WindowSpec spec)
    {
        var sb = new StringBuilder();
        sb.Append("PART:");
        for (var i = 0; i < spec.PartitionBy.Count; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(spec.PartitionBy[i]));
        }

        sb.Append(";ORDER:");
        for (var i = 0; i < spec.OrderBy.Count; i++)
        {
            if (i > 0)
                sb.Append('|');

            sb.Append(SqlExprPrinter.Print(spec.OrderBy[i].Expr));
            sb.Append(spec.OrderBy[i].Desc ? ":DESC" : ":ASC");
        }

        sb.Append(";FRAME:");
        if (spec.Frame is null)
        {
            sb.Append(SqlConst.NULL);
            return sb.ToString();
        }

        sb.Append(spec.Frame.Unit);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, spec.Frame.Start);
        sb.Append(':');
        AppendWindowFrameBoundCacheKey(sb, spec.Frame.End);
        return sb.ToString();
    }

    private static void AppendWindowFrameBoundCacheKey(StringBuilder sb, WindowFrameBound bound)
    {
        sb.Append(bound.Kind);
        sb.Append('(');
        sb.Append(bound.Offset?.ToString(CultureInfo.InvariantCulture) ?? "null");
        sb.Append(')');
    }

    /// <summary>
    /// EN: Implements ExecuteUnion.
    /// PT: Implementa ExecuteUnion.
    /// </summary>
    public TableResultMock ExecuteUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy = null,
        SqlRowLimit? rowLimit = null,
        string? sqlContextForErrors = null)
    {
        var dialect2 = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para UNION.");
        ClearSubqueryEvaluationCaches();
        return UnionExecutionHelper.Execute(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            sqlContextForErrors,
            _cnn,
            dialect2,
            parts1 => ExecuteSelect(parts1, null, null),
            ApplyOrderAndLimit,
            () => AstQueryPlanMetricsHelper.BuildPlanMockRuntimeContext(_cnn),
            AstQueryPlanMetricsHelper.CountKnownInputTables,
            query => AstQueryPlanMetricsHelper.EstimateRowsRead(_cnn, query));
    }

    /// <summary>
    /// EN: Implements ExecuteSelect.
    /// PT: Implementa ExecuteSelect.
    /// </summary>
    public TableResultMock ExecuteSelect(SqlSelectQuery q)
    {
        var sw = Stopwatch.StartNew();
        ClearSubqueryEvaluationCaches();
        QueryDebugTraceBuilder? debugTrace = _cnn.IsDebugTraceCaptureEnabled
            ? new QueryDebugTraceBuilder(SqlConst.SELECT)
            : null;
        var result = ExecuteSelect(q, null, null, debugTrace);
        sw.Stop();

        if (!HasSqlCalcFoundRows(q) && !IsRowCountHelperSelect(q))
            _cnn.SetLastFoundRows(result.Count);

        var metrics = AstQueryPlanMetricsHelper.BuildPlanRuntimeMetrics(_cnn, q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(q, metrics);
        var planWarnings = QueryPlanWarningHelper.BuildPlanWarnings(q, metrics);
        var runtimeContext = AstQueryPlanMetricsHelper.BuildPlanMockRuntimeContext(_cnn);
        if (_cnn.Db.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatSelect(
                q,
                metrics,
                indexRecommendations,
                planWarnings,
                runtimeContext: runtimeContext);
            result.ExecutionPlan = plan;
            _cnn.RegisterExecutionPlan(plan);
        }
        if (debugTrace is not null)
            _cnn.RegisterDebugTrace(debugTrace.Build());
        return result;
    }

    private IReadOnlyList<SqlIndexRecommendation> BuildIndexRecommendations(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
        => SelectPlanIndexRecommendationHelper.Build(_cnn, query, metrics);

    private TableResultMock ExecuteSelect(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(selectQuery, nameof(selectQuery));

        // 0) Build CTE materializations (simple: materialize each CTE into a temp source)
        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var cteStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var res = cte.Query switch
            {
                SqlSelectQuery cteSelect => ExecuteSelect(cteSelect, ctes, outerRow),
                SqlUnionQuery cteUnion => ExecuteUnion(
                    cteUnion.Parts,
                    cteUnion.AllFlags,
                    cteUnion.OrderBy,
                    cteUnion.RowLimit,
                    cteUnion.RawSql),
                _ => throw new NotSupportedException($"CTE query type '{cte.Query.GetType().Name}' is not supported.")
            };
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
            debugTrace?.AddStep(
                "CteMaterialize",
                0,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(cteStart)),
                cte.Name);
        }

        if (TryEvaluateSimpleUnionAllCount(selectQuery, ctes, outerRow, out var fastCountResult))
            return fastCountResult;

        // 1) FROM
        var fromStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var rows = BuildFrom(
            selectQuery.Table,
            ctes,
            selectQuery.Where,
            hasOrderBy: selectQuery.OrderBy.Count > 0,
            hasGroupBy: selectQuery.GroupBy.Count > 0);
        if (debugTrace is not null)
        {
            var fromRows = rows as List<EvalRow> ?? [.. rows];
            debugTrace.AddStep(
                "TableScan",
                (int)Math.Min(int.MaxValue, AstQueryPlanMetricsHelper.GetKnownSourceRows(_cnn, selectQuery.Table)),
                fromRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(fromStart)),
                SqlSourceFormattingHelper.FormatSource(selectQuery.Table));
            rows = fromRows;
        }

        // 2) JOINS
        foreach (var j in selectQuery.Joins)
        {
            var joinStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = debugTrace is not null
                ? (rows as ICollection<EvalRow>)?.Count ?? rows.Count()
                : 0;
            rows = ApplyJoin(
                rows,
                j,
                ctes,
                hasOrderBy: selectQuery.OrderBy.Count > 0,
                hasGroupBy: selectQuery.GroupBy.Count > 0);
            if (debugTrace is not null)
            {
                var joinedRows = rows as List<EvalRow> ?? [.. rows];
                debugTrace.AddStep(
                    $"Join({FormatJoinTypeForDebug(j.Type)})",
                    inputRows,
                    joinedRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(joinStart)),
                    SqlSourceFormattingHelper.FormatJoinDebugDetails(j));
                rows = joinedRows;
            }
        }

        // 2.5) Correlated subquery: expose outer row fields/sources to subquery evaluation (EXISTS, IN subselect, etc.)
        if (outerRow is not null)
            rows = AttachOuterRows(rows, outerRow);

        // 3) WHERE
        if (selectQuery.Where is not null)
        {
            var filterStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = debugTrace is not null
                ? (rows as ICollection<EvalRow>)?.Count ?? rows.Count()
                : 0;
            rows = ApplyRowPredicate(rows, selectQuery.Where, ctes);
            if (debugTrace is not null)
            {
                var filteredRows = rows as List<EvalRow> ?? [.. rows];
                debugTrace.AddStep(
                    "Filter",
                    inputRows,
                    filteredRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(filterStart)),
                    SqlExprPrinter.Print(selectQuery.Where));
                rows = filteredRows;
            }
        }

        // 4) GROUP BY / HAVING / SELECT projection
        bool needsGrouping = selectQuery.GroupBy.Count > 0 || selectQuery.Having is not null || ContainsAggregate(selectQuery);

        if (needsGrouping)
        {
            var groupedRows = rows as List<EvalRow> ?? [.. rows];
            if (debugTrace is null && TryEvaluateSimpleStringAggregate(selectQuery, groupedRows, ctes, out var fastStringAggregateResult))
                return fastStringAggregateResult;

            return ExecuteGroup(selectQuery, ctes, groupedRows, debugTrace);
        }

        // 5) Project non-grouped
        var projectedRows = rows as List<EvalRow> ?? [.. rows];
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var projected = ProjectRows(selectQuery, projectedRows, ctes);
        debugTrace?.AddStep(
            "Project",
            projectedRows.Count,
            projected.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(selectQuery.SelectItems));

        // 6) DISTINCT
        if (selectQuery.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = projected.Count;
            projected = ApplyDistinct(projected, Dialect);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            _cnn.SetLastFoundRows(projected.Count);

        // 7) ORDER BY / LIMIT
        projected = ApplyOrderAndLimit(projected, selectQuery, ctes, debugTrace);
        projected = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
        return projected;
    }

    private bool TryEvaluateSimpleUnionAllCount(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow,
        out TableResultMock result)
    {
        result = null!;

        if (query.Table?.DerivedUnion is null
            || query.Joins.Count > 0
            || query.Where is not null
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.ForJson is not null
            || query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseScalarCountAggregate(exprRaw, out var countArg) || countArg is not StarExpr)
            return false;

        var union = query.Table.DerivedUnion;
        if (union.RowLimit is not null
            || ContainsDistinctUnionFlag(union.AllFlags))
            return false;

        long count = 0;
        foreach (var part in union.Parts)
        {
            if (!TryCountSimpleRows(part, ctes, outerRow, out var partCount))
                return false;

            count += partCount;
        }

        var tableAlias = query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty;
        var columnAlias = SelectPlanProjectionHelper.InferColumnAlias(exprRaw);
        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    tableAlias,
                    columnAlias,
                    0,
                    DbType.Int64,
                    isNullable: false)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = count });
        result.JoinFields.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        if (query.OrderBy.Count > 0 || query.RowLimit is not null)
        {
            var orderCtes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = ApplyOrderAndLimit(result, query, orderCtes);
        }

        return true;
    }

    private static bool ContainsDistinctUnionFlag(IReadOnlyList<bool> allFlags)
    {
        for (var i = 0; i < allFlags.Count; i++)
        {
            if (!allFlags[i])
                return true;
        }

        return false;
    }

    private bool TryEvaluateSimpleStringAggregate(
        SqlSelectQuery query,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes,
        out TableResultMock result)
    {
        result = null!;

        if (query.GroupBy.Count > 0
            || query.Having is not null
            || query.SelectItems.Count != 1)
        {
            return false;
        }

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseStringAggregateCall(exprRaw, out var aggregateCall))
            return false;

        var dialect2 = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
        var aggregateDefinition = aggregateCall.ResolvedScalarFunction;
        if (aggregateDefinition is null
            && !dialect2.TryGetScalarFunctionDefinition(aggregateCall, out aggregateDefinition))
        {
            return false;
        }

        if (aggregateDefinition is not null
            && !aggregateDefinition.AllowsCall)
        {
            return false;
        }

        if (aggregateDefinition is null)
            return false;

        if (aggregateCall.Distinct)
            return false;

        var firstRow = rows.Count > 0 ? rows[0] : EvalRow.Empty();
        var aggregateGroup = new EvalGroup(rows);
        var resultValue = EvalStringAggregateForCallExpr(aggregateCall, aggregateGroup, ctes, aggregateCall.Name);

        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty,
                    SelectPlanProjectionHelper.InferColumnAlias(exprRaw),
                    0,
                    DbType.String,
                    isNullable: true)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = resultValue });
        result.JoinFields.Add(firstRow.Fields);

        if (HasSqlCalcFoundRows(query))
            _cnn.SetLastFoundRows(result.Count);

        if (query.Distinct)
            result = ApplyDistinct(result, Dialect);

        result = ApplyOrderAndLimit(result, query, ctes);
        result = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(result, query);
        return true;
    }

    private bool TryParseStringAggregateCall(string exprRaw, out CallExpr call)
    {
        call = null!;

        SqlExpr expr;
        try
        {
            expr = ParseScalarExpr(exprRaw);
        }
        catch
        {
            return false;
        }

        if (expr is not CallExpr parsedCall)
            return false;

        if (parsedCall.Name is not ("GROUP_CONCAT" or "STRING_AGG" or "LISTAGG"))
            return false;

        if (parsedCall.WithinGroupOrderBy is not null)
            return false;

        call = parsedCall;
        return true;
    }

    private bool TryCountSimpleRows(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow,
        out long count)
    {
        count = 0;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.Distinct
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (outerRow is null && query.Where is null)
        {
            if (query.Table is null)
            {
                count = 1;
                return true;
            }

            if (AstQueryPlanMetricsHelper.HasKnownPhysicalTable(query.Table))
            {
                count = AstQueryPlanMetricsHelper.GetKnownSourceRows(_cnn, query.Table);
                return true;
            }
        }

        if (outerRow is null
            && query.Table is not null
            && query.Where is not null
            && TryCountRowsFromPrimaryKey(query, ctes, out count))
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
            foreach (var _ in rows)
                count++;
            return true;
        }

        if (outerRow is not null)
        {
            foreach (var candidate in rows)
            {
                if (Eval(query.Where, AttachOuterRow(candidate, outerRow), group: null, ctes).ToBool())
                    count++;
            }

            return true;
        }

        foreach (var candidate in rows)
        {
            if (Eval(query.Where, candidate, group: null, ctes).ToBool())
                count++;
        }

        return true;
    }

    private bool TryCountRowsFromPrimaryKey(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out long count)
    {
        count = 0;

        var src = ResolveSource(query.Table!, ctes);
        if (src.Physical is not TableMock tableMock)
            return false;

        var primaryKeyIndexes = tableMock.PrimaryKeyIndexes;
        if (primaryKeyIndexes.Count == 0)
            return false;

        var hintPlan = AstQueryIndexHelper.BuildMySqlIndexHintPlan(query.Table!.MySqlIndexHints, src.Physical, hasOrderBy: query.OrderBy.Count > 0, hasGroupBy: false);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");

        if (!PartitionHelper.TryCollectColumnEqualities(query.Where!, src, out var equalsByColumn))
            return false;

        var pkValues = new Dictionary<int, object?>(primaryKeyIndexes.Count);
        foreach (var pkIdx in primaryKeyIndexes)
        {
            if (!tableMock.ColumnsByIndex.TryGetValue(pkIdx, out var pkColumnName))
                return false;

            var normalizedColumn = pkColumnName.NormalizeName();
            if (!equalsByColumn.TryGetValue(normalizedColumn, out var value))
                return false;

            pkValues[pkIdx] = value;
        }

        IndexHelper.RecordPrimaryKeyHintMetric(tableMock, hintPlan);
        if (_cnn.Metrics.Enabled)
            _cnn.Metrics.IndexLookups++;
        count = tableMock.TryFindRowByPk(pkValues, out _)
            ? 1
            : 0;
        return true;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var sourceRows = rows as List<EvalRow> ?? [.. rows];
        var keyExprs = BuildGroupByKeyExpressions(q);

        GroupKey BuildGroupKey(EvalRow row)
        {
            var values = new object?[keyExprs.Length];
            for (var i = 0; i < keyExprs.Length; i++)
                values[i] = Eval(keyExprs[i], row, group: null, ctes);

            return new GroupKey(values);
        }

        var groupStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var grouped = MaterializeGroups(sourceRows.GroupBy(
            BuildGroupKey,
            GroupKey.Comparer));
        debugTrace?.AddStep(
            "Group",
            sourceRows.Count,
            grouped.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(groupStart)),
            QueryDebugTraceFormattingHelper.FormatGroupDebugDetails(q));

        // HAVING filter (MySQL: HAVING pode referenciar alias do SELECT)
        if (q.Having is null)
        {
            // Project grouped
            return ProjectGrouped(q, grouped, ctes, debugTrace);
        }

        // pré-parse das expressões do SELECT que têm Alias (ex: COUNT(val) AS C)
        var aliasExprs = new List<(string Alias, SqlExpr Ast)>(q.SelectItems.Count);
        for (var i = 0; i < q.SelectItems.Count; i++)
        {
            var selectItem = q.SelectItems[i];

            // pega alias mesmo se o parser não preencheu si.Alias
            var (exprRaw, alias) = SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
            if (string.IsNullOrWhiteSpace(alias))
                continue;

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

            aliasExprs.Add((alias!, ast));
        }

        var havingExpr = HavingHelper.NormalizeHavingExpression(q.Having, q);

        var havingStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var inputGroups = grouped.Count;
        grouped = ApplyHavingPredicate(grouped, havingExpr, aliasExprs, ctes);
        debugTrace?.AddStep(
            "Having",
            inputGroups,
            grouped.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(havingStart)),
            SqlExprPrinter.Print(q.Having));

        // Project grouped
        return ProjectGrouped(q, grouped, ctes, debugTrace);
    }


    private IEnumerable<EvalRow> ApplyRowPredicate(
        IEnumerable<EvalRow> rows,
        SqlExpr predicate,
        IDictionary<string, Source> ctes)
        => rows.Where(r => Eval(predicate, r, group: null, ctes).ToBool());

    private List<MaterializedGroup> ApplyHavingPredicate(
        IReadOnlyList<MaterializedGroup> grouped,
        SqlExpr havingExpr,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes)
    {
        if (grouped.Count == 0)
            return [];

        var filtered = new List<MaterializedGroup>(grouped.Count);

        var firstGroup = grouped[0];
        var firstEvalCtx = BuildHavingEvaluationContext(firstGroup, aliasExprs, ctes, out var firstEvalGroup);
        HavingHelper.EnsureHavingIdentifiersAreBound(havingExpr, firstEvalCtx, Dialect!);
        if (Eval(havingExpr, firstEvalCtx, firstEvalGroup, ctes).ToBool())
            filtered.Add(firstGroup);

        for (var i = 1; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var evalCtx = BuildHavingEvaluationContext(group, aliasExprs, ctes, out var evalGroup);
            if (Eval(havingExpr, evalCtx, evalGroup, ctes).ToBool())
                filtered.Add(group);
        }

        return filtered;
    }

    private EvalRow BuildHavingEvaluationContext(
        MaterializedGroup grouped,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes,
        out EvalGroup evalGroup)
    {
        var rows = grouped.Rows;
        evalGroup = new EvalGroup(rows);
        var first = rows[0];

        var fields = new Dictionary<string, object?>(first.Fields, StringComparer.OrdinalIgnoreCase);
        fields.EnsureCapacity(first.Fields.Count + aliasExprs.Count);

        var sources = new Dictionary<string, Source>(first.Sources, StringComparer.OrdinalIgnoreCase);
        sources.EnsureCapacity(first.Sources.Count);

        var baseOrdinalValues = first.OrdinalValues is null ? [] : first.OrdinalValues;
        var ordinalValues = new object?[baseOrdinalValues.Length + aliasExprs.Count];
        if (baseOrdinalValues.Length > 0)
            Array.Copy(baseOrdinalValues, ordinalValues, baseOrdinalValues.Length);

        var ordinalIndexes = first.OrdinalIndexes is null
            ? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, int>(first.OrdinalIndexes, StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < aliasExprs.Count; i++)
        {
            var (alias, ast) = aliasExprs[i];
            var value = Eval(ast, first, evalGroup, ctes);
            fields[alias] = value;

            var ordinalIndex = baseOrdinalValues.Length + i;
            ordinalValues[ordinalIndex] = value;
            ordinalIndexes[alias] = ordinalIndex;
        }

        return new EvalRow(fields, sources)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes
        };
    }

    private SqlExpr[] BuildGroupByKeyExpressions(SqlSelectQuery q)
    {
        var keyExprs = new List<SqlExpr>(q.GroupBy.Count);

        foreach (var groupByRaw in q.GroupBy)
        {
            var raw = groupByRaw.Trim();
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: GROUP BY ordinal must be >= 1");

                var idx = ord - 1;
                if (idx >= q.SelectItems.Count)
                    throw new InvalidOperationException($"invalid: GROUP BY ordinal {ord} out of range");

                var selectItem = q.SelectItems[idx];
                var (exprRaw, _) = SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                keyExprs.Add(ParseExpr(exprRaw));
                continue;
            }

            keyExprs.Add(ParseExpr(groupByRaw));
        }

        return [.. keyExprs];
    }

    // ---------------- FROM/JOIN ----------------

    private IEnumerable<EvalRow> BuildFrom(
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (from is null)
        {
            // SELECT without FROM => one synthetic row
            yield return EvalRow.Empty();
            yield break;
        }

        var src = ResolveSource(from, ctes);
        if (from.PartitionNames is { Count: > 0 } requestedPartitions
            && src.Physical is TableMock partitionedTable)
        {
            src = src.WithRequestedPartitions(requestedPartitions);
        }
        src = PartitionHelper.ApplyPartitionPruning(src, where);
        var sourceRows = IndexHelper.TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var r in sourceRows)
            yield return CreateSourceEvalRow(src, r);
    }

    private void TryRecordPrimaryKeyHintMetric(
        ITableMock table,
        MySqlIndexHintPlan? hintPlan)
    {
        if (hintPlan is null || !_cnn.Metrics.Enabled)
            return;

        if (!hintPlan.HasRowAccessHints)
            return;

        string? hintedPrimaryEquivalent = null;
        foreach (var item in hintPlan.PrimaryEquivalentIndexNames)
        {
            if (!hintPlan.AllowedIndexNames.Contains(item))
                continue;

            hintedPrimaryEquivalent = item;
            break;
        }

        if (!string.IsNullOrWhiteSpace(hintedPrimaryEquivalent))
            _cnn.Metrics.IncrementIndexHint(hintedPrimaryEquivalent!);
    }

    private bool TryCollectYearBound(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
        {
            if (!TryCollectYearBound(andExpr.Left, src, partitionedColumnName, out var leftLow, out var leftHigh))
                return false;

            if (!TryCollectYearBound(andExpr.Right, src, partitionedColumnName, out var rightLow, out var rightHigh))
                return false;

            lowerBound = MaxNullable(leftLow, rightLow);
            upperBound = MinNullable(leftHigh, rightHigh);
            return lowerBound.HasValue && upperBound.HasValue && lowerBound.Value <= upperBound.Value;
        }

        if (expr is BinaryExpr orExpr && orExpr.Op == SqlBinaryOp.Or)
        {
            var leftRanges = new List<(int? Low, int? High)>();
            var rightRanges = new List<(int? Low, int? High)>();
            var leftOk = TryCollectYearBound(orExpr.Left, src, partitionedColumnName, out var leftLow, out var leftHigh);
            var rightOk = TryCollectYearBound(orExpr.Right, src, partitionedColumnName, out var rightLow, out var rightHigh);

            if (leftOk && leftLow.HasValue && leftHigh.HasValue)
                leftRanges.Add((leftLow, leftHigh));
            if (rightOk && rightLow.HasValue && rightHigh.HasValue)
                rightRanges.Add((rightLow, rightHigh));

            if (leftRanges.Count == 0 || rightRanges.Count == 0)
                return false;

            lowerBound = MinNullable(leftRanges[0].Low, rightRanges[0].Low);
            upperBound = MaxNullable(leftRanges[0].High, rightRanges[0].High);
            return lowerBound.HasValue && upperBound.HasValue;
        }

        if (TryResolveYearComparisonBound(expr, src, partitionedColumnName, out var low, out var high))
        {
            lowerBound = low;
            upperBound = high;
            return true;
        }

        if (expr is BetweenExpr between
            && !between.Negated
            && (TryGetYearPartitionFunctionInfo(between.Expr, src, partitionedColumnName, out _)
                || TryResolveColumnName(between.Expr, src, out var columnName)
                    && string.Equals(columnName, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
            && TryResolveConstantValue(between.Low, out var lowValue)
            && TryResolveConstantValue(between.High, out var highValue)
            && TryResolvePartitionYearConstant(lowValue, out var lowYear)
            && TryResolvePartitionYearConstant(highValue, out var highYear)
            && lowYear <= highYear)
        {
            lowerBound = lowYear;
            upperBound = highYear;
            return true;
        }

        return false;
    }

    private bool TryResolveYearComparisonBound(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        if (expr is not BinaryExpr cmp
            || cmp.Op is not (SqlBinaryOp.Greater or SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less or SqlBinaryOp.LessOrEqual))
        {
            return false;
        }

        if (TryGetYearPartitionFunctionInfo(cmp.Left, src, partitionedColumnName, out _)
            && TryResolveConstantValue(cmp.Right, out var rightValue)
            && TryResolvePartitionYearConstant(rightValue, out var rightYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, rightYear, functionOnLeft: true, out lowerBound, out upperBound);
        }

        if (TryResolveColumnName(cmp.Left, src, out var leftColumn)
            && string.Equals(leftColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase)
            && TryResolveConstantValue(cmp.Right, out rightValue)
            && TryResolvePartitionYearConstant(rightValue, out rightYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, rightYear, functionOnLeft: true, out lowerBound, out upperBound);
        }

        if (TryGetYearPartitionFunctionInfo(cmp.Right, src, partitionedColumnName, out _)
            && TryResolveConstantValue(cmp.Left, out var leftValue)
            && TryResolvePartitionYearConstant(leftValue, out var leftYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, leftYear, functionOnLeft: false, out lowerBound, out upperBound);
        }

        if (TryResolveColumnName(cmp.Right, src, out var rightColumn)
            && string.Equals(rightColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase)
            && TryResolveConstantValue(cmp.Left, out leftValue)
            && TryResolvePartitionYearConstant(leftValue, out leftYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, leftYear, functionOnLeft: false, out lowerBound, out upperBound);
        }

        return false;
    }

    private static bool TryBuildYearComparisonBound(
        SqlBinaryOp op,
        int year,
        bool functionOnLeft,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        const int MinYear = 1;
        const int MaxYear = 9999;

        static int ClampYear(int value)
            => value < MinYear ? MinYear : value > MaxYear ? MaxYear : value;

        int LowerFromGreater() => ClampYear(year + 1);
        int UpperFromLess() => ClampYear(year - 1);

        if (functionOnLeft)
        {
            switch (op)
            {
                case SqlBinaryOp.Greater:
                    lowerBound = LowerFromGreater();
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.GreaterOrEqual:
                    lowerBound = ClampYear(year);
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.Less:
                    lowerBound = MinYear;
                    upperBound = UpperFromLess();
                    return true;
                case SqlBinaryOp.LessOrEqual:
                    lowerBound = MinYear;
                    upperBound = ClampYear(year);
                    return true;
            }
        }
        else
        {
            switch (op)
            {
                case SqlBinaryOp.Greater:
                    lowerBound = MinYear;
                    upperBound = UpperFromLess();
                    return true;
                case SqlBinaryOp.GreaterOrEqual:
                    lowerBound = MinYear;
                    upperBound = ClampYear(year);
                    return true;
                case SqlBinaryOp.Less:
                    lowerBound = LowerFromGreater();
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.LessOrEqual:
                    lowerBound = ClampYear(year);
                    upperBound = MaxYear;
                    return true;
            }
        }

        return false;
    }

    private static int? MaxNullable(int? left, int? right)
        => left.HasValue && right.HasValue
            ? Math.Max(left.Value, right.Value)
            : left ?? right;

    private static int? MinNullable(int? left, int? right)
        => left.HasValue && right.HasValue
            ? Math.Min(left.Value, right.Value)
            : left ?? right;

    private bool TryGetPartitionValue(
        SqlExpr maybeColumn,
        SqlExpr maybeValue,
        Source src,
        string partitionedColumnName,
        out object? value)
    {
        value = null;
        if (!TryResolveColumnName(maybeColumn, src, out var resolvedColumn)
            || !string.Equals(resolvedColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!TryResolveConstantValue(maybeValue, out value))
            return false;

        return true;
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
        IDictionary<string, Source> ctes,
        bool hasOrderBy,
        bool hasGroupBy)
        => JoinService.ApplyJoin(leftRows, join, ctes, hasOrderBy, hasGroupBy);

    private Source ResolveSource(
        SqlTableSource ts,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow = null)
    {
        var source = SourceResolver.ResolveBaseSource(ts, ctes, outerRow);
        return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
    }

    internal readonly record struct AutoJsonProjection(
        int ColumnIndex,
        string? Qualifier,
        string PropertyName,
        bool IsJsonFragment);

    internal sealed class AutoJsonRootRow(Dictionary<string, object?> properties)
    {
        internal Dictionary<string, object?> Properties { get; } = properties;
        internal Dictionary<string, List<Dictionary<string, object?>>> Nested { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        internal Dictionary<string, object?> ToJsonObject()
        {
            var json = new Dictionary<string, object?>(Properties, StringComparer.OrdinalIgnoreCase);
            foreach (var nested in Nested)
                json[nested.Key] = nested.Value;

            return json;
        }
    }

    private Source ApplyTableTransformsIfNeeded(
        Source source,
        SqlPivotSpec? pivot,
        SqlUnpivotSpec? unpivot,
        IDictionary<string, Source> ctes)
        => PivotHelper.ApplyTableTransformsIfNeeded(source, pivot, unpivot, ctes);

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
        var projectedColumnCount = selectPlan.Evaluators.Count;
        foreach (var r in rows)
        {
            var outRow = new Dictionary<int, object?>(projectedColumnCount);
            for (int i = 0; i < projectedColumnCount; i++)
                outRow[i] = selectPlan.Evaluators[i](r, null);

            res.Add(outRow);
            res.JoinFields.Add(r.Fields);
        }

        return res;
    }

    private TableResultMock ProjectGrouped(
        SqlSelectQuery q,
        IReadOnlyList<MaterializedGroup> groups,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var res = new TableResultMock();
        var groupsList = groups as List<MaterializedGroup> ?? new List<MaterializedGroup>(groups);
        var hasGroups = groupsList.Count > 0;

        // SQL aggregate semantics: when no GROUP BY is present and the filtered input is empty,
        // aggregate projections (e.g. COUNT(*)) still return a single row.
        if (!hasGroups && q.GroupBy.Count == 0)
            groupsList.Add(new MaterializedGroup(default, new List<EvalRow>()));

        var representativeRows = hasGroups
            ? new List<EvalRow>(groupsList.Count)
            : [];
        if (hasGroups)
        {
            for (var i = 0; i < groupsList.Count; i++)
                representativeRows.Add(groupsList[i].Rows[0]);
        }

        var selectPlan = BuildSelectPlan(
            q,
            representativeRows,
            ctes);

        // columns
        for (int i = 0; i < selectPlan.Columns.Count; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        // rows
        var groupedColumnCount = selectPlan.Evaluators.Count;
        foreach (var g in groupsList)
        {
            var eg = new EvalGroup(g.Rows);
            var outRow = new Dictionary<int, object?>(groupedColumnCount);

            var first = g.Rows.Count > 0 ? g.Rows[0] : EvalRow.Empty();
            for (int i = 0; i < groupedColumnCount; i++)
                outRow[i] = selectPlan.Evaluators[i](first, eg);

            res.Add(outRow);
            res.JoinFields.Add(first.Fields);
        }

        if (q.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = res.Count;
            res = ApplyDistinct(res, Dialect);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(q.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(q))
            _cnn.SetLastFoundRows(res.Count);

        // ORDER / LIMIT
        debugTrace?.AddStep(
            "Project",
            groupsList.Count,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(q.SelectItems));
        res = ApplyOrderAndLimit(res, q, ctes, debugTrace);
        res = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(res, q, debugTrace);
        return res;
    }

    private void ComputeWindowSlots(
        List<WindowSlot> slots,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        if (slots.Count == 0 || rows.Count == 0)
            return;

        foreach (var slotGroup in GroupWindowSlotsBySpec(slots))
        {
            var spec = slotGroup[0].Expr.Spec;
            var partitions = WindowPartitionHelper.BuildPartitions(
                slotGroup[0].Expr,
                rows,
                (expr, row) => Eval(expr, row, null, ctes),
                value => NormalizeDistinctKey(value));

            foreach (var part in partitions.Values)
            {
                var orderValuesByRow = WindowPartitionHelper.SortPartition(
                    part,
                    spec.OrderBy,
                    (expr, row) => Eval(expr, row, null, ctes),
                    CompareSql);
                var partitionContext = new WindowPartitionExecutionContext(this, part, spec, ctes, orderValuesByRow);

                foreach (var slot in slotGroup)
                {
                    var w = slot.Expr;
                    var dialect = Dialect ?? throw new InvalidOperationException("Dialect is required for window function validation.");
                    var windowDefinition = w.ResolvedWindowFunction;
                    if (windowDefinition is null
                        && dialect.TryGetWindowFunctionDefinition(w, out var resolvedWindowDefinition))
                    {
                        windowDefinition = resolvedWindowDefinition;
                    }

                    var isRowNumber = dialect.IsRowNumberWindowFunction(w.Name);
                    var isRank = w.Name.Equals("RANK", StringComparison.OrdinalIgnoreCase);
                    var isDenseRank = w.Name.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase);
                    var isNtile = w.Name.Equals("NTILE", StringComparison.OrdinalIgnoreCase);
                    var isPercentRank = w.Name.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase);
                    var isCumeDist = w.Name.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase);
                    var isLag = w.Name.Equals("LAG", StringComparison.OrdinalIgnoreCase);
                    var isLead = w.Name.Equals("LEAD", StringComparison.OrdinalIgnoreCase);
                    var isFirstValue = w.Name.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isLastValue = w.Name.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isNthValue = w.Name.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);

                    // Fail fast for unsupported window functions to keep runtime behavior aligned with parser dialect gates.
                    var resolvedWindowDefinition2 = windowDefinition
                        ?? throw SqlUnsupported.ForDialect(dialect, $"window functions ({w.Name})");

                    if (resolvedWindowDefinition2!.RequiresOrderBy && w.Spec.OrderBy.Count == 0)
                        throw new InvalidOperationException($"Window function '{w.Name}' requires ORDER BY in OVER clause.");

                    if (isRowNumber)
                    {
                        long rn = 1;
                        foreach (var r in part)
                        {
                            slot.Map[r] = rn;
                            rn++;
                        }
                        continue;
                    }

                    if (isNtile)
                    {
                        FillNtile(slot.Map, partitionContext, w, ctes);
                        continue;
                    }

                    if (isPercentRank || isCumeDist)
                    {
                        FillPercentRankOrCumeDist(slot.Map, partitionContext, isPercentRank);
                        continue;
                    }

                    if (isLag || isLead)
                    {
                        FillLagOrLead(slot.Map, partitionContext, w, ctes, isLead);
                        continue;
                    }

                    if (isFirstValue || isLastValue)
                    {
                        FillFirstOrLastValue(slot.Map, partitionContext, w, ctes, isLastValue);
                        continue;
                    }

                    if (isNthValue)
                    {
                        FillNthValue(slot.Map, partitionContext, w, ctes);
                        continue;
                    }

                    FillRankOrDenseRank(slot.Map, partitionContext, isRank);
                }
            }
        }
    }

    /// <summary>
    /// EN: Fills FIRST_VALUE/LAST_VALUE results for all rows in the current partition.
    /// PT: Preenche os resultados de FIRST_VALUE/LAST_VALUE para todas as linhas da partição atual.
    /// </summary>
    private void FillFirstOrLastValue(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillLast)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetRow = part[fillLast ? part.Count - 1 : 0];
            var value = valueSelector is null
                ? Eval(valueExpr, targetRow, null, ctes)
                : valueSelector(targetRow);
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }


    /// <summary>
    /// EN: Fills NTH_VALUE results using the resolved 1-based index in the ordered partition.
    /// PT: Preenche os resultados de NTH_VALUE usando o índice 1-based resolvido na partição ordenada.
    /// </summary>
    private void FillNthValue(
            Dictionary<EvalRow, object?> map,
            WindowPartitionExecutionContext partitionContext,
            WindowFunctionExpr windowFunctionExpr,
            IDictionary<string, Source> ctes)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        var nth = ResolveNthValueIndex(windowFunctionExpr.Args, part[0], ctes);
        if (nth <= 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetIndex = nth - 1;
            var value = targetIndex < part.Count
                ? valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex])
                : null;
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = frameRange.StartIndex + (nth - 1);
            if (targetIndex > frameRange.EndIndex)
            {
                map[part[i]] = null;
                continue;
            }

            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }

    /// <summary>
    /// EN: Resolves row index boundaries for ROWS/RANGE/GROUPS window frames for the current row.
    /// PT: Resolve os limites de índice de linha para frames ROWS/RANGE/GROUPS da janela na linha atual.
    /// </summary>
    private RowsFrameRange ResolveWindowFrameRange(
        WindowFrameSpec? frame,
        List<EvalRow> part,
        int rowIndex,
        IReadOnlyList<WindowOrderItem> orderBy,
        IDictionary<string, Source> ctes,
        Dictionary<EvalRow, object?[]>? precomputedOrderValuesByRow = null)
    {
        if (part.Count == 0)
            return RowsFrameRange.Empty;

        if (frame is null || frame.Unit == WindowFrameUnit.Rows)
            return WindowFrameRangeResolver.ResolveRowsFrameRange(frame, part.Count, rowIndex);

        if (orderBy.Count == 0)
            throw new InvalidOperationException($"Window frame unit '{frame.Unit}' requires ORDER BY in OVER clause.");

        var orderValuesByRow = precomputedOrderValuesByRow ?? WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            part,
            orderBy,
            (expr, row) => Eval(expr, row, null, ctes));
        return WindowFrameRangeResolver.Resolve(
            frame,
            part,
            rowIndex,
            orderBy,
            orderValuesByRow,
            (left, right) => WindowOrderValueHelper.WindowOrderValuesEqual(left, right, CompareSql));
    }

    private static bool TryReadIntLiteral(SqlExpr expr, out int value)
    {
        value = default;
        if (expr is not LiteralExpr lit)
            return false;

        var raw = lit.Value;
        if (raw is null || raw is DBNull)
            return false;

        if (raw is IConvertible)
        {
            try
            {
                value = Convert.ToInt32(raw, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }


        return false;
    }

    private static bool TryReadLongLiteral(SqlExpr expr, out long value)
    {
        value = default;
        if (expr is not LiteralExpr lit)
            return false;

        var raw = lit.Value;
        if (raw is null || raw is DBNull)
            return false;

        if (raw is IConvertible)
        {
            try
            {
                value = Convert.ToInt64(raw, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// EN: Resolves NTH_VALUE index from literal or evaluated expression with safe fallback.
    /// PT: Resolve o índice do NTH_VALUE a partir de literal ou expressão avaliada com fallback seguro.
    /// </summary>
    private int ResolveNthValueIndex(
            IReadOnlyList<SqlExpr> args,
            EvalRow sampleRow,
            IDictionary<string, Source> ctes)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = Eval(args[1], sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt32(evaluated, CultureInfo.InvariantCulture);
                return parsed > 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
    }
    /// <summary>
    /// EN: Fills LAG/LEAD values for rows in the current ordered partition.
    /// PT: Preenche valores de LAG/LEAD para as linhas da partição ordenada atual.
    /// </summary>
    private void FillLagOrLead(
            Dictionary<EvalRow, object?> map,
            WindowPartitionExecutionContext partitionContext,
            WindowFunctionExpr windowFunctionExpr,
            IDictionary<string, Source> ctes,
            bool fillLead)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var offset = ResolveLagLeadOffset(windowFunctionExpr.Args, part[0], ctes);
        var defaultExpr = windowFunctionExpr.Args.Count >= 3 ? windowFunctionExpr.Args[2] : null;
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        var hasWholePartitionFrame = windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition();

        if (hasWholePartitionFrame)
        {
            if (offset == 0)
            {
                foreach (var currentRow in part)
                    map[currentRow] = valueSelector is null
                        ? Eval(valueExpr, currentRow, null, ctes)
                        : valueSelector(currentRow);

                return;
            }

            for (int i = 0; i < part.Count; i++)
            {
                var targetIndex = fillLead ? i + offset : i - offset;
                var currentRow = part[i];
                map[currentRow] = targetIndex >= 0 && targetIndex < part.Count
                    ? valueSelector is null
                        ? Eval(valueExpr, part[targetIndex], null, ctes)
                        : valueSelector(part[targetIndex])
                    : defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
            }

            return;
        }

        if (offset == 0)
        {
            for (int i = 0; i < part.Count; i++)
            {
                var currentRow = part[i];
                var frameRange = partitionContext.GetFrameRange(i);
                if (frameRange.IsEmpty || i < frameRange.StartIndex || i > frameRange.EndIndex)
                {
                    map[currentRow] = defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
                    continue;
                }

                map[currentRow] = valueSelector is null
                    ? Eval(valueExpr, currentRow, null, ctes)
                    : valueSelector(currentRow);
            }

            return;
        }

        for (int i = 0; i < part.Count; i++)
        {
            var targetIndex = fillLead ? i + offset : i - offset;
            var currentRow = part[i];
            var frameRange = partitionContext.GetFrameRange(i);

            if (!frameRange.IsEmpty && targetIndex >= frameRange.StartIndex && targetIndex <= frameRange.EndIndex)
            {
                map[currentRow] = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                continue;
            }

            map[currentRow] = defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
        }
    }

    /// <summary>
    /// EN: Builds a direct value accessor for simple window-value references when the row shape is unambiguous.
    /// PT: Monta um acesso direto ao valor para referencias simples de janela quando a forma da linha e inequivoca.
    /// </summary>
    private Func<EvalRow, object?>? TryCreateWindowValueSelector(
        SqlExpr valueExpr,
        EvalRow sampleRow)
    {
        if (valueExpr is IdentifierExpr identifier)
        {
            if (Dialect is null
                || IsReservedWindowValueIdentifier(identifier.Name)
                || !sampleRow.TryGetSingleSource(out var singleSource)
                || singleSource is null
                || !singleSource.ContainsColumnName(identifier.Name))
            {
                return null;
            }

            var columnName = identifier.Name;
            return row => row.GetByName(columnName);
        }

        if (valueExpr is ColumnExpr column)
        {
            if (string.IsNullOrWhiteSpace(column.Qualifier))
            {
                if (!sampleRow.TryGetSingleSource(out var singleSource)
                    || singleSource is null
                    || !singleSource.ContainsColumnName(column.Name))
                {
                    return null;
                }

                var columnName = column.Name;
                return row => row.GetByName(columnName);
            }

            return row => QueryRowValueHelper.ResolveColumn(column.Qualifier, column.Name, row);
        }

        return null;
    }

    /// <summary>
    /// EN: Identifies reserved identifiers that must keep the generic window-value evaluation path.
    /// PT: Identifica identificadores reservados que precisam manter o caminho generico de avaliacao de valor de janela.
    /// </summary>
    private bool IsReservedWindowValueIdentifier(string name)
    {
        if (Dialect!.TryGetScalarFunctionDefinition(name, out var definition)
            && definition is not null
            && definition.AllowsIdentifier)
        {
            return true;
        }

        return name.Equals("_ROWID", StringComparison.OrdinalIgnoreCase)
           || name.Equals("USER", StringComparison.OrdinalIgnoreCase)
           || name.Equals("ORA_INVOKING_USER", StringComparison.OrdinalIgnoreCase)
           || name.Equals("ORA_INVOKING_USERID", StringComparison.OrdinalIgnoreCase)
           || name.Equals("CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase)
           || name.Equals("CURRENT_DATABASE", StringComparison.OrdinalIgnoreCase)
           || name.Equals("CURRENT_CATALOG", StringComparison.OrdinalIgnoreCase)
           || IsSqlServerRowCountIdentifier(name, Dialect);
    }

    /// <summary>
    /// EN: Resolves LAG/LEAD offset from literal or evaluated expression with safe fallback.
    /// PT: Resolve o offset de LAG/LEAD a partir de literal ou expressão avaliada com fallback seguro.
    /// </summary>
    private int ResolveLagLeadOffset(
            IReadOnlyList<SqlExpr> args,
            EvalRow sampleRow,
            IDictionary<string, Source> ctes)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral >= 0)
            return parsedLiteral;

        var evaluated = Eval(args[1], sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt32(evaluated, CultureInfo.InvariantCulture);
                return parsed >= 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
    }

    /// <summary>
    /// EN: Fills RANK/DENSE_RANK using per-row ROWS frame boundaries when present.
    /// PT: Preenche RANK/DENSE_RANK usando limites de frame ROWS por linha quando presentes.
    /// </summary>
    private void FillRankOrDenseRank(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        bool fillRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        if (partitionContext.CoversWholePartition())
        {
            var denseRank = 1L;
            foreach (var peerGroup in partitionContext.GetPeerGroups())
            {
                var value = fillRank ? peerGroup.Start + 1L : denseRank;
                for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                    map[part[i]] = value;

                denseRank++;
            }

            return;
        }

        var orderValuesByRow = partitionContext.GetRequiredOrderValuesByRow();

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, i))
            {
                map[part[i]] = null;
                continue;
            }

            var currentValues = orderValuesByRow[part[i]];
            long rank = 1;
            long denseRank = 1;
            object?[]? prevValues = null;

            for (var frameIndex = frameRange.StartIndex; frameIndex <= frameRange.EndIndex; frameIndex++)
            {
                var frameValues = orderValuesByRow[part[frameIndex]];
                if (prevValues is not null && !WindowOrderValueHelper.WindowOrderValuesEqual(prevValues, frameValues, CompareSql))
                {
                    rank = (frameIndex - frameRange.StartIndex) + 1;
                    denseRank++;
                }

                if (WindowOrderValueHelper.WindowOrderValuesEqual(frameValues, currentValues, CompareSql))
                    break;

                prevValues = frameValues;
            }

            map[part[i]] = fillRank ? rank : denseRank;
        }
    }
    /// <summary>
    /// EN: Computes and fills PERCENT_RANK or CUME_DIST values for the current partition.
    /// PT: Calcula e preenche valores de PERCENT_RANK ou CUME_DIST para a partição atual.
    /// </summary>
    private void FillPercentRankOrCumeDist(
            Dictionary<EvalRow, object?> map,
            WindowPartitionExecutionContext partitionContext,
            bool fillPercentRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        if (partitionContext.CoversWholePartition())
        {
            foreach (var peerGroup in partitionContext.GetPeerGroups())
            {
                var peerCount = peerGroup.End - peerGroup.Start + 1;
                var value = fillPercentRank
                    ? part.Count <= 1 ? 0d : (double)peerGroup.Start / (part.Count - 1)
                    : (double)(peerGroup.Start + peerCount) / part.Count;

                for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                    map[part[i]] = value;
            }

            return;
        }

        var orderValuesByRow = partitionContext.GetRequiredOrderValuesByRow();

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var row = part[rowIndex];
            var frameRange = partitionContext.GetFrameRange(rowIndex);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, rowIndex))
            {
                map[row] = null;
                continue;
            }

            var frameCount = frameRange.EndIndex - frameRange.StartIndex + 1;
            var currentValues = orderValuesByRow[row];
            long lessThanCount = 0;
            long peerCount = 0;

            for (var frameIndex = frameRange.StartIndex; frameIndex <= frameRange.EndIndex; frameIndex++)
            {
                var frameValues = orderValuesByRow[part[frameIndex]];
                if (WindowOrderValueHelper.WindowOrderValuesEqual(frameValues, currentValues, CompareSql))
                {
                    peerCount++;
                    continue;
                }

                if (frameIndex < rowIndex)
                    lessThanCount++;
            }

            var rankInFrame = lessThanCount + 1;
            if (fillPercentRank)
            {
                map[row] = frameCount <= 1 ? 0d : ((double)(rankInFrame - 1)) / (frameCount - 1);
            }
            else
            {
                map[row] = (double)(lessThanCount + peerCount) / frameCount;
            }
        }
    }

    /// <summary>
    /// EN: Fills NTILE values honoring per-row ROWS frame boundaries when present.
    /// PT: Preenche valores de NTILE respeitando os limites de frame ROWS por linha quando presentes.
    /// </summary>
    private void FillNtile(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        var bucketCount = ResolveNtileBucketCount(windowFunctionExpr, part.Count, part[0], ctes);
        if (bucketCount <= 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
                map[part[rowIndex]] = (rowIndex * bucketCount) / part.Count + 1;

            return;
        }

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var frameRange = partitionContext.GetFrameRange(rowIndex);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, rowIndex))
            {
                map[part[rowIndex]] = null;
                continue;
            }

            var frameSize = frameRange.EndIndex - frameRange.StartIndex + 1;
            var positionInFrame = (rowIndex - frameRange.StartIndex) + 1;
            var tile = ((positionInFrame - 1) * bucketCount) / frameSize + 1;
            map[part[rowIndex]] = tile;
        }
    }

    /// <summary>
    /// EN: Resolves the bucket count argument for NTILE from literal or evaluated expression.
    /// PT: Resolve o argumento de quantidade de buckets do NTILE a partir de literal ou expressão avaliada.
    /// </summary>
    private long ResolveNtileBucketCount(
        WindowFunctionExpr windowFunctionExpr,
        int partitionSize,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes)
    {
        if (partitionSize <= 0)
            return 0;

        if (windowFunctionExpr.Args.Count == 0)
            return 1;

        var arg = windowFunctionExpr.Args[0];
        if (TryReadLongLiteral(arg, out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = Eval(arg, sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt64(evaluated, CultureInfo.InvariantCulture);
                return parsed > 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
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
        var cacheKey = ctes.Count == 0 ? BuildSelectPlanCacheKey(q, sampleRows) : null;
        if (cacheKey is not null
            && _cnn.TryGetCachedSelectPlan(cacheKey, out var cachedPlan)
            && cachedPlan is not null)
        {
            return cachedPlan.CanBeCachedWithoutClone
                ? cachedPlan
                : cachedPlan.CloneForExecution();
        }

        var plan = SelectPlanBuilderHelper.Build(
            q,
            sampleRows,
            ctes,
            Dialect,
            ParseScalarExpr,
            Eval,
            ResolveColumn);

        if (cacheKey is not null)
            _cnn.TryCacheSelectPlan(cacheKey, plan.CanBeCachedWithoutClone ? plan : plan.CloneForCache());

        return plan;
    }

    private string? BuildSelectPlanCacheKey(SqlSelectQuery query, List<EvalRow> sampleRows)
    {
        if (string.IsNullOrWhiteSpace(query.RawSql))
            return null;

        var cacheDialect = Dialect ?? _cnn.ExecutionDialect;
        var sb = new StringBuilder(query.RawSql.Length + 160);
        sb.Append(query.RawSql);
        sb.Append("|dialect:");
        sb.Append(cacheDialect.Name);
        sb.Append(':');
        sb.Append(cacheDialect.Version);
        sb.Append("|schema:");
        sb.Append(_cnn.GetSelectPlanCacheGeneration());
        sb.Append("|sources:");
        sb.Append(sampleRows.Count);

        if (sampleRows.Count == 0)
        {
            sb.Append("|<empty>");
            return sb.ToString();
        }

        var firstRow = sampleRows[0];
        if (firstRow.Sources.Count <= 1)
        {
            foreach (var sourceEntry in firstRow.Sources)
            {
                sb.Append('|');
                sb.Append(sourceEntry.Key);
                sb.Append('=');
                sb.Append(sourceEntry.Value.Name);
                sb.Append('/');
                sb.Append(sourceEntry.Value.Alias);
                sb.Append(':');
                for (var i = 0; i < sourceEntry.Value.ColumnNames.Count; i++)
                {
                    if (i > 0)
                        sb.Append(',');

                    sb.Append(sourceEntry.Value.ColumnNames[i]);
                }
            }

            return sb.ToString();
        }

        var sources = new List<KeyValuePair<string, Source>>(firstRow.Sources.Count);
        foreach (var sourceEntry in firstRow.Sources)
            sources.Add(sourceEntry);

        sources.Sort(static (left, right) => StringComparer.OrdinalIgnoreCase.Compare(left.Key, right.Key));

        foreach (var sourceEntry in sources)
        {
            sb.Append('|');
            sb.Append(sourceEntry.Key);
            sb.Append('=');
            sb.Append(sourceEntry.Value.Name);
            sb.Append('/');
            sb.Append(sourceEntry.Value.Alias);
            sb.Append(':');
            for (var i = 0; i < sourceEntry.Value.ColumnNames.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');

                sb.Append(sourceEntry.Value.ColumnNames[i]);
            }
        }

        return sb.ToString();
    }

    private void EnsureDialectSupportsSequenceFunction(string? functionName)
        => SequenceFunctionSupportHelper.EnsureSupported(Dialect, functionName);

    // Remove "AS alias" somente quando:
    // - está no FINAL do select item
    // - e esse SqlConst.AS está fora de parênteses (pra não quebrar CAST(x AS CHAR))
    private static (string expr, string? alias) SplitTrailingAsAlias(
        string raw,
        string? alreadyAlias)
        => SelectAliasParserHelper.SplitTrailingAsAlias(raw, alreadyAlias);

    // ---------------- ORDER / LIMIT ----------------

    private TableResultMock ApplyOrderAndLimit(
        TableResultMock res,
        SqlSelectQuery q,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
        => AstQueryOrderLimitHelper.Apply(
            res,
            q,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            CompareSql,
            debugTrace);

    private static string FormatJoinTypeForDebug(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.CrossApply => "CROSS APPLY",
            SqlJoinType.OuterApply => "OUTER APPLY",
            _ => joinType.ToString().ToUpperInvariant()
        };


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
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para acesso JSON.");
        var extract = new FunctionCallExpr("JSON_EXTRACT", [ja.Target, ja.Path])
            .BindScalarFunctionDefinition(dialect);
        return ja.Unquote
            ? new FunctionCallExpr("JSON_UNQUOTE", [extract])
                .BindScalarFunctionDefinition(dialect)
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
        var cacheKey = BuildCorrelatedSubqueryCacheKey(SqlConst.SCALAR, sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddScalar(
            cacheKey,
            _ =>
            {
                var query = GetSingleSubqueryOrThrow(sq, "EVAL subquery");
                if (TryEvaluateScalarSubqueryFast(query, row, ctes, out var fastValue))
                    return fastValue;

                var r = ExecuteSelect(LimitToSingleRow(query), ctes, row);
                return r.Count > 0 && r[0].TryGetValue(0, out var v) ? v : null;
            });
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
                if (TryResolveLocalFunctionValue(id.Name, out var localValue))
                    return localValue;

                return EvalIdentifier(id, row);

            case ColumnExpr col:
                return ResolveColumn(col.Qualifier, col.Name, row);

            case StarExpr:
                // only meaningful inside COUNT(*)
                return "*";

            case IsNullExpr isn:
                return EvalIsNull(isn, row, group, ctes);

            case LikeExpr like:
                return EvalLike(like, row, group, ctes);

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
                return EvalNot(u, row, group, ctes);

            case BinaryExpr b:
                return EvalBinary(b, row, group, ctes);

            case InExpr i:
                return EvalIn(i, row, group, ctes);

            case ExistsExpr ex:
                return EvalExists(ex, row, ctes);

            case QuantifiedComparisonExpr qc:
                return EvalQuantifiedComparison(qc, row, group, ctes);


            case CaseExpr c:
                return EvalCase(c, row, group, ctes);

            case JsonAccessExpr ja:
                return EvalJsonAccess(ja, row, group, ctes);
            case FunctionCallExpr fn:
                return EvalFunction(fn, row, group, ctes);
            case CallExpr ce:
                return EvalCall(ce, row, group, ctes);
            case BetweenExpr b:
                return EvalBetween(b, row, group, ctes);
            case SubqueryExpr sq:
                return EvalScalarSubquery(sq, ctes, row);
            case RowExpr re:
                return EvalRowExpression(re, row, group, ctes);

            case RawSqlExpr:
                // unsupported expression (e.g. CAST(x AS CHAR)): best-effort: null
                return null;

            default:
                throw new InvalidOperationException($"Expr não suportada no executor: {expr.GetType().Name}");
        }
    }

    private object? EvalIdentifier(IdentifierExpr identifier, EvalRow row)
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para avaliação de função temporal.");
        if (dialect.TryGetScalarFunctionDefinition(identifier.Name, out var metadataDefinition)
            && metadataDefinition is not null
            && metadataDefinition.AllowsIdentifier)
        {
            if (metadataDefinition.AstExecutor is not null
                && metadataDefinition.AstExecutor(
                    new FunctionCallExpr(identifier.Name, Array.Empty<SqlExpr>()),
                    dialect,
                    static _ => null,
                    out var boundIdentifierValue))
            {
                return boundIdentifierValue;
            }

            if (metadataDefinition.TemporalKind is not null
                && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(
                    dialect,
                    identifier.Name,
                    _evaluationLocalNow,
                    _evaluationUtcNow,
                    out var temporalIdentifierValue))
            {
                return temporalIdentifierValue;
            }
        }

        if (dialect.TryGetScalarFunctionDefinition(identifier.Name, out metadataDefinition)
            && metadataDefinition is not null
            && !metadataDefinition.AllowsIdentifier)
        {
            throw SqlUnsupported.ForDialect(dialect, identifier.Name.ToUpperInvariant());
        }

        if (identifier.Name.Equals("@@DATEFIRST", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return 7;

        if (identifier.Name.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return _cnn.GetLastInsertId();

        if (identifier.Name.Equals("@@MAX_PRECISION", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return 38;

        if (identifier.Name.Equals("@@TEXTSIZE", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return 4096;

        if (identifier.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return "dbo";

        if (identifier.Name.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return "dbo";

        if (identifier.Name.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return "sa";

        if (dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            if (identifier.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
                return "SYS";
            if (identifier.Name.Equals("ORA_INVOKING_USER", StringComparison.OrdinalIgnoreCase))
                return "SYS";
            if (identifier.Name.Equals("ORA_INVOKING_USERID", StringComparison.OrdinalIgnoreCase))
                return 0;
        }

        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            if (identifier.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            if (identifier.Name.Equals("CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase))
                return "public";
            if (identifier.Name.Equals("CURRENT_DATABASE", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("CURRENT_CATALOG", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            if (identifier.Name.Equals("CURRENT_ROLE", StringComparison.OrdinalIgnoreCase))
                return "postgres";
        }

        if (IsSqlServerRowCountIdentifier(identifier.Name, Dialect))
            return _cnn.GetLastFoundRows();

        return ResolveIdentifier(identifier.Name, row);
    }

    private object? EvalIsNull(
        IsNullExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var value = Eval(expression.Expr, row, group, ctes);
        var isNull = value is null || value is DBNull;
        return expression.Negated ? !isNull : isNull;
    }

    private bool EvalLike(
        LikeExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var left = Eval(expression.Left, row, group, ctes)?.ToString() ?? string.Empty;
        var pattern = Eval(expression.Pattern, row, group, ctes)?.ToString() ?? string.Empty;
        var escape = expression.Escape is null
            ? null
            : Eval(expression.Escape, row, group, ctes)?.ToString();
        return left.Like(pattern, Dialect, escape, expression.CaseInsensitive ? true : null);
    }

    private object? EvalNot(
        UnaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (expression.Expr is InExpr notInExpression)
            return EvalNotIn(notInExpression, row, group, ctes);

        return !Eval(expression.Expr, row, group, ctes).ToBool();
    }

    private object? EvalJsonAccess(
        JsonAccessExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (!Dialect!.SupportsJsonArrowOperators)
            throw SqlUnsupported.ForDialect(Dialect, "JSON -> / ->> / #> / #>> operators");

        var mapped = MapJsonAccess(expression);
        return Eval(mapped, row, group, ctes);
    }

    private object?[] EvalRowExpression(
        RowExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => [.. expression.Items.Select(item => Eval(item, row, group, ctes))];

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
        if (TryEvalLogicalBinary(b, row, group, ctes, out var logicalResult))
            return logicalResult;

        if (TryEvaluateCorrelatedCountComparisonFast(b, row, ctes, out var countComparisonResult))
            return countComparisonResult;

        var l = Eval(b.Left, row, group, ctes);
        var r = Eval(b.Right, row, group, ctes);

        if (TryEvalConcatBinary(b.Op, l, r, out var concatResult))
            return concatResult;

        if (TryEvalArithmeticBinary(b.Op, l, r, out var arithmeticResult))
            return arithmeticResult;

        if (TryEvalNullSafeEqualityBinary(b.Op, l, r, out var nullSafeEqualityResult))
            return nullSafeEqualityResult;

        if (l is null || l is DBNull || r is null || r is DBNull)
        {
            // SQL: comparisons with NULL => false (except IS NULL handled elsewhere)
            return false;
        }

        return EvalComparisonBinary(b.Op, l, r);
    }

    private bool TryEvaluateCorrelatedCountComparisonFast(
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

        if (TryEvaluateCountComparisonAgainstLiteral(expression.Right, expression.Left, ReverseComparisonOperator(expression.Op), row, ctes, out result))
            return true;

        return false;
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

        if (candidate is not SubqueryExpr subquery || !TryGetDecimalLiteral(otherSide, out var literalValue))
            return false;

        var query = GetSingleSubqueryOrThrow(subquery, "COUNT comparison");
        if (query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseScalarCountAggregate(exprRaw, out var countArg) || countArg is not StarExpr)
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

        if (!TryBuildCorrelatedLookupCompositeKey(state.KeyPairs, row, ctes, useInnerSide: false, out var outerKey))
            return false;

        var count = state.Counts.TryGetValue(outerKey, out var matchedCount)
            ? matchedCount
            : 0;
        var comparison = CompareSql((decimal)count, literalValue);
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

        if (!TryGetCorrelatedCountLookupPattern(
                query.Where!,
                ResolveSource(query.Table!, ctes),
                out var keyPairs,
                out var innerFilterExpr))
            return false;

        var cacheKey = BuildCorrelatedLookupStateCacheKey(
            "COUNT_PREAGG",
            query.Table!,
            keyPairs,
            innerFilterExpr);

        if (_subqueryEvaluationCache.TryGetOperationData(cacheKey, out CorrelatedCountLookupState? cachedState)
            && cachedState is not null)
        {
            state = cachedState;
            return true;
        }

        var built = BuildCorrelatedCountLookupState(query, ctes, keyPairs, innerFilterExpr);
        if (built is null)
            return false;

        var cached = _subqueryEvaluationCache.GetOrAddOperationData(
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
        var rows = BuildFrom(
            query.Table,
            ctes,
            where: innerFilterExpr,
            hasOrderBy: false,
            hasGroupBy: false);

        if (innerFilterExpr is not null)
            rows = ApplyRowPredicate(rows, innerFilterExpr, ctes);

        var estimatedCount = GetKnownRowCount(rows);
        var compositeCounts = estimatedCount > 0
            ? new Dictionary<string, int>(estimatedCount, StringComparer.Ordinal)
            : new Dictionary<string, int>(StringComparer.Ordinal);
        if (rows is List<EvalRow> rowList)
        {
            for (var i = 0; i < rowList.Count; i++)
            {
                if (!TryBuildCorrelatedLookupCompositeKey(keyPairs, rowList[i], ctes, useInnerSide: true, out var compositeKey))
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
                if (!TryBuildCorrelatedLookupCompositeKey(keyPairs, candidate, ctes, useInnerSide: true, out var compositeKey))
                    return null;

                if (compositeCounts.TryGetValue(compositeKey, out var currentCount))
                    compositeCounts[compositeKey] = currentCount + 1;
                else
                    compositeCounts[compositeKey] = 1;
            }
        }

        return new CorrelatedCountLookupState(compositeCounts, keyPairs, innerFilterExpr);
    }

    private bool TryGetCorrelatedCountLookupPattern(
        SqlExpr where,
        Source source,
        out IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        out SqlExpr? innerFilterExpr)
    {
        keyPairs = [];
        innerFilterExpr = null;

        var conjuncts = new List<SqlExpr>();
        FlattenConjuncts(where, conjuncts);
        if (conjuncts.Count == 0)
            return false;

        var pairs = new List<CorrelatedLookupKeyPair>();
        var filterParts = new List<SqlExpr>();

        foreach (var conjunct in conjuncts)
        {
            if (TryGetCorrelatedCountEquality(conjunct, source, out var innerKeyExpr, out var outerKeyExpr))
            {
                pairs.Add(new CorrelatedLookupKeyPair(innerKeyExpr, outerKeyExpr));
                continue;
            }

            if (!ExpressionUsesOnlyInnerColumnsOrConstants(conjunct, source))
                return false;

            filterParts.Add(conjunct);
        }

        if (pairs.Count == 0)
            return false;

        keyPairs = pairs;
        innerFilterExpr = filterParts.Count switch
        {
            0 => null,
            1 => filterParts[0],
            _ => CombineConjuncts(filterParts)
        };
        return true;
    }

    private static void FlattenConjuncts(SqlExpr expr, List<SqlExpr> conjuncts)
    {
        if (expr is BinaryExpr binary && binary.Op == SqlBinaryOp.And)
        {
            FlattenConjuncts(binary.Left, conjuncts);
            FlattenConjuncts(binary.Right, conjuncts);
            return;
        }

        conjuncts.Add(expr);
    }

    private static SqlExpr CombineConjuncts(IReadOnlyList<SqlExpr> conjuncts)
    {
        if (conjuncts.Count == 0)
            throw new InvalidOperationException("Nenhum conjuncto para combinar.");

        var combined = conjuncts[0];
        for (var i = 1; i < conjuncts.Count; i++)
            combined = new BinaryExpr(SqlBinaryOp.And, combined, conjuncts[i]);

        return combined;
    }

    private bool TryGetCorrelatedCountEquality(
        SqlExpr expression,
        Source source,
        out SqlExpr innerKeyExpr,
        out SqlExpr outerKeyExpr)
    {
        innerKeyExpr = null!;
        outerKeyExpr = null!;

        if (expression is not BinaryExpr eq || eq.Op != SqlBinaryOp.Eq)
            return false;

        var leftIsInner = TryResolveInnerColumnName(eq.Left, source, out _);
        var rightIsInner = TryResolveInnerColumnName(eq.Right, source, out _);

        if (leftIsInner == rightIsInner)
            return false;

        var otherSide = leftIsInner ? eq.Right : eq.Left;
        if (ExpressionReferencesInnerColumns(otherSide, source))
            return false;

        innerKeyExpr = leftIsInner ? eq.Left : eq.Right;
        outerKeyExpr = otherSide;
        return true;
    }

    private bool TryBuildCorrelatedLookupCompositeKey(
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        EvalRow row,
        IDictionary<string, Source> ctes,
        bool useInnerSide,
        out string key)
    {
        key = string.Empty;

        if (keyPairs.Count == 0)
            return false;

        var sb = new StringBuilder();
        for (var i = 0; i < keyPairs.Count; i++)
        {
            var expr = useInnerSide ? keyPairs[i].InnerExpr : keyPairs[i].OuterExpr;
            var value = Eval(expr, row, group: null, ctes);
            if (IsNullish(value))
                return false;

            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            AppendLookupScalarKeyComponent(sb, component);
        }

        key = sb.ToString();
        return true;
    }

    private static bool TryBuildInLookupCompositeKey(
        IReadOnlyList<object?> values,
        out string key)
    {
        key = string.Empty;

        if (values.Count == 0)
            return false;

        var sb = new StringBuilder();
        foreach (var value in values)
        {
            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            AppendLookupScalarKeyComponent(sb, component);
        }

        key = sb.ToString();
        return true;
    }

    private static void AppendLookupScalarKeyComponent(StringBuilder sb, InLookupScalarKey component)
    {
        if (sb.Length > 0)
            sb.Append('|');

        sb.Append(component.Kind.Length);
        sb.Append(':');
        sb.Append(component.Kind);
        sb.Append(';');
        sb.Append(component.Value.Length);
        sb.Append(':');
        sb.Append(component.Value);
    }

    private InMembershipState EvaluateRowMembershipCandidates(
        object?[] leftRow,
        IEnumerable<object?[]> candidates,
        ref bool hasNullCandidate)
    {
        foreach (var candidate in candidates)
        {
            if (TryEvaluateRowCandidateMembership(leftRow, candidate, ref hasNullCandidate, out var state)
                && state.Matched)
            {
                return state;
            }
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static bool TryResolveInnerColumnName(
        SqlExpr expr,
        Source source,
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
                        if (!source.ContainsColumnName(id.Name))
                            return false;

                        column = id.Name.NormalizeName();
                        return true;
                    }

                    var qualifier = id.Name[..dot].NormalizeName();
                    var sourceAlias = source.Alias.NormalizeName();
                    var sourceName = source.Name.NormalizeName();
                    if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                        && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                        return false;

                    var resolved = id.Name[(dot + 1)..].NormalizeName();
                    if (!source.ContainsColumnName(resolved))
                        return false;

                    column = resolved;
                    return true;
                }

            case ColumnExpr col:
                {
                    if (!string.IsNullOrWhiteSpace(col.Qualifier))
                    {
                        var qualifier = col.Qualifier.NormalizeName();
                        var sourceAlias = source.Alias.NormalizeName();
                        var sourceName = source.Name.NormalizeName();
                        if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                            && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }

                    var resolved = col.Name.NormalizeName();
                    if (!source.ContainsColumnName(resolved))
                        return false;

                    column = resolved;
                    return true;
                }

            default:
                return false;
        }
    }

    private static bool ExpressionReferencesInnerColumns(
        SqlExpr expr,
        Source source)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return TryResolveInnerColumnName(id, source, out _);
            case ColumnExpr col:
                return TryResolveInnerColumnName(col, source, out _);
            case LiteralExpr:
            case ParameterExpr:
                return false;
            case UnaryExpr unary:
                return ExpressionReferencesInnerColumns(unary.Expr, source);
            case IsNullExpr isNull:
                return ExpressionReferencesInnerColumns(isNull.Expr, source);
            case BinaryExpr binary:
                return ExpressionReferencesInnerColumns(binary.Left, source)
                    || ExpressionReferencesInnerColumns(binary.Right, source);
            case LikeExpr like:
                return ExpressionReferencesInnerColumns(like.Left, source)
                    || ExpressionReferencesInnerColumns(like.Pattern, source)
                    || (like.Escape is not null && ExpressionReferencesInnerColumns(like.Escape, source));
            case InExpr inExpr:
                if (ExpressionReferencesInnerColumns(inExpr.Left, source))
                    return true;
                foreach (var item in inExpr.Items)
                {
                    if (ExpressionReferencesInnerColumns(item, source))
                        return true;
                }
                return false;
            case BetweenExpr between:
                return ExpressionReferencesInnerColumns(between.Expr, source)
                    || ExpressionReferencesInnerColumns(between.Low, source)
                    || ExpressionReferencesInnerColumns(between.High, source);
            case FunctionCallExpr fn:
                return fn.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case CallExpr call:
                return call.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case JsonAccessExpr json:
                return ExpressionReferencesInnerColumns(json.Target, source)
                    || ExpressionReferencesInnerColumns(json.Path, source);
            case RowExpr row:
                return row.Items.Any(item => ExpressionReferencesInnerColumns(item, source));
            case CaseExpr c:
                if (c.BaseExpr is not null && ExpressionReferencesInnerColumns(c.BaseExpr, source))
                    return true;
                foreach (var when in c.Whens)
                {
                    if (ExpressionReferencesInnerColumns(when.When, source)
                        || ExpressionReferencesInnerColumns(when.Then, source))
                        return true;
                }
                return c.ElseExpr is not null && ExpressionReferencesInnerColumns(c.ElseExpr, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    private static bool ExpressionUsesOnlyInnerColumnsOrConstants(
        SqlExpr expr,
        Source source)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return TryResolveInnerColumnName(id, source, out _);
            case ColumnExpr col:
                return TryResolveInnerColumnName(col, source, out _);
            case LiteralExpr:
            case ParameterExpr:
                return true;
            case UnaryExpr unary:
                return ExpressionUsesOnlyInnerColumnsOrConstants(unary.Expr, source);
            case IsNullExpr isNull:
                return ExpressionUsesOnlyInnerColumnsOrConstants(isNull.Expr, source);
            case BinaryExpr binary:
                return ExpressionUsesOnlyInnerColumnsOrConstants(binary.Left, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(binary.Right, source);
            case LikeExpr like:
                return ExpressionUsesOnlyInnerColumnsOrConstants(like.Left, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(like.Pattern, source)
                    && (like.Escape is null || ExpressionUsesOnlyInnerColumnsOrConstants(like.Escape, source));
            case InExpr inExpr:
                if (!ExpressionUsesOnlyInnerColumnsOrConstants(inExpr.Left, source))
                    return false;
                return inExpr.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
            case BetweenExpr between:
                return ExpressionUsesOnlyInnerColumnsOrConstants(between.Expr, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(between.Low, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(between.High, source);
            case FunctionCallExpr fn:
                return fn.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case CallExpr call:
                return call.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case JsonAccessExpr json:
                return ExpressionUsesOnlyInnerColumnsOrConstants(json.Target, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(json.Path, source);
            case RowExpr row:
                return row.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
            case CaseExpr c:
                if (c.BaseExpr is not null && !ExpressionUsesOnlyInnerColumnsOrConstants(c.BaseExpr, source))
                    return false;
                foreach (var when in c.Whens)
                {
                    if (!ExpressionUsesOnlyInnerColumnsOrConstants(when.When, source)
                        || !ExpressionUsesOnlyInnerColumnsOrConstants(when.Then, source))
                        return false;
                }
                return c.ElseExpr is null || ExpressionUsesOnlyInnerColumnsOrConstants(c.ElseExpr, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    private static bool TryGetDecimalLiteral(SqlExpr expression, out decimal value)
    {
        value = default;

        if (expression is not LiteralExpr literal || literal.Value is null)
            return false;

        try
        {
            value = Convert.ToDecimal(literal.Value, CultureInfo.InvariantCulture);
        }
        catch
        {
            return false;
        }

        return true;
    }

    private static SqlBinaryOp ReverseComparisonOperator(SqlBinaryOp op)
        => op switch
        {
            SqlBinaryOp.Eq => SqlBinaryOp.Eq,
            SqlBinaryOp.Neq => SqlBinaryOp.Neq,
            SqlBinaryOp.Greater => SqlBinaryOp.Less,
            SqlBinaryOp.GreaterOrEqual => SqlBinaryOp.LessOrEqual,
            SqlBinaryOp.Less => SqlBinaryOp.Greater,
            SqlBinaryOp.LessOrEqual => SqlBinaryOp.GreaterOrEqual,
            _ => throw new InvalidOperationException($"Operador não reversível: {op}")
        };

    private bool TryEvalLogicalBinary(
        BinaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = expression.Op switch
        {
            SqlBinaryOp.And => Eval(expression.Left, row, group, ctes).ToBool()
                && Eval(expression.Right, row, group, ctes).ToBool(),
            SqlBinaryOp.Or => Eval(expression.Left, row, group, ctes).ToBool()
                || Eval(expression.Right, row, group, ctes).ToBool(),
            _ => null
        };

        return expression.Op is SqlBinaryOp.And or SqlBinaryOp.Or;
    }

    private static bool TryEvalArithmeticBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op is not (SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide))
        {
            result = null;
            return false;
        }

        if (left is null || right is null)
        {
            result = null;
            return true;
        }

        if (TryEvalDateIntervalArithmeticBinary(op, left, right, out result))
            return true;

        var leftNumber = ConvertBinaryArithmeticOperandToDecimal(left);
        var rightNumber = ConvertBinaryArithmeticOperandToDecimal(right);
        result = op switch
        {
            SqlBinaryOp.Add => leftNumber + rightNumber,
            SqlBinaryOp.Subtract => leftNumber - rightNumber,
            SqlBinaryOp.Multiply => leftNumber * rightNumber,
            SqlBinaryOp.Divide => rightNumber == 0m ? null : leftNumber / rightNumber,
            _ => throw new InvalidOperationException("op aritmético inválido")
        };
        return true;
    }

    private bool TryEvalConcatBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op != SqlBinaryOp.Concat)
        {
            result = null;
            return false;
        }

        var nullInputReturnsNull = Dialect?.ConcatReturnsNullOnNullInput ?? true;
        if (left is null or DBNull || right is null or DBNull)
        {
            if (nullInputReturnsNull)
            {
                result = null;
                return true;
            }
        }

        var leftText = left is null or DBNull ? string.Empty : left.ToString() ?? string.Empty;
        var rightText = right is null or DBNull ? string.Empty : right.ToString() ?? string.Empty;
        result = string.Concat(leftText, rightText);
        return true;
    }

    private static bool TryEvalDateIntervalArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        result = null;

        if (!TryCoerceDateTime(left, out var dateTime))
            return false;

        if (right is IntervalValue interval)
        {
            result = op switch
            {
                SqlBinaryOp.Add => dateTime.Add(interval.Span),
                SqlBinaryOp.Subtract => dateTime.Subtract(interval.Span),
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
            return true;
        }

        if (TryCoerceDateTime(right, out var rightDateTime) && op == SqlBinaryOp.Subtract)
        {
            result = (decimal)(dateTime.Date - rightDateTime.Date).TotalDays;
            return true;
        }

        if (!TryConvertNumericToDouble(right, out var dayOffset))
            return false;

        result = op switch
        {
            SqlBinaryOp.Add => dateTime.AddDays(dayOffset),
            SqlBinaryOp.Subtract => dateTime.AddDays(-dayOffset),
            _ => throw new InvalidOperationException("op aritmético inválido")
        };
        return true;
    }

    internal static bool TryConvertNumericToDouble(object? value, out double result)
    {
        result = 0d;
        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    internal static bool TryConvertNumericToDecimal(object? value, out decimal result)
    {
        result = 0m;
        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static decimal ConvertBinaryArithmeticOperandToDecimal(object value)
    {
        if (value is decimal decimalValue) return decimalValue;
        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        if (value is float singleValue) return Convert.ToDecimal(singleValue, CultureInfo.InvariantCulture);
        if (value is double doubleValue) return Convert.ToDecimal(doubleValue, CultureInfo.InvariantCulture);
        if (value is string text
            && decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedValue))
            return parsedValue;

        throw new InvalidOperationException($"Não consigo converter '{value}' para número.");
    }

    private bool TryEvalNullSafeEqualityBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op != SqlBinaryOp.NullSafeEq)
        {
            result = null;
            return false;
        }

        if (left is null && right is null)
        {
            result = true;
            return true;
        }

        if (left is null || right is null)
        {
            result = false;
            return true;
        }

        result = left.Compare(right, Dialect) == 0;
        return true;
    }

    private bool EvalComparisonBinary(SqlBinaryOp op, object left, object right)
    {
        var comparison = left.Compare(right, Dialect);

        return op switch
        {
            SqlBinaryOp.Eq => comparison == 0,
            SqlBinaryOp.Neq => comparison != 0,
            SqlBinaryOp.Greater => comparison > 0,
            SqlBinaryOp.GreaterOrEqual => comparison >= 0,
            SqlBinaryOp.Less => comparison < 0,
            SqlBinaryOp.LessOrEqual => comparison <= 0,
            SqlBinaryOp.Regexp => EvalRegexp(left, right, Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para REGEXP.")),
            SqlBinaryOp.SoundLike => EvalSoundLike(left, right),
            _ => throw new InvalidOperationException($"Binary op não suportado: {op}")
        };
    }

    private static bool EvalSoundLike(object left, object right)
    {
        var leftSoundex = ComputeSoundex(left.ToString() ?? string.Empty);
        var rightSoundex = ComputeSoundex(right.ToString() ?? string.Empty);
        return leftSoundex == rightSoundex;
    }

    private static bool EvalRegexp(object l, object r, ISqlDialect dialect)
    {
        try
        {
            var options = RegexOptions.CultureInvariant;
            if (dialect.RegexIsCaseInsensitive)
                options |= RegexOptions.IgnoreCase;

            return Regex.IsMatch(l.ToString() ?? "", r.ToString() ?? "", options);
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
        var leftSelector = TryCreateWindowValueSelector(i.Left, row);
        var leftVal = leftSelector is null
            ? Eval(i.Left, row, group, ctes)
            : leftSelector(row);

        if (leftVal is null)
            return false;

        var state = EvaluateInMembership(i, leftVal, row, group, ctes);
        return state.Matched;
    }

    /// <summary>
    /// EN: Evaluates SQL NOT IN semantics, handling NULL candidate propagation as UNKNOWN (filtered out).
    /// PT: Avalia semântica SQL de NOT IN, tratando propagação de candidatos NULL como UNKNOWN (filtrado).
    /// </summary>
    private bool EvalNotIn(
        InExpr i,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var leftSelector = TryCreateWindowValueSelector(i.Left, row);
        var leftVal = leftSelector is null
            ? Eval(i.Left, row, group, ctes)
            : leftSelector(row);
        if (leftVal is null || leftVal is DBNull)
            return false;

        var state = EvaluateInMembership(i, leftVal, row, group, ctes);
        if (state.Matched)
            return false;

        // SQL three-valued logic: x NOT IN (..., NULL, ...) => UNKNOWN when no match.
        if (state.HasNullCandidate)
            return false;

        return true;
    }

    /// <summary>
    /// EN: Computes IN membership state including matched candidates and NULL-candidate presence for SQL three-valued logic.
    /// PT: Calcula estado de pertencimento de IN incluindo candidatos casados e presença de candidato NULL para lógica SQL de três valores.
    /// </summary>
    private InMembershipState EvaluateInMembership(
        InExpr i,
        object leftVal,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var hasNullCandidate = false;

        if (TryEvaluateInSubqueryMembership(i, leftVal, row, ctes, ref hasNullCandidate, out var subqueryState))
            return subqueryState;

        foreach (var it in i.Items)
        {
            var v = Eval(it, row, group, ctes);
            if (TryEvaluateEnumerableMembership(leftVal, v, ref hasNullCandidate, out var enumerableState))
            {
                if (enumerableState.Matched)
                    return enumerableState;

                continue;
            }

            if (TryEvaluateCandidateMembership(leftVal, v, ref hasNullCandidate, out var candidateState))
                return candidateState;
        }

        return new InMembershipState(Matched: false, HasNullCandidate: hasNullCandidate);
    }

    private bool TryEvaluateInSubqueryMembership(
        InExpr expression,
        object leftVal,
        EvalRow row,
        IDictionary<string, Source> ctes,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        state = default;
        if (expression.Items.Count != 1 || expression.Items[0] is not SubqueryExpr subquery)
            return false;

        if (leftVal is object?[] leftRow)
        {
            var rowLookup = GetOrEvaluateInSubqueryRowLookup(subquery, row, ctes);
            hasNullCandidate |= rowLookup.HasNullCandidate;

            if (rowLookup.RowCandidates is not null
                && TryBuildInLookupCompositeKey(leftRow, out var rowKey))
            {
                state = CreateMembershipState(rowLookup.RowCandidates.Contains(rowKey), hasNullCandidate);
                return true;
            }

            state = EvaluateRowMembershipCandidates(leftRow, rowLookup.RowValues ?? [], ref hasNullCandidate);
            return true;
        }

        var scalarLookup = GetOrEvaluateInSubqueryLookup(subquery, row, ctes);
        hasNullCandidate |= scalarLookup.HasNullCandidate;

        if (scalarLookup.ScalarCandidates is not null
            && TryCreateInLookupScalarKey(leftVal, Dialect, out var scalarKey))
        {
            state = CreateMembershipState(scalarLookup.ScalarCandidates.Contains(scalarKey), hasNullCandidate);
            return true;
        }

        state = EvaluateMembershipCandidates(
            leftVal,
            scalarLookup.Values,
            ref hasNullCandidate);
        return true;
    }

    private InSubqueryLookupState GetOrEvaluateInSubqueryRowLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = BuildCorrelatedSubqueryCacheKey("IN_ROW_LOOKUP", sq.Sql, row);
        if (_subqueryEvaluationCache.TryGetOperationData(cacheKey, out InSubqueryLookupState? cachedState) && cachedState != null)
            return cachedState;

        return _subqueryEvaluationCache.GetOrAddOperationData(
            cacheKey,
            _ => BuildInSubqueryRowLookupState(sq, row, ctes));
    }

    private bool TryEvaluateEnumerableMembership(
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        state = default;
        if (candidateValue is not IEnumerable enumerable || candidateValue is string)
            return false;

        state = EvaluateMembershipCandidates(leftVal, enumerable, ref hasNullCandidate);
        return true;
    }

    private InMembershipState EvaluateMembershipCandidates(
        object leftVal,
        IEnumerable candidates,
        ref bool hasNullCandidate)
    {
        foreach (var candidate in candidates)
        {
            if (TryEvaluateCandidateMembership(leftVal, candidate, ref hasNullCandidate, out var state))
                return state;
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private bool TryEvaluateCandidateMembership(
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        if (IsSqlNullLike(candidateValue))
        {
            state = RegisterNullCandidate(ref hasNullCandidate);
            return false;
        }

        if (TryEvaluateRowCandidateMembership(leftVal, candidateValue, ref hasNullCandidate, out state))
            return state.Matched;

        if (leftVal.EqualsSql(candidateValue, Dialect))
        {
            state = CreateMembershipState(matched: true, hasNullCandidate);
            return true;
        }

        state = CreateMembershipState(matched: false, hasNullCandidate);
        return false;
    }

    private bool TryEvaluateRowCandidateMembership(
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        state = default;
        if (leftVal is not object?[] leftRow || candidateValue is not object?[] rightRow)
            return false;

        if (HasNullElement(leftRow) || HasNullElement(rightRow))
        {
            state = RegisterNullCandidate(ref hasNullCandidate);
            return true;
        }

        state = CreateMembershipState(RowValuesMatch(leftRow, rightRow), hasNullCandidate);
        return true;
    }

    private static InMembershipState RegisterNullCandidate(ref bool hasNullCandidate)
    {
        hasNullCandidate = true;
        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static InMembershipState CreateMembershipState(bool matched, bool hasNullCandidate)
        => new(Matched: matched, HasNullCandidate: hasNullCandidate);

    private bool RowValuesMatch(object?[] left, object?[] right)
    {
        if (left.Length != right.Length)
            return false;

        for (var i = 0; i < left.Length; i++)
        {
            if (!left[i].EqualsSql(right[i], Dialect))
                return false;
        }

        return true;
    }

    private static bool IsSqlNullLike(object? value)
        => value is null or DBNull;

    /// <summary>
    /// EN: Checks whether an object array contains at least one SQL NULL-like value.
    /// PT: Verifica se um array de objetos contém ao menos um valor SQL nulo.
    /// </summary>
    private static bool HasNullElement(object?[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is null or DBNull)
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
                    && actualValue.EqualsSql(kv.Value, Dialect))
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
                    || !actualValue.EqualsSql(equality.Value, Dialect))
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

                foreach (var rawRow in src.Rows())
                {
                    if (rawRow.TryGetValue(qualifiedColumnName!, out var actualValue)
                        && actualValue.EqualsSql(kv.Value, Dialect))
                    {
                        count++;
                    }
                }

                return true;
            }
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
                    || !actualValue.EqualsSql(equality.Value, Dialect))
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
        countArg = null!;

        SqlExpr expr;
        try
        {
            expr = ParseScalarExpr(exprRaw);
        }
        catch
        {
            return false;
        }

        switch (expr)
        {
            case CallExpr call when IsCountAggregateCall(call.Name):
                if (call.Distinct || call.WithinGroupOrderBy is not null || call.Filter is not null)
                    return false;

                if (call.Args.Count == 0)
                {
                    countArg = new StarExpr();
                    return true;
                }

                if (call.Args.Count == 1)
                {
                    countArg = call.Args[0];
                    return true;
                }

                return false;

            case FunctionCallExpr fn when IsCountAggregateCall(fn.Name):
                if (fn.Args.Count == 0)
                {
                    countArg = new StarExpr();
                    return true;
                }

                if (fn.Args.Count == 1)
                {
                    countArg = fn.Args[0];
                    return true;
                }

                return false;

            default:
                return false;
        }
    }

    private static bool IsCountAggregateCall(string name)
        => name.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("COUNT_BIG", StringComparison.OrdinalIgnoreCase);

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

        // UNKNOWN in WHERE is filtered out (same observable result as false).
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

        // If no FALSE but at least one UNKNOWN => UNKNOWN => filtered out in WHERE.
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

        var cmp = left.Compare(right, Dialect);
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

    /// <summary>
    /// EN: Resolves and caches first-column values for correlated IN-subquery evaluation using outer-row-aware cache keys.
    /// PT: Resolve e cacheia valores da primeira coluna para avaliação de IN-subquery correlacionada usando chaves de cache sensíveis à linha externa.
    /// </summary>
    private List<object?> GetOrEvaluateInSubqueryFirstColumnValues(
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


    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (c.BaseExpr is null)
            return EvaluateSearchedCase(c, row, group, ctes);

        return EvaluateSimpleCase(c, row, group, ctes);
    }

    private object? EvaluateSearchedCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        foreach (var whenThen in @case.Whens)
        {
            if (Eval(whenThen.When, row, group, ctes).ToBool())
                return Eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes);
    }

    private object? EvaluateSimpleCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var baseValue = Eval(@case.BaseExpr!, row, group, ctes);

        foreach (var whenThen in @case.Whens)
        {
            var whenValue = Eval(whenThen.When, row, group, ctes);
            if (ShouldSkipSimpleCaseMatch(baseValue, whenValue))
                continue;

            if (baseValue!.Compare(whenValue!, Dialect) == 0)
                return Eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes);
    }

    private object? EvaluateCaseElse(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => @case.ElseExpr is not null
            ? Eval(@case.ElseExpr, row, group, ctes)
            : null;

    private static bool ShouldSkipSimpleCaseMatch(object? baseValue, object? whenValue)
        => baseValue is null or DBNull || whenValue is null or DBNull;

    private object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => FunctionEvaluator.Evaluate(
            fn,
            row,
            group,
            ctes,
            Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para avaliação de função."),
            _evaluationLocalNow,
            _evaluationUtcNow,
            i => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null);

    private static bool TryEvalBoundScalarFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        // Prefer the executor hook stored in the registered function definition.
        var definition = fn.ResolvedScalarFunction;
        if (definition is null
            && !dialect.TryGetScalarFunctionDefinition(fn, out definition))
        {
            result = null;
            return false;
        }

        if (definition is null
            || !definition.AllowsCall
            || definition.AstExecutor is null)
        {
            result = null;
            return false;
        }

        return definition.AstExecutor(fn, dialect, evalArg, out result);
    }

    private bool TryEvalNonSqlServerScalarFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (QueryTextSearchFunctionHelper.TryEvalFindInSetFunction(fn, evalArg, out result)
            || QueryTextSearchFunctionHelper.TryEvalMatchAgainstFunction(fn, dialect, evalArg, out result)
            || QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions(fn, dialect, evalArg, out result))
        {
            return true;
        }

        if (IsFoundRowsEquivalentFunction(fn.Name, dialect))
        {
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            result = _cnn.GetLastFoundRows();
            return true;
        }

        if (QueryMySqlUtilityFunctionHelper.TryEvalUtilityFunctions(fn, dialect, evalArg, TryConvertNumericToInt64, out result)
            || QueryMySqlDateTimeFunctionHelper.TryEvalFunctions(fn, dialect, evalArg, TryConvertNumericToDouble, TryConvertNumericToInt64, TryCoerceDateTime, TryParseExactCachedDateTime, out result))
        {
            return true;
        }

        if (QueryMariaDbFunctionHelper.TryEvalFunctions(fn, dialect, evalArg, out result))
            return true;

        if (QueryMariaDbSpecialFunctionHelper.TryEvalFunctions(fn, dialect, evalArg, out result))
            return true;

        if (AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate(fn, dialect, row, group, ctes, evalArg, Eval, GetTemporalUnit, out result)
            || AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryMySqlUtilityFunctionEvaluator.TryEvaluate(fn, dialect, row, evalArg, TryConvertNumericToInt64, TryConvertNumericToDouble, out result))
        {
            return true;
        }

        if (QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions(fn, dialect, evalArg, TryCoerceDateTime, out result)
            || QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions(fn, dialect, evalArg, out result)
            || TryEvalConvertFunction(fn, dialect, evalArg, out result)
            || TryEvalCollationFunction(fn, dialect, evalArg, out result)
            || TryEvalConIdFunctions(fn, dialect, evalArg, out result)
            || TryEvalCubeTableFunction(fn, dialect, evalArg, out result)
            || TryEvalCvFunction(fn, dialect, evalArg, out result)
            || TryEvalDataObjToPartitionFunctions(fn, dialect, evalArg, out result)
            || TryEvalDepthFunction(fn, dialect, evalArg, out result)
            || TryEvalDerefFunction(fn, dialect, evalArg, out result)
            || TryEvalDumpFunction(fn, dialect, evalArg, out result)
            || TryEvalExistsNodeFunction(fn, dialect, evalArg, out result)
            || TryEvalFromTzFunction(fn, dialect, evalArg, out result)
            || TryEvalGroupIdFunction(fn, dialect, out result)
            || TryEvalHexToRawFunction(fn, dialect, evalArg, out result)
            || TryEvalIterationNumberFunction(fn, dialect, out result)
            || TryEvalJsonDataGuideFunction(fn, dialect, evalArg, out result)
            || TryEvalJsonTransformFunction(fn, dialect, evalArg, out result)
            || TryEvalLnnvlFunction(fn, dialect, evalArg, out result)
            || TryEvalLocalTimeFunction(fn, dialect, out result)
            || TryEvalLocalTimestampFunction(fn, dialect, out result)
            || TryEvalLowerFunction(fn, dialect, evalArg, out result)
            || TryEvalLtrimFunction(fn, dialect, evalArg, out result)
            || TryEvalDivFunction(fn, dialect, evalArg, out result)
            || TryEvalModFunction(fn, dialect, evalArg, out result)
            || TryEvalMonthsBetweenFunction(fn, dialect, evalArg, out result)
            || TryEvalMidnightSecondsFunction(fn, dialect, evalArg, out result)
            || TryEvalNanvlFunction(fn, dialect, evalArg, out result)
            || TryEvalNewTimeFunction(fn, dialect, evalArg, out result)
            || TryEvalNextDayFunction(fn, dialect, evalArg, out result)
            || TryEvalNlsFunctions(fn, dialect, evalArg, out result)
            || TryEvalNumIntervalFunctions(fn, dialect, evalArg, out result)
            || TryEvalMakeRefFunction(fn, dialect, evalArg, out result)
            || AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || TryEvalTranslateFunctions(fn, dialect, evalArg, out result)
            || AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresSystemFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, _cnn.GetCurrentQueryText, out result)
            || AstQueryPostgresDateFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || TryEvalDb2DateTruncFunction(fn, dialect, evalArg, out result)
            || AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresTextFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryPostgresUuidFunctionEvaluator.TryEvaluate(fn, dialect, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalSqlServerAndCompatibilityFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalNumericFunction(fn, evalArg, out result)
            || TryEvalAppNameFunction(fn, out result)
            || TryEvalCharIndexFunction(fn, evalArg, out result)
            || TryEvalCurrentUserFunction(fn, dialect, out result))
        {
            return true;
        }

        SqlServerFunctionSupportHelper.EnsureSupport(fn, dialect);

        if (SqlServerSessionFunctionEvaluator.TryEvaluate(fn, evalArg, out result))
            return true;

        if (SqlServerDatabaseFunctionEvaluator.TryEvaluate(fn, evalArg, out result)
            || SqlServerIdentityFunctionEvaluator.TryEvaluate(fn, evalArg, out result)
            || SqlServerUtilityFunctionEvaluator.TryEvaluate(fn, evalArg, out result)
            || AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || TryEvalDataLengthFunction(fn, evalArg, out result)
            || AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, GetTemporalUnit, ResolveTemporalUnit, out result)
            || AstQueryDb2DateFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, ResolveTemporalUnit, out result)
            || TryEvalDegreesFunction(fn, evalArg, out result)
            || AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate(fn, dialect, row, group, ctes, evalArg, Eval, GetTemporalUnit, out result))
        {
            return true;
        }

        if (fn.Name.Equals("EOMONTH", StringComparison.OrdinalIgnoreCase)
            && !(fn.ResolvedScalarFunction?.AllowsCall
                ?? (dialect.TryGetScalarFunctionDefinition(fn, out var eomonthDefinition)
                    && eomonthDefinition is not null
                    && eomonthDefinition.AllowsCall)))
        {
            throw SqlUnsupported.ForDialect(dialect, "EOMONTH");
        }

        if (TryEvalEomonthFunction(fn, evalArg, out result)
            || TryEvalDifferenceFunction(fn, evalArg, out result)
            || TryEvalErrorFunctions(fn, out result)
            || TryEvalExpFunction(fn, evalArg, out result)
            || TryEvalFloorFunction(fn, evalArg, out result)
            || TryEvalSqlServerFormatFunction(fn, dialect, evalArg, out result)
            || TryEvalSqlServerFormatMessageFunction(fn, evalArg, out result)
            || TryEvalSqlServerCompressFunction(fn, evalArg, out result)
            || TryEvalSqlServerDecompressFunction(fn, evalArg, out result)
            || TryEvalSqlServerChecksumFunction(fn, evalArg, out result))
        {
            return true;
        }

        if (fn.Name.Equals("GETUTCDATE", StringComparison.OrdinalIgnoreCase)
            && !(fn.ResolvedScalarFunction?.AllowsCall
                ?? (dialect.TryGetScalarFunctionDefinition(fn, out var getUtcDateDefinition)
                    && getUtcDateDefinition is not null
                    && getUtcDateDefinition.AllowsCall)))
        {
            throw SqlUnsupported.ForDialect(dialect, "GETUTCDATE");
        }

        if (TryEvalGetUtcDateFunction(fn, out result))
            return true;

        result = null;
        return false;
    }

    private bool TryEvalGeneralScalarFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalGeneralSystemAndJsonFunctions(fn, dialect, evalArg, out result)
            || TryEvalGeneralTextAndMathFunctions(fn, dialect, evalArg, out result)
            || TryEvalGeneralDateAndTimeFunctions(fn, row, group, ctes, dialect, evalArg, out result))
        {
            return true;
        }

        EnsureDialectSupportsSequenceFunction(fn.Name);
        if (SqlSequenceEvaluator.TryEvaluateCall(_cnn, fn.Name, fn.Args, expr => Eval(expr, row, group, ctes), out var sequenceValue))
        {
            result = sequenceValue;
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalGeneralSystemAndJsonFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
        => GeneralSystemAndJsonFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result);

    private bool TryEvalGeneralTextAndMathFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryGeneralScalarFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result);

    private bool TryEvalGeneralDateAndTimeFunctions(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalLastInsertIdFunction(fn, evalArg, out result))
        {
            return true;
        }

        if (AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result))
        {
            return true;
        }

        if (AstQueryGeneralScalarFunctionEvaluator.TryEvalSubDateFunction(fn, row, group, ctes, ParseIntervalValue, evalArg, out result))
        {
            return true;
        }

        return AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result);
    }

    private bool TryEvalLastInsertIdFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("LAST_INSERT_ID", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LAST_INSERT_ROWID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count > 0)
        {
            var value = evalArg(0);
            _cnn.SetLastInsertId(value);
            result = value;
            return true;
        }

        result = _cnn.GetLastInsertId() ?? 0;
        return true;
    }

    private bool TryEvalCastStringAndDateTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
        => CastStringAndDateTailEvaluator.TryEvaluate(fn, row, group, ctes, dialect, evalArg, out result);

    private bool TryEvalCastConversionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
        => CastConversionFamilyEvaluator.TryEvaluate(fn, dialect, evalArg, out result);

    private bool TryEvalCastConcatAndStringTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = TryEvalConcatFunctions(fn, evalArg, out var handledConcat);
        if (handledConcat)
            return true;

        if (TryEvalCharFunction(fn, dialect, evalArg, out result)
            || TryEvalDialectSpecificCastFunction(fn, dialect, evalArg, out result)
            || TryEvalBasicStringFunction(fn, evalArg, out result)
            || TryEvalSubstringFunction(fn, evalArg, out result)
            || TryEvalReplaceFunction(fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalCastDateTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate(fn, dialect, row, group, ctes, evalArg, Eval, GetTemporalUnit, out result))
        {
            return true;
        }

        if (AstQueryGeneralDateFunctionEvaluator.TryEvaluate(fn, dialect, evalArg, out result)
            || AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, GetTemporalUnit, ResolveTemporalUnit, out result)
            || TryEvalFieldFunction(fn, dialect, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalUserDefinedScalarFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = null;

        if (!_cnn.TryGetFunction(fn.Name, out var function) || function is null)
            return false;

        if (fn.Args.Count != function.Parameters.Count)
            throw new InvalidOperationException($"Function '{fn.Name}' expects {function.Parameters.Count} argument(s), but received {fn.Args.Count}.");

        var parameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < function.Parameters.Count; i++)
        {
            var parameter = function.Parameters[i];
            parameterScope[parameter.NormalizedName] = Eval(fn.Args[i], row, group, ctes);
        }

        _localParameterScopes.Push(parameterScope);
        try
        {
            result = Eval(function.Body, row, group, ctes);
            return true;
        }
        finally
        {
            _localParameterScopes.Pop();
        }
    }

    private bool TryResolveLocalFunctionValue(string name, out object? value)
    {
        var normalized = ProcedureDef.NormalizeParamName(name);
        foreach (var scope in _localParameterScopes)
        {
            if (scope.TryGetValue(normalized, out value))
                return true;
        }

        value = null;
        return false;
    }

    internal static bool TryEvalCharFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHAR", StringComparison.OrdinalIgnoreCase)
            && !fn.Name.Equals("NCHAR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        // SQL Server/MySQL CHAR(n) and SQL Server NCHAR(n) return the character represented by the numeric code.
        if (dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            || MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            try
            {
                var codePoint = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                result = char.ConvertFromUtf32(codePoint);
                return true;
            }
            catch
            {
                // Fall back to textual conversion when the argument is not numeric.
            }
        }

        result = value!.ToString() ?? string.Empty;
        return true;
    }

    internal static bool TryEvalConvertFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("CONVERT() espera ao menos um argumento.");

            var value1 = evalArg(0);
            if (IsNullish(value1))
            {
                result = null;
                return true;
            }

            result = value1 is string text2 ? text2 : value1!.ToString();
            return true;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value is string text ? text : value!.ToString();
        return true;
    }

    internal static bool TryConvertNumericToInt64(object value, out long numeric)
    {
        switch (value)
        {
            case sbyte sb:
                numeric = sb;
                return true;
            case byte b:
                numeric = b;
                return true;
            case short s:
                numeric = s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i:
                numeric = i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l:
                numeric = l;
                return true;
            case ulong ul when ul <= long.MaxValue:
                numeric = (long)ul;
                return true;
        }

        return long.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    private static bool TryEvalCollationFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "COLLATION")
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = "BINARY";
        return true;
    }

    private static bool TryEvalConIdFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CON_DBID_TO_ID" or "CON_GUID_TO_ID" or "CON_NAME_TO_ID" or "CON_UID_TO_ID"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Convert.ToInt64(value.ToDec(), CultureInfo.InvariantCulture);
        }
        catch
        {
            result = null;
        }

        return true;
    }

    private static bool TryEvalDialectSpecificCastFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            return false;

        var normalizedName = fn.Name.ToUpperInvariant();
        if (normalizedName is not ("BIGINT" or "BPCHAR" or "DBCLOB" or "DEC" or "DECIMAL" or "DOUBLE" or "DOUBLE_PRECISION" or "FLOAT" or "FLOAT4" or "FLOAT8" or "GRAPHIC" or "INT" or "INTEGER" or "REAL" or "SMALLINT" or "VARGRAPHIC" or "VARCHAR"))
            return false;

        if (fn.Args.Count == 0)
            return false;

        var value = evalArg(0);
        if (IsNullish(value))
            return true;

        try
        {
            result = normalizedName switch
            {
                "BIGINT" => CoerceToInt64(value!),
                "SMALLINT" => CoerceToInt16(value!),
                "INT" or "INTEGER" => CoerceToInt32(value!),
                "DEC" or "DECIMAL" => CoerceToDecimal(value!),
                "DOUBLE" or "DOUBLE_PRECISION" or "FLOAT" or "FLOAT4" or "FLOAT8" or "REAL" => CoerceToDouble(value!),
                "BPCHAR" or "DBCLOB" or "GRAPHIC" or "VARGRAPHIC" => value?.ToString(),
                "VARCHAR" => value?.ToString(),
                _ => null
            };
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            LogFunctionEvaluationFailure(e);
            result = null;
        }
#pragma warning restore CA1031

        return true;
    }

    private static long CoerceToInt64(object value)
    {
        if (value is long longValue)
            return longValue;

        if (value is int intValue)
            return intValue;

        if (value is short shortValue)
            return shortValue;

        if (value is decimal decimalValue)
            return (long)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return parsedLong;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (long)parsedDecimal;

        return 0L;
    }

    private static int CoerceToInt32(object value)
    {
        if (value is int intValue)
            return intValue;

        if (value is long longValue)
            return (int)longValue;

        if (value is short shortValue)
            return shortValue;

        if (value is decimal decimalValue)
            return (int)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
            return parsedInt;

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return (int)parsedLong;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (int)parsedDecimal;

        return 0;
    }

    private static short CoerceToInt16(object value)
    {
        if (value is short shortValue)
            return shortValue;

        if (value is int intValue)
            return (short)intValue;

        if (value is long longValue)
            return (short)longValue;

        if (value is decimal decimalValue)
            return (short)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (short.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedShort))
            return parsedShort;

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
            return (short)parsedInt;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return (short)parsedDecimal;

        return 0;
    }

    private static decimal CoerceToDecimal(object value)
    {
        if (value is decimal decimalValue)
            return decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
            return parsedDecimal;

        return 0m;
    }

    private static double CoerceToDouble(object value)
    {
        if (value is double doubleValue)
            return doubleValue;

        if (value is float floatValue)
            return floatValue;

        if (value is decimal decimalValue)
            return (double)decimalValue;

        var text = value.ToString()?.Trim() ?? string.Empty;
        if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDouble))
            return parsedDouble;

        return 0d;
    }

    private static bool TryEvalCubeTableFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CUBE_TABLE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalCvFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDataObjToPartitionFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("DATAOBJ_TO_MAT_PARTITION", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATAOBJ_TO_PARTITION", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDepthFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DEPTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalDerefFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DEREF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = evalArg(0);
        return true;
    }

    private static bool TryEvalDumpFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DUMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = $"Typ=1 Len={text.Length}";
        return true;
    }

    private static bool TryEvalExistsNodeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("EXISTSNODE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalFromTzFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FROM_TZ", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FROM_TZ() espera data e fuso.");

        var baseValue = evalArg(0);
        var tzValue = evalArg(1)?.ToString();
        if (IsNullish(baseValue) || string.IsNullOrWhiteSpace(tzValue))
        {
            result = null;
            return true;
        }

        if (!TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!TryParseOffset(tzValue!, out var offset))
        {
            result = null;
            return true;
        }

        result = new DateTimeOffset(dateTime, offset);
        return true;
    }

    private static bool TryEvalGroupIdFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("GROUP_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = 0;
        return true;
    }

    private static bool TryEvalHexToRawFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("HEXTORAW", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            result = null;
            return true;
        }

        if (!TryNormalizeHexPayload(value, out var hex) || hex.Length % 2 != 0)
        {
            result = null;
            return true;
        }

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!byte.TryParse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
            {
                result = null;
                return true;
            }

            buffer[i / 2] = part;
        }

        result = buffer;
        return true;
    }

    private static bool TryEvalIterationNumberFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("ITERATION_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalJsonDataGuideFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_DATAGUIDE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = "{}";
        return true;
    }

    private static bool TryEvalJsonTransformFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "JSON_TRANSFORM")
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value is string text ? text : value!.ToString();
        return true;
    }

    private static bool TryEvalLnnvlFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LNNVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = 1;
            return true;
        }

        result = value.ToBool() ? 0 : 1;
        return true;
    }

    private static bool TryEvalLocalTimestampFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("LOCALTIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!(dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            || dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = DateTime.Now;
        return true;
    }

    private static bool TryEvalLocalTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("LOCALTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!(dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            || dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = DateTime.Now.TimeOfDay;
        return true;
    }

    private static bool TryEvalLowerFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString()?.ToLowerInvariant();
        return true;
    }

    private static bool TryEvalLtrimFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString()?.TrimStart();
        return true;
    }

    private static bool TryEvalModFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MOD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MOD() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            var r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            result = r == 0m ? null : l % r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDivFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DIV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DIV() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            var r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            result = r == 0m ? null : decimal.Truncate(l / r);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMonthsBetweenFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MONTHS_BETWEEN", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MONTHS_BETWEEN() espera duas datas.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        if (!TryCoerceDateTime(left, out var leftDate) || !TryCoerceDateTime(right, out var rightDate))
        {
            result = null;
            return true;
        }

        var monthsLeft = leftDate.Year * 12 + leftDate.Month;
        var monthsRight = rightDate.Year * 12 + rightDate.Month;
        var monthDiff = monthsLeft - monthsRight;
        var dayDiff = (leftDate.Day - rightDate.Day) / 31m;
        result = monthDiff + dayDiff;
        return true;
    }

    private static bool TryEvalMidnightSecondsFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MIDNIGHT_SECONDS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is TimeSpan timeSpan)
        {
            result = (int)timeSpan.TotalSeconds;
            return true;
        }

        if (value is DateTime dateTime)
        {
            result = (int)dateTime.TimeOfDay.TotalSeconds;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            result = (int)dto.TimeOfDay.TotalSeconds;
            return true;
        }

        if (TryCoerceTimeSpan(value, out var parsedTime))
        {
            result = (int)parsedTime.TotalSeconds;
            return true;
        }

        if (TryCoerceDateTime(value, out var parsedDate))
        {
            result = (int)parsedDate.TimeOfDay.TotalSeconds;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalNanvlFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NANVL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("NANVL() espera 2 argumentos.");

        var first = evalArg(0);
        var second = evalArg(1);
        if (IsNullish(first))
        {
            result = second;
            return true;
        }

        var number = Convert.ToDouble(first, CultureInfo.InvariantCulture);
        result = double.IsNaN(number) ? second : first;
        return true;
    }

    private static bool TryEvalNewTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NEW_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("NEW_TIME() espera data e dois fusos.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        if (!TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var fromTz = evalArg(1)?.ToString() ?? string.Empty;
        var toTz = evalArg(2)?.ToString() ?? string.Empty;
        if (!TryParseOffset(fromTz, out var fromOffset) || !TryParseOffset(toTz, out var toOffset))
        {
            result = null;
            return true;
        }

        var dto = new DateTimeOffset(dateTime, fromOffset);
        result = dto.ToOffset(toOffset).DateTime;
        return true;
    }

    private static bool TryEvalNextDayFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NEXT_DAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("NEXT_DAY() espera data e nome do dia.");

        var baseValue = evalArg(0);
        var dayValue = evalArg(1)?.ToString();
        if (IsNullish(baseValue) || string.IsNullOrWhiteSpace(dayValue))
        {
            result = null;
            return true;
        }

        if (!TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!TryParseOracleDayOfWeek(dayValue!, out var targetDay))
        {
            result = null;
            return true;
        }

        var current = dateTime.Date;
        var daysAhead = ((int)targetDay - (int)current.DayOfWeek + 7) % 7;
        if (daysAhead == 0)
            daysAhead = 7;

        result = current.AddDays(daysAhead);
        return true;
    }

    private static bool TryEvalNlsFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID" or "NLS_CHARSET_NAME" or "NLS_COLLATION_ID"
            or "NLS_COLLATION_NAME" or "NLS_INITCAP" or "NLS_LOWER" or "NLS_UPPER" or "NLSSORT"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        if (name is "NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID")
        {
            result = 0;
            return true;
        }

        if (name is "NLS_CHARSET_NAME")
        {
            result = "AL32UTF8";
            return true;
        }

        if (name is "NLS_COLLATION_ID")
        {
            result = 0;
            return true;
        }

        if (name is "NLS_COLLATION_NAME")
        {
            result = "BINARY";
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (name is "NLS_INITCAP")
        {
            result = ApplyInitCap(text);
            return true;
        }

        if (name is "NLS_LOWER")
        {
            result = text.ToLowerInvariant();
            return true;
        }

        if (name is "NLS_UPPER")
        {
            result = text.ToUpperInvariant();
            return true;
        }

        result = text;
        return true;
    }

    private static bool TryEvalNumIntervalFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NUMTOYMINTERVAL", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() espera número e unidade.");

        var numberValue = evalArg(0);
        var unitValue = evalArg(1)?.ToString();
        if (IsNullish(numberValue) || string.IsNullOrWhiteSpace(unitValue))
        {
            result = null;
            return true;
        }

        var number = Convert.ToDouble(numberValue, CultureInfo.InvariantCulture);
        var unit = unitValue!.Trim().ToUpperInvariant();
        if (fn.Name.Equals("NUMTODSINTERVAL", StringComparison.OrdinalIgnoreCase))
        {
            result = unit switch
            {
                "DAY" or "DAYS" => TimeSpan.FromDays(number),
                "HOUR" or "HOURS" => TimeSpan.FromHours(number),
                "MINUTE" or "MINUTES" => TimeSpan.FromMinutes(number),
                "SECOND" or "SECONDS" => TimeSpan.FromSeconds(number),
                _ => (TimeSpan?)null
            };
            return true;
        }

        // NUMTOYMINTERVAL
        result = unit switch
        {
            SqlConst.YEAR or "YEARS" => TimeSpan.FromDays(365d * number),
            "MONTH" or "MONTHS" => TimeSpan.FromDays(30d * number),
            _ => (TimeSpan?)null
        };
        return true;
    }

    private static bool TryEvalMakeRefFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MAKE_REF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDb2DateTruncFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATE_TRUNC", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATE_TRUNC() espera unidade e data.");

        var unitText = evalArg(0)?.ToString() ?? string.Empty;
        var value = evalArg(1);
        if (IsNullish(value) || string.IsNullOrWhiteSpace(unitText) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var unit = ResolveTemporalUnit(unitText);
        result = TruncateDateTime(dateTime, unit);
        return true;
    }

    private static bool TryEvalPostgresJsonFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is "TO_JSON" or "TO_JSONB" or "ROW_TO_JSON")
        {
            var value = evalArg(0);
            result = IsNullish(value) ? null : JsonSerializer.Serialize(value);
            return true;
        }

        if (name is "JSON_SCALAR" or "JSON_SERIALIZE")
        {
            if (dialect.Version < 17)
                throw SqlUnsupported.ForDialect(dialect, name);

            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonCandidate(value!, out var candidate))
            {
                result = null;
                return true;
            }

            result = candidate.GetRawText();
            return true;
        }

        if (name is "JSONB_PATH_EXISTS" or "JSONB_PATH_QUERY_ARRAY")
        {
            if (dialect.Version < 12)
                throw SqlUnsupported.ForDialect(dialect, name);

            if (fn.Args.Count < 2)
                throw new InvalidOperationException($"{name}() espera JSONB e jsonpath.");

            var value = evalArg(0);
            var path = evalArg(1)?.ToString();
            if (IsNullish(value) || string.IsNullOrWhiteSpace(path))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            if (!TryReadPostgresJsonPath(element, path!, out var target))
            {
                result = name == "JSONB_PATH_EXISTS" ? false : "[]";
                return true;
            }

            if (name == "JSONB_PATH_EXISTS")
            {
                result = true;
                return true;
            }

            result = BuildJsonArray(new object?[] { target });
            return true;
        }

        if (name is "JSON_TYPEOF" or "JSONB_TYPEOF")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Object => "object",
                JsonValueKind.Array => "array",
                JsonValueKind.String => "string",
                JsonValueKind.Number => "number",
                JsonValueKind.True => "boolean",
                JsonValueKind.False => "boolean",
                JsonValueKind.Null => "null",
                _ => null
            };
            return true;
        }

        if (name is "JSON_ARRAY_LENGTH" or "JSONB_ARRAY_LENGTH")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element)
                || element.ValueKind != JsonValueKind.Array)
            {
                result = null;
                return true;
            }

            result = element.GetArrayLength();
            return true;
        }

        if (name is "JSON_BUILD_ARRAY" or "JSONB_BUILD_ARRAY")
        {
            var values = new object?[fn.Args.Count];
            for (var i = 0; i < fn.Args.Count; i++)
                values[i] = evalArg(i);

            result = BuildJsonArray(values);
            return true;
        }

        if (name is "JSON_BUILD_OBJECT" or "JSONB_BUILD_OBJECT")
        {
            if (fn.Args.Count % 2 != 0)
                throw new InvalidOperationException($"{name}() espera um numero par de argumentos.");

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < fn.Args.Count; i += 2)
            {
                var key = evalArg(i)?.ToString() ?? string.Empty;
                var val = evalArg(i + 1);
                pairs.Add((key, val));
            }

            result = BuildJsonObject(pairs);
            return true;
        }

        if (name is "JSON_EXTRACT_PATH" or "JSONB_EXTRACT_PATH" or "JSON_EXTRACT_PATH_TEXT" or "JSONB_EXTRACT_PATH_TEXT")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var pathSegment = evalArg(i)?.ToString();
                System.Text.Json.JsonElement nextElement;
                if (string.IsNullOrEmpty(pathSegment)
                    || !TryReadPostgresJsonPathElement(element, pathSegment!, out nextElement))
                {
                    result = null;
                    return true;
                }

                element = nextElement;
            }

            if (name.EndsWith("_TEXT", StringComparison.Ordinal))
            {
                result = element.ValueKind switch
                {
                    JsonValueKind.String => element.GetString(),
                    JsonValueKind.Null => null,
                    _ => element.GetRawText()
                };
                return true;
            }

            result = element.GetRawText();
            return true;
        }

        if (name is "JSON_STRIP_NULLS" or "JSONB_STRIP_NULLS")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (IsNullish(value) || !TryParseJsonNode(value!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            var normalized = CloneJsonNode(root);
            StripJsonNullProperties(normalized);
            result = normalized.ToJsonString();
            return true;
        }

        if (name is "JSONB_OBJECT")
        {
            if (fn.Args.Count == 1)
            {
                if (!TryReadPostgresTextArray(evalArg(0), out var entries) || entries.Count % 2 != 0)
                {
                    result = null;
                    return true;
                }

                var pairs = new List<(string Key, object? Value)>();
                for (var i = 0; i < entries.Count; i += 2)
                    pairs.Add((entries[i], entries[i + 1]));

                result = BuildJsonObject(pairs);
                return true;
            }

            if (fn.Args.Count == 2)
            {
                if (!TryReadPostgresTextArray(evalArg(0), out var keys)
                    || !TryReadPostgresTextArray(evalArg(1), out var values)
                    || keys.Count != values.Count)
                {
                    result = null;
                    return true;
                }

                var pairs = new List<(string Key, object? Value)>();
                for (var i = 0; i < keys.Count; i++)
                    pairs.Add((keys[i], values[i]));

                result = BuildJsonObject(pairs);
                return true;
            }

            result = null;
            return true;
        }

        if (name is "JSONB_SET" or "JSONB_SET_LAX")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException($"{name}() espera JSON, caminho e novo valor.");

            var json = evalArg(0);
            var pathValue = evalArg(1);
            var newValue = evalArg(2);
            if (IsNullish(json) || IsNullish(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            if (!TryParsePostgresJsonPathTokens(pathValue!, out var tokens))
            {
                result = null;
                return true;
            }

            if (name is "JSONB_SET_LAX" && IsNullish(newValue))
            {
                var createIfMissingLax = fn.Args.Count < 4 || Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
                var treatment = fn.Args.Count > 4
                    ? (evalArg(4)?.ToString() ?? "use_json_null").Trim().ToLowerInvariant()
                    : "use_json_null";

                if (treatment == "return_target")
                {
                    result = root.ToJsonString();
                    return true;
                }

                if (treatment == "delete_key")
                {
                    if (tokens.Count > 0)
                    {
                        if (createIfMissingLax || TryGetJsonNodeAtPath(root, tokens, out _))
                            TryRemoveJsonPathValue(root, tokens);
                    }

                    result = root.ToJsonString();
                    return true;
                }

                if (treatment == "raise_exception")
                    throw new InvalidOperationException("JSONB_SET_LAX() recebeu null com tratamento raise_exception.");

                newValue = null;
            }

            var createIfMissing = fn.Args.Count < 4 || Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
            if (!createIfMissing && !TryGetJsonNodeAtPath(root, tokens, out _))
            {
                result = root.ToJsonString();
                return true;
            }

            if (!TrySetJsonPathValue(ref root, tokens, newValue))
            {
                result = null;
                return true;
            }

            result = root.ToJsonString();
            return true;
        }

        if (name is "JSONB_INSERT")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSONB_INSERT() espera JSON, caminho e novo valor.");

            var json = evalArg(0);
            var pathValue = evalArg(1);
            var newValue = evalArg(2);
            var insertAfter = fn.Args.Count > 3 && Convert.ToBoolean(evalArg(3), CultureInfo.InvariantCulture);
            if (IsNullish(json) || IsNullish(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            if (!TryParsePostgresJsonPathTokens(pathValue!, out var tokens)
                || tokens.Count == 0)
            {
                result = null;
                return true;
            }

            if (!TryInsertJsonPathValue(root, tokens, newValue, insertAfter))
            {
                result = null;
                return true;
            }

            result = root.ToJsonString();
            return true;
        }

        if (name is "JSONB_PRETTY")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var value = evalArg(0);
            if (IsNullish(value) || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var options = new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true
            };
            result = JsonSerializer.Serialize(element, options)
                .Replace(@"
", "\n");
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalTranslateFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TRANSLATE", StringComparison.OrdinalIgnoreCase)
            && !fn.Name.Equals("TRANSLATE...USING", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var from = evalArg(1)?.ToString() ?? string.Empty;
        var to = evalArg(2)?.ToString() ?? string.Empty;

        var builder = new StringBuilder(source.Length);
        foreach (var ch in source)
        {
            var index = from.IndexOf(ch);
            if (index < 0)
            {
                builder.Append(ch);
                continue;
            }

            if (index < to.Length)
                builder.Append(to[index]);
        }

        result = builder.ToString();
        return true;
    }

    internal static bool TryParseOffset(string value, out TimeSpan offset)
    {
        offset = default;
        var trimmed = value.Trim();
        if (string.Equals(trimmed, "UTC", StringComparison.OrdinalIgnoreCase))
        {
            offset = TimeSpan.Zero;
            return true;
        }

        if (TryParseCachedTimeSpan(trimmed, out offset))
            return true;

        if (trimmed.Length == 6
            && (trimmed[0] == '+' || trimmed[0] == '-')
            && trimmed[3] == ':')
        {
            if (int.TryParse(trimmed[1..3], out var hours)
                && int.TryParse(trimmed[4..6], out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (trimmed[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        if (trimmed.Length == 5 && (trimmed[0] == '+' || trimmed[0] == '-'))
        {
            if (int.TryParse(trimmed[1..3], out var hours)
                && int.TryParse(trimmed[3..5], out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (trimmed[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        return false;
    }

    private static bool TryParseOracleDayOfWeek(string value, out DayOfWeek day)
    {
        day = default;
        var normalized = value.Trim().ToUpperInvariant();
        if (_oracleDayOfWeekMap.TryGetValue(normalized, out day))
            return true;

        return false;
    }

    private static string ApplyInitCap(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var builder = new StringBuilder(value.Length);
        var makeUpper = true;
        foreach (var ch in value)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(makeUpper
                    ? char.ToUpperInvariant(ch)
                    : char.ToLowerInvariant(ch));
                makeUpper = false;
            }
            else
            {
                builder.Append(ch);
                makeUpper = true;
            }
        }

        return builder.ToString();
    }

    private static bool TryNormalizeHexPayload(string trimmed, out string hex)
    {
        hex = string.Empty;

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = trimmed[2..];
            return true;
        }

        if (trimmed.Length >= 3
            && (trimmed[0] == 'x' || trimmed[0] == 'X')
            && trimmed[1] == '\''
            && trimmed[^1] == '\'')
        {
            hex = trimmed[2..^1];
            return true;
        }

        hex = trimmed;
        return true;
    }

    internal static bool TryEvalNumericFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        var value = evalArg(0);

        if (((name.Equals("ABS", StringComparison.OrdinalIgnoreCase)) || (name.Equals("ABSVAL", StringComparison.OrdinalIgnoreCase))))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                if (value is decimal dec)
                {
                    result = Math.Abs(dec);
                    return true;
                }

                if (value is float or double)
                {
                    result = Math.Abs(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                    return true;
                }

                result = Math.Abs(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("ACOS", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ASIN", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ATAN", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CBRT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("COS", StringComparison.OrdinalIgnoreCase)
            || name.Equals("COT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SIN", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TAN", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ACOSH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ASINH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ATANH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("COSH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SINH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TANH", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var arg = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                result = name.Equals("ACOS", StringComparison.OrdinalIgnoreCase)
                    ? Math.Acos(arg)
                    : name.Equals("ASIN", StringComparison.OrdinalIgnoreCase)
                        ? Math.Asin(arg)
                    : name.Equals("ATAN", StringComparison.OrdinalIgnoreCase)
                        ? Math.Atan(arg)
                            : name.Equals("CBRT", StringComparison.OrdinalIgnoreCase)
                                ? Cbrt(arg)
                            : name.Equals("COS", StringComparison.OrdinalIgnoreCase)
                                ? Math.Cos(arg)
                                : name.Equals("SIN", StringComparison.OrdinalIgnoreCase)
                                    ? Math.Sin(arg)
                                    : name.Equals("TAN", StringComparison.OrdinalIgnoreCase)
                                        ? Math.Tan(arg)
                                        : name.Equals("COT", StringComparison.OrdinalIgnoreCase)
                                            ? 1d / Math.Tan(arg)
                                            : name.Equals("ACOSH", StringComparison.OrdinalIgnoreCase)
                                                ? Acosh(arg)
                                                : name.Equals("ASINH", StringComparison.OrdinalIgnoreCase)
                                                    ? Asinh(arg)
                                                    : name.Equals("ATANH", StringComparison.OrdinalIgnoreCase)
                                                        ? Atanh(arg)
                                                        : name.Equals("COSH", StringComparison.OrdinalIgnoreCase)
                                                            ? Math.Cosh(arg)
                                                            : name.Equals("SINH", StringComparison.OrdinalIgnoreCase)
                                                                ? Math.Sinh(arg)
                                                                : Math.Tanh(arg);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("ATAN2", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ATN2", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ATAN2() espera 2 argumentos.");

            var y = evalArg(0);
            var x = evalArg(1);
            if (IsNullish(y) || IsNullish(x))
            {
                result = null;
                return true;
            }

            try
            {
                result = Math.Atan2(
                    Convert.ToDouble(y, CultureInfo.InvariantCulture),
                    Convert.ToDouble(x, CultureInfo.InvariantCulture));
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("CEIL", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CEILING", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                if (value is decimal dec)
                {
                    result = Math.Ceiling(dec);
                    return true;
                }

                result = Math.Ceiling(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("BIN", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                result = Convert.ToString(number, 2);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("BIT_COUNT", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                var bits = unchecked((ulong)number);
                var count = 0;
                while (bits != 0)
                {
                    count += (int)(bits & 1UL);
                    bits >>= 1;
                }

                result = count;
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("BIT_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is byte[] bytes)
            {
                result = bytes.Length * 8;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = text.Length * 8;
            return true;
        }

        if (name.Equals("ASCII", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = text.Length == 0 ? 0 : (int)text[0];
            return true;
        }

        if (name.Equals("SIGN", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                result = number == 0d ? 0 : (number > 0d ? 1 : -1);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        result = null;
        return false;
    }

    private static double Acosh(double value)
        => Math.Log(value + Math.Sqrt(value - 1d) * Math.Sqrt(value + 1d));

    private static double Asinh(double value)
        => Math.Log(value + Math.Sqrt((value * value) + 1d));

    private static double Atanh(double value)
        => 0.5d * Math.Log((1d + value) / (1d - value));

    private static double Cbrt(double value)
        => value < 0d
            ? -Math.Pow(-value, 1d / 3d)
            : Math.Pow(value, 1d / 3d);

    internal static double Log2(double value)
        => Math.Log(value, 2d);

    internal static long NextRandomInt64()
    {
        var buffer = new byte[8];
        lock (_randomLock)
            _sharedRandom.NextBytes(buffer);
        return BitConverter.ToInt64(buffer, 0);
    }

    internal static double NextRandomDouble()
    {
        lock (_randomLock)
            return _sharedRandom.NextDouble();
    }

    internal static bool TryEvalAppNameFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("APP_NAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "DbSqlLikeMem";
        return true;
    }

    internal static bool TryEvalCharIndexFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHARINDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var start = fn.Args.Count > 2 ? evalArg(2) : null;
        var startIndex = 0;

        if (!IsNullish(start))
        {
            startIndex = Convert.ToInt32(start.ToDec()) - 1;
            if (startIndex < 0)
            {
                result = 0;
                return true;
            }
        }

        if (needle.Length == 0)
        {
            result = startIndex + 1;
            return true;
        }

        var index = haystack.IndexOf(needle, startIndex, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    internal static bool TryEvalCurrentUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect)
            ? "root@localhost"
            : "dbo";
        return true;
    }

    private static int? TryResolveSqlServerSystemTypeId(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        return typeName!.Trim().ToUpperInvariant() switch
        {
            "BIGINT" => 127,
            "BIT" => 104,
            "DATE" => 40,
            "DATETIME" => 61,
            "DATETIME2" => 42,
            "DATETIMEOFFSET" => 43,
            "DECIMAL" or "NUMERIC" => 106,
            "FLOAT" => 62,
            "INT" => 56,
            "NCHAR" => 239,
            "NVARCHAR" => 231,
            "REAL" => 59,
            "SMALLINT" => 52,
            "TIME" => 41,
            "TINYINT" => 48,
            "UNIQUEIDENTIFIER" => 36,
            "VARCHAR" => 167,
            _ => null
        };
    }

    private static string? TryResolveSqlServerSystemTypeName(object? typeIdValue)
    {
        if (IsNullish(typeIdValue))
            return null;

        var typeId = Convert.ToInt32(typeIdValue, CultureInfo.InvariantCulture);
        return typeId switch
        {
            36 => "uniqueidentifier",
            40 => "date",
            41 => "time",
            42 => "datetime2",
            43 => "datetimeoffset",
            48 => "tinyint",
            52 => "smallint",
            56 => "int",
            59 => "real",
            61 => "datetime",
            62 => "float",
            104 => "bit",
            106 => "decimal",
            127 => "bigint",
            167 => "varchar",
            231 => "nvarchar",
            239 => "nchar",
            _ => null
        };
    }

    private static int? TryResolveSqlServerDatabasePrincipalId(string? principalName)
    {
        if (string.IsNullOrWhiteSpace(principalName))
            return null;

        return principalName!.Trim().ToUpperInvariant() switch
        {
            "DBO" => 1,
            "GUEST" => 2,
            "PUBLIC" => 0,
            _ => null
        };
    }

    private static object? TryResolveSqlServerTypeProperty(string? typeName, string? propertyName)
    {
        if (string.IsNullOrWhiteSpace(typeName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var normalizedType = typeName!.Trim().ToUpperInvariant();
        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "OWNERID" => TryResolveSqlServerSystemTypeId(normalizedType) is null ? null : 1,
            "PRECISION" => normalizedType switch
            {
                "BIGINT" => 19,
                "BIT" => 1,
                "DATE" => 10,
                "DATETIME" => 23,
                "DATETIME2" => 27,
                "DATETIMEOFFSET" => 34,
                "DECIMAL" or "NUMERIC" => 38,
                "FLOAT" => 53,
                "INT" => 10,
                "REAL" => 24,
                "SMALLINT" => 5,
                "TIME" => 16,
                "TINYINT" => 3,
                _ => null
            },
            _ => null
        };
    }

    private object? TryResolveSqlServerDatabaseProperty(string? databaseName, string? propertyName)
    {
        if (string.IsNullOrWhiteSpace(databaseName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var normalizedDatabase = databaseName!.Trim().Trim('[', ']').NormalizeName();
        if (!string.Equals(normalizedDatabase, _cnn.Database.NormalizeName(), StringComparison.OrdinalIgnoreCase))
            return null;

        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "STATUS" => "ONLINE",
            "UPDATEABILITY" => "READ_WRITE",
            "VERSION" => _cnn.Db.Version,
            _ => null
        };
    }

    private object? TryResolveSqlServerColumnProperty(
        object? objectIdValue,
        string? columnName,
        string? propertyName)
    {
        if (IsNullish(objectIdValue) || string.IsNullOrWhiteSpace(columnName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        ITableMock? table = objectIdValue switch
        {
            string objectName => TryResolveSqlServerTable(objectName, out var tableByName) ? tableByName : null,
            _ => TryResolveSqlServerTableByObjectId(Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture))
        };

        if (table is null)
            return null;

        var column = table.GetColumn(columnName!);
        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "ALLOWSNULL" => column.Nullable ? 1 : 0,
            "COLUMNID" => column.Index + 1,
            "ISIDENTITY" => column.Identity ? 1 : 0,
            _ => null
        };
    }

    private int? TryResolveSqlServerColumnLength(string? objectName, string? columnName)
    {
        if (!TryResolveSqlServerTable(objectName, out var table) || table is null || string.IsNullOrWhiteSpace(columnName))
            return null;

        var column = table!.GetColumn(columnName!);
        return column.DbType switch
        {
            DbType.Boolean => 1,
            DbType.Byte or DbType.SByte => 1,
            DbType.Int16 or DbType.UInt16 => 2,
            DbType.Int32 or DbType.UInt32 => 4,
            DbType.Int64 or DbType.UInt64 => 8,
            DbType.Single => 4,
            DbType.Double => 8,
            DbType.Decimal or DbType.Currency or DbType.VarNumeric => 17,
            DbType.Guid => 16,
            DbType.Date => 3,
            DbType.Time => 5,
            DbType.DateTime => 8,
            DbType.DateTime2 => 8,
            DbType.DateTimeOffset => 10,
            DbType.Binary => column.Size,
            DbType.String or DbType.StringFixedLength => column.Size,
            DbType.AnsiString or DbType.AnsiStringFixedLength => column.Size,
            _ => column.Size
        };
    }

    private string? TryResolveSqlServerColumnName(object? objectIdValue, object? columnIdValue)
    {
        if (IsNullish(objectIdValue) || IsNullish(columnIdValue))
            return null;

        var table = TryResolveSqlServerTableByObjectId(Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture));
        if (table is null)
            return null;

        var columnId = Convert.ToInt32(columnIdValue, CultureInfo.InvariantCulture);
        if (columnId <= 0)
            return null;

        return table.Columns.Values
            .FirstOrDefault(col => col.Index == columnId - 1)
            ?.Name;
    }

    private bool TryResolveSqlServerTable(string? objectName, out ITableMock? table)
    {
        table = null;
        if (string.IsNullOrWhiteSpace(objectName))
            return false;

        var normalizedInput = objectName!.Trim().Trim('[', ']').NormalizeName();
        var objectEntry = EnumerateSqlServerObjects()
            .Where(item => item.FullName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase)
                || item.TableName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToList();

        if (objectEntry.Count != 1)
            return false;

        return _cnn.TryGetTable(objectEntry[0].TableName, out table, objectEntry[0].SchemaName);
    }

    private ITableMock? TryResolveSqlServerTableByObjectId(int objectId)
    {
        var objectEntry = EnumerateSqlServerObjects().FirstOrDefault(item => item.ObjectId == objectId);
        if (objectEntry == default)
            return null;

        return _cnn.TryGetTable(objectEntry.TableName, out var table, objectEntry.SchemaName)
            ? table
            : null;
    }

    private int? TryResolveSqlServerObjectId(string? objectName)
    {
        if (string.IsNullOrWhiteSpace(objectName))
            return null;

        var normalizedInput = objectName!.Trim().Trim('[', ']').NormalizeName();
        var matches = EnumerateSqlServerObjects()
            .Where(item => item.FullName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase)
                || item.TableName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToList();

        if (matches.Count != 1)
            return null;

        return matches[0].ObjectId;
    }

    private object? TryResolveSqlServerObjectProperty(object? objectIdValue, string? propertyName)
    {
        if (IsNullish(objectIdValue) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var entry = EnumerateSqlServerObjects().FirstOrDefault(item => item.ObjectId == objectId);
        if (entry == default)
            return null;

        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "ISTABLE" => entry.ObjectKind == SqlConst.TABLE ? 1 : 0,
            "ISPROCEDURE" => entry.ObjectKind == "PROCEDURE" ? 1 : 0,
            _ => null
        };
    }

    private string? TryResolveSqlServerObjectName(object? objectIdValue)
    {
        if (IsNullish(objectIdValue))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var match = EnumerateSqlServerObjects()
            .FirstOrDefault(item => item.ObjectId == objectId);
        return match.ObjectId == 0 ? null : match.TableName;
    }

    private string? TryResolveSqlServerObjectSchemaName(object? objectIdValue)
    {
        if (IsNullish(objectIdValue))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var match = EnumerateSqlServerObjects()
            .FirstOrDefault(item => item.ObjectId == objectId);
        return match.ObjectId == 0 ? null : match.SchemaName;
    }

    private List<(int ObjectId, string SchemaName, string TableName, string FullName, string ObjectKind)> EnumerateSqlServerObjects()
    {
        var objects = new List<(int ObjectId, string SchemaName, string TableName, string FullName, string ObjectKind)>();
        var nextId = 1;

        foreach (var schema in _cnn.Db.Values.OrderBy(static s => s.SchemaName, StringComparer.OrdinalIgnoreCase))
        {
            foreach (var table in schema.Tables.Values.OrderBy(static t => t.TableName, StringComparer.OrdinalIgnoreCase))
            {
                objects.Add((nextId++, schema.SchemaName, table.TableName, $"{schema.SchemaName}.{table.TableName}", SqlConst.TABLE));
            }

            foreach (var procedure in schema.Procedures.Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase))
            {
                objects.Add((nextId++, schema.SchemaName, procedure, $"{schema.SchemaName}.{procedure}", "PROCEDURE"));
            }
        }

        return objects;
    }

    private static int? TryResolveSqlServerRoleMembership(string? roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName))
            return null;

        return roleName!.Trim().ToUpperInvariant() switch
        {
            "DB_OWNER" => 1,
            "PUBLIC" => 1,
            "DB_DATAREADER" => 0,
            "DB_DATAWRITER" => 0,
            _ => null
        };
    }

    private static int? TryResolveSqlServerServerRoleMembership(string? roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName))
            return null;

        return roleName!.Trim().ToUpperInvariant() switch
        {
            "SYSADMIN" => 1,
            "SERVERADMIN" => 0,
            _ => null
        };
    }

    internal static bool TryEvalDataLengthFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATALENGTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte[] bytes)
        {
            result = bytes.Length;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = Encoding.Unicode.GetByteCount(text);
        return true;
    }

    private static bool TryEvalDegreesFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DEGREES", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var radians = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = radians * (180d / Math.PI);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalEomonthFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("EOMONTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 1)
            throw new InvalidOperationException("EOMONTH() espera ao menos 1 argumento.");

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var monthOffset = fn.Args.Count > 1 ? evalArg(1) : null;
        if (!IsNullish(monthOffset))
        {
            dateTime = dateTime.AddMonths(Convert.ToInt32(monthOffset!.ToDec()));
        }

        var lastDay = DateTime.DaysInMonth(dateTime.Year, dateTime.Month);
        result = new DateTime(dateTime.Year, dateTime.Month, lastDay);
        return true;
    }

    private static bool TryEvalDifferenceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DIFFERENCE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var first = evalArg(0)?.ToString() ?? string.Empty;
        var second = evalArg(1)?.ToString() ?? string.Empty;
        var soundex1 = ComputeSoundex(first);
        var soundex2 = ComputeSoundex(second);
        var score = 0;
        for (var i = 0; i < Math.Min(soundex1.Length, soundex2.Length); i++)
        {
            if (soundex1[i] == soundex2[i])
                score++;
        }

        result = score;
        return true;
    }

    internal static bool TryEvalErrorFunctions(
        FunctionCallExpr fn,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("ERROR_LINE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_MESSAGE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_NUMBER", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_PROCEDURE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_SEVERITY", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_STATE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = name.Equals("ERROR_MESSAGE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ERROR_PROCEDURE", StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : 0;
        return true;
    }

    private static bool TryEvalExpFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("EXP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = Math.Exp(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalFloorFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FLOOR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Floor(dec);
                return true;
            }

            result = Math.Floor(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    internal static bool TryEvalSqlServerFormatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("sqlazure", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FORMAT() espera valor e máscara.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var format = evalArg(1)?.ToString();
        var cultureName = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var culture = string.IsNullOrWhiteSpace(cultureName)
            ? CultureInfo.InvariantCulture
            : CultureInfo.GetCultureInfo(cultureName!);

        result = value is IFormattable formattable
            ? formattable.ToString(format, culture)
            : value!.ToString();
        return true;
    }

    internal static bool TryEvalSqlServerFormatMessageFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FORMATMESSAGE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMATMESSAGE() espera ao menos a mensagem.");

        result = FormatPrintf(
            evalArg(0)?.ToString() ?? string.Empty,
            [.. Enumerable.Range(1, Math.Max(0, fn.Args.Count - 1)).Select(evalArg)]);
        return true;
    }

    internal static bool TryEvalSqlServerCompressFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("COMPRESS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var input = value switch
        {
            byte[] bytes => bytes,
            _ => Encoding.Unicode.GetBytes(value!.ToString() ?? string.Empty)
        };

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(input, 0, input.Length);

        result = output.ToArray();
        return true;
    }

    internal static bool TryEvalSqlServerDecompressFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DECOMPRESS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is not byte[] bytes)
        {
            result = null;
            return true;
        }

        using var input = new MemoryStream(bytes);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        result = output.ToArray();
        return true;
    }

    internal static bool TryEvalSqlServerChecksumFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isChecksum = fn.Name.Equals("CHECKSUM", StringComparison.OrdinalIgnoreCase);
        var isBinaryChecksum = fn.Name.Equals("BINARY_CHECKSUM", StringComparison.OrdinalIgnoreCase);
        if (!isChecksum && !isBinaryChecksum)
        {
            result = null;
            return false;
        }

        var hash = new HashCode();
        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (value is null or DBNull)
            {
                hash.Add(0);
                continue;
            }

            if (value is byte[] bytes)
            {
                foreach (var b in bytes)
                    hash.Add(b);
                continue;
            }

            if (value is string text)
            {
                var normalized = isChecksum ? text.ToUpperInvariant() : text;
                foreach (var ch in normalized)
                    hash.Add(ch);
                continue;
            }

            hash.Add(value);
        }

        result = hash.ToHashCode();
        return true;
    }

    internal static bool TryEvalGetUtcDateFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("GETUTCDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalGetAnsiNullFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("GETANSINULL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = 1;
        return true;
    }

    internal static bool TryEvalGroupingFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("GROUPING", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("GROUPING_ID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect)
            && dialect.Version < 80)
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        result = 0;
        return true;
    }

    private static bool TryEvalHostFunctions(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!(fn.Name.Equals("HOST_ID", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("HOST_NAME", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = fn.Name.Equals("HOST_ID", StringComparison.OrdinalIgnoreCase)
            ? 1
            : "localhost";
        return true;
    }

    private bool TryEvalSessionContextFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SESSION_CONTEXT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("SESSION_CONTEXT() expects a key.");

        var key = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(key))
        {
            result = null;
            return true;
        }

        _cnn.TryGetSessionContextValue(key!, out result);
        return true;
    }

    internal static bool TryEvalIsDateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = TryCoerceDateTime(value, out _)
            ? 1
            : 0;
        return true;
    }

    internal static bool TryEvalIsJsonFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISJSON", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = 0;
            return true;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out _);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    internal static bool TryEvalIsNumericFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISNUMERIC", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = double.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out _)
            ? 1
            : 0;
        return true;
    }

    private static string ComputeSoundex(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var firstLetter = char.ToUpperInvariant(value[0]);
        var codes = new StringBuilder();

        char? lastCode = null;
        foreach (var ch in value.Skip(1))
        {
            var code = GetSoundexCode(ch);
            if (code is null)
            {
                lastCode = null;
                continue;
            }

            if (lastCode.HasValue && lastCode.Value == code.Value)
                continue;

            codes.Append(code.Value);
            lastCode = code.Value;
        }

        var soundex = new StringBuilder(4);
        soundex.Append(firstLetter);
        soundex.Append(codes);
        while (soundex.Length < 4)
            soundex.Append('0');

        if (soundex.Length > 4)
            soundex.Length = 4;

        return soundex.ToString();
    }

    private static bool TryEvalIpFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        if (name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 1 : 0;
            return true;
        }

        if (name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? 1 : 0;
            return true;
        }

        if (ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            result = 0;
            return true;
        }

        var bytes = ip.GetAddressBytes();
        if (bytes.Length != 16)
        {
            result = 0;
            return true;
        }

        if (name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase))
        {
            var isCompat = bytes.Take(12).All(static b => b == 0);
            result = isCompat ? 1 : 0;
            return true;
        }

        if (name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase))
        {
            var isMapped = bytes.Take(10).All(static b => b == 0)
                && bytes[10] == 0xFF
                && bytes[11] == 0xFF;
            result = isMapped ? 1 : 0;
            return true;
        }

        result = 0;
        return true;
    }

    private static bool TryEvalIsUuidFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("IS_UUID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = Guid.TryParse(text, out _) ? 1 : 0;
        return true;
    }

    private static bool TryEvalJsonArrayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_ARRAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var values = new object?[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
            values[i] = evalArg(i);

        result = BuildJsonArray(values);
        return true;
    }

    private static bool TryEvalJsonDepthFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_DEPTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is System.Text.Json.JsonElement element)
        {
            result = GetJsonDepth(element);
            return true;
        }

        var text = value?.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            result = null;
            return true;
        }

        try
        {
            if (!QueryJsonFunctionHelper.TryGetJsonRootElement(text!, out var root))
            {
                result = null;
                return true;
            }

            result = GetJsonDepth(root);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static int GetJsonDepth(System.Text.Json.JsonElement element)
    {
        if (element.ValueKind is System.Text.Json.JsonValueKind.Object)
        {
            var maxDepth = 0;
            foreach (var property in element.EnumerateObject())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(property.Value));

            return 1 + maxDepth;
        }

        if (element.ValueKind is System.Text.Json.JsonValueKind.Array)
        {
            var maxDepth = 0;
            foreach (var item in element.EnumerateArray())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(item));

            return 1 + maxDepth;
        }

        return 1;
    }

    internal static bool TryEvalJsonUtilityFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("JSON_VALID", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString();
            if (string.IsNullOrWhiteSpace(text))
            {
                result = 0;
                return true;
            }

            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text!, out _);
                result = 1;
            }
            catch
            {
                result = 0;
            }

            return true;
        }

        if (fn.Name.Equals("JSON_TYPE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Object => "OBJECT",
                JsonValueKind.Array => "ARRAY",
                JsonValueKind.String => "STRING",
                JsonValueKind.Number => element.TryGetInt64(out _)
                    ? "INTEGER"
                    : "DOUBLE",
                JsonValueKind.True => "BOOLEAN",
                JsonValueKind.False => "BOOLEAN",
                JsonValueKind.Null => SqlConst.NULL,
                _ => null
            };
            return true;
        }

        if (fn.Name.Equals("JSON_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 1)
            {
                var path = evalArg(1)?.ToString();
                if (!string.IsNullOrWhiteSpace(path)
                    && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
                {
                    element = pathElement;
                }
                else if (!string.IsNullOrWhiteSpace(path))
                {
                    result = null;
                    return true;
                }
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Array => element.GetArrayLength(),
                JsonValueKind.Object => element.EnumerateObject().Count(),
                _ => 1
            };
            return true;
        }

        if (fn.Name.Equals("JSON_STORAGE_SIZE", StringComparison.OrdinalIgnoreCase))
        {
            if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
            {
                result = null;
                return false;
            }

            if (dialect.Version < 80)
                throw SqlUnsupported.ForDialect(dialect, "JSON_STORAGE_SIZE");

            if (fn.Args.Count == 0)
                throw new InvalidOperationException("JSON_STORAGE_SIZE() espera um JSON.");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var raw = element.GetRawText();
            result = (long)Encoding.UTF8.GetByteCount(raw);
            return true;
        }

        if (fn.Name.Equals("JSON_OVERLAPS", StringComparison.OrdinalIgnoreCase))
        {
            if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
            {
                result = null;
                return false;
            }

            if (dialect.Version < 80)
                throw SqlUnsupported.ForDialect(dialect, "JSON_OVERLAPS");

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_OVERLAPS() espera dois JSONs.");

            var leftValue = evalArg(0);
            var rightValue = evalArg(1);
            if (IsNullish(leftValue) || IsNullish(rightValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(leftValue!, out var leftElement)
                || !TryParseJsonElement(rightValue!, out var rightElement))
            {
                result = null;
                return true;
            }

            result = JsonOverlaps(leftElement, rightElement) ? 1 : 0;
            return true;
        }

        if (fn.Name.Equals("JSON_OBJECT", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count % 2 != 0)
                throw new InvalidOperationException("JSON_OBJECT() espera um número par de argumentos.");

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < fn.Args.Count; i += 2)
            {
                var key = evalArg(i)?.ToString() ?? string.Empty;
                var val = evalArg(i + 1);
                pairs.Add((key, val));
            }

            result = BuildJsonObject(pairs);
            return true;
        }

        if (fn.Name.Equals("JSON_QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = JsonSerializer.Serialize(text);
            return true;
        }

        if (fn.Name.Equals("JSON_PRETTY", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var options = new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true
            };
            result = JsonSerializer.Serialize(element, options)
                .Replace(@"
", "\n");
            return true;
        }

        if (fn.Name.Equals("JSON_KEYS", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 1)
            {
                var path = evalArg(1)?.ToString();
                if (!string.IsNullOrWhiteSpace(path)
                    && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
                {
                    element = pathElement;
                }
                else if (!string.IsNullOrWhiteSpace(path))
                {
                    result = null;
                    return true;
                }
            }

            if (element.ValueKind != JsonValueKind.Object)
            {
                result = null;
                return true;
            }

            var keys = element.EnumerateObject().Select(static prop => (object?)prop.Name).ToArray();
            result = BuildJsonArray(keys);
            return true;
        }

        if (fn.Name.Equals("JSON_SET", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException("JSON_SET() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                if (!TrySetJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_REMOVE", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_REMOVE() espera um JSON e ao menos um path.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                TryRemoveJsonPathValue(root, tokens);
            }

            result = root.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_INSERT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            var isInsert = fn.Name.Equals("JSON_INSERT", StringComparison.OrdinalIgnoreCase);
            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                var exists = TryGetJsonNodeAtPath(root, tokens, out _);
                if (isInsert && exists)
                    continue;

                if (!isInsert && !exists)
                    continue;

                if (!TrySetJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_CONTAINS", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_CONTAINS() espera um JSON e um candidato.");

            var targetValue = evalArg(0);
            var candidateValue = evalArg(1);
            if (IsNullish(targetValue) || IsNullish(candidateValue))
            {
                result = null;
                return true;
            }

            if (targetValue is null || candidateValue is null
                || !TryParseJsonElement(targetValue, out var targetElement)
                || !TryParseJsonCandidate(candidateValue, out var candidateElement))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 2)
            {
                var path = evalArg(2)?.ToString();
                if (string.IsNullOrWhiteSpace(path)
                    || !QueryJsonFunctionHelper.TryReadJsonPathElement(targetElement, path!, out var pathElement))
                {
                    result = 0;
                    return true;
                }

                result = JsonContains(pathElement, candidateElement) ? 1 : 0;
                return true;
            }

            result = JsonContains(targetElement, candidateElement) ? 1 : 0;
            return true;
        }

        if (fn.Name.Equals("JSON_CONTAINS_PATH", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSON_CONTAINS_PATH() espera um JSON, modo e paths.");

            var json = evalArg(0);
            var mode = evalArg(1)?.ToString() ?? string.Empty;
            if (IsNullish(json))
            {
                result = null;
                return true;
            }

            if (json is null || !TryParseJsonElement(json, out var element))
            {
                result = null;
                return true;
            }

            var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
            var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
            if (!requireAll && !requireOne)
            {
                result = null;
                return true;
            }

            var anyFound = false;
            for (var i = 2; i < fn.Args.Count; i++)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path))
                {
                    if (requireAll)
                    {
                        result = 0;
                        return true;
                    }

                    continue;
                }

                var found = QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out _);
                if (found)
                    anyFound = true;

                if (requireAll && !found)
                {
                    result = 0;
                    return true;
                }

                if (requireOne && found)
                {
                    result = 1;
                    return true;
                }
            }

            result = requireAll ? 1 : (anyFound ? 1 : 0);
            return true;
        }

        if (fn.Name.Equals("JSON_SEARCH", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSON_SEARCH() espera JSON, modo e termo.");

            var json = evalArg(0);
            var mode = evalArg(1)?.ToString() ?? string.Empty;
            var search = evalArg(2)?.ToString() ?? string.Empty;
            if (IsNullish(json) || string.IsNullOrWhiteSpace(search))
            {
                result = null;
                return true;
            }

            if (json is null || !TryParseJsonElement(json, out var element))
            {
                result = null;
                return true;
            }

            var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
            var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
            if (!requireAll && !requireOne)
            {
                result = null;
                return true;
            }

            var pathStart = 3;
            if (fn.Args.Count > 4)
            {
                var escapeCandidate = evalArg(3)?.ToString();
                if (!string.IsNullOrEmpty(escapeCandidate)
                    && escapeCandidate!.Length == 1)
                    pathStart = 4;
            }

            var results = new List<string>();
            if (fn.Args.Count > pathStart)
            {
                for (var i = pathStart; i < fn.Args.Count; i++)
                {
                    var path = evalArg(i)?.ToString();
                    if (string.IsNullOrWhiteSpace(path))
                        continue;

                    if (QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var scoped))
                        CollectJsonSearchMatches(scoped, path!, search, results);
                }
            }
            else
            {
                CollectJsonSearchMatches(element, "$", search, results);
            }

            if (results.Count == 0)
            {
                result = null;
                return true;
            }

            result = requireOne ? results[0] : BuildJsonArray(results.Cast<object?>());
            return true;
        }

        result = null;
        return false;
    }

    internal static string BytesToHex(byte[] hash)
    {
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    internal static byte[] ComputeHash(System.Security.Cryptography.HashAlgorithm algorithm, byte[] bytes)
    {
        using (algorithm)
            return algorithm.ComputeHash(bytes);
    }

    internal static int GetIsoWeekOfYear(DateTime dateTime)
    {
        var day = CultureInfo.InvariantCulture.Calendar.GetDayOfWeek(dateTime);
        if (day is DayOfWeek.Monday or DayOfWeek.Tuesday or DayOfWeek.Wednesday)
            dateTime = dateTime.AddDays(3);

        return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(
            dateTime,
            CalendarWeekRule.FirstFourDayWeek,
            DayOfWeek.Monday);
    }

    internal static int GetIsoWeekYear(DateTime dateTime)
    {
        var week = GetIsoWeekOfYear(dateTime);
        var year = dateTime.Year;
        if (week == 52 && dateTime.Month == 1)
            year -= 1;
        else if (week == 1 && dateTime.Month == 12)
            year += 1;
        return year;
    }

    private static bool TryEvalReplaceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals(SqlConst.REPLACE, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var source = evalArg(0);
        var from = evalArg(1);
        var to = evalArg(2);
        if (IsNullish(source) || IsNullish(from) || IsNullish(to))
        {
            result = null;
            return true;
        }

        result = (source!.ToString() ?? string.Empty)
            .Replace(from!.ToString() ?? string.Empty, to!.ToString() ?? string.Empty);
        return true;
    }

    private object? EvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2) return null;
        var v = evalArg(0);
        var type = fn.Args[1] is RawSqlExpr trx ? trx.Sql : (evalArg(1)?.ToString() ?? "");
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
                if (decimal.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return dx;
                return null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (v is double dfx) return dfx;
                if (v is float ffx) return (double)ffx;
                if (v is decimal ddx) return (double)ddx;
                if (double.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var fx)) return fx;
                return null;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                return TryCoerceDateTime(v, out var dt) ? dt : null;
            }

            return v!.ToString();
        }
        catch
        {
            return null;
        }
    }

    private object? EvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors)
    {
        if (fn.Args.Count < 2)
            return swallowErrors ? null : throw new InvalidOperationException($"{fn.Name}() requires value and target type.");

        var value = evalArg(0);
        if (IsNullish(value))
            return null;

        var type = fn.Args[1] is RawSqlExpr rx ? rx.Sql : (evalArg(1)?.ToString() ?? string.Empty);
        type = type.Trim();
        var cultureName = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;

        try
        {
            var culture = string.IsNullOrWhiteSpace(cultureName)
                ? CultureInfo.InvariantCulture
                : CultureInfo.GetCultureInfo(cultureName!);

            if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para PARSE.")).IsIntegerCastTypeName(type))
            {
                if (int.TryParse(value!.ToString(), NumberStyles.Integer, culture, out var parsedInt))
                    return parsedInt;
                return null;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (decimal.TryParse(value!.ToString(), NumberStyles.Any, culture, out var parsedDecimal))
                    return parsedDecimal;
                return null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (double.TryParse(value!.ToString(), NumberStyles.Any, culture, out var parsedDouble))
                    return parsedDouble;
                return null;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase))
            {
                if (TryParseCachedDateTime(value!.ToString()!, culture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate))
                    return parsedDate;
                return null;
            }

            return value!.ToString();
        }
        catch
        {
            if (swallowErrors)
                return null;
            throw;
        }
    }

    private object? EvalCast(FunctionCallExpr fn, Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2) return null;

        var v = evalArg(0);
        var type = fn.Args[1] is RawSqlExpr rx ? rx.Sql : (evalArg(1)?.ToString() ?? "");
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

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (v is double dfx) return dfx;
                if (v is float ffx) return (double)ffx;
                if (v is decimal ddx) return (double)ddx;
                if (double.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var fx)) return fx;
                return 0d;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                return TryCoerceDateTime(v, out var dt) ? dt : null;
            }

            if (type.Equals("JSON", StringComparison.OrdinalIgnoreCase))
            {
                static string? ValidateJsonOrNull(string? json)
                {
                    if (json is null || string.IsNullOrWhiteSpace(json))
                        return null;

                    var normalizedJson = json.Trim();

                    QueryJsonFunctionHelper.TryGetJsonRootElement(normalizedJson, out _);
                    return normalizedJson;
                }

                if (v is string s)
                    return ValidateJsonOrNull(s);

                if (v is System.Text.Json.JsonElement je)
                    return ValidateJsonOrNull(je.GetRawText());

                var serialized = JsonSerializer.Serialize(v);
                return ValidateJsonOrNull(serialized);
            }

            return v!.ToString();
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    private object? TryEvalJsonAndNumberFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out bool handled)
    {
        handled = true;

        if (TryEvalJsonAccessShimFunction(fn, evalArg, out var jsonAccessResult))
            return jsonAccessResult;

        if (TryEvalJsonExtractionFunction(fn, dialect, evalArg, out var jsonExtractionResult))
            return jsonExtractionResult;

        if (TryEvalSqlServerJsonModifyFunction(fn, dialect, evalArg, out var jsonModifyResult))
            return jsonModifyResult;

        if (TryEvalOpenJsonFunction(fn, dialect, evalArg, out var openJsonResult))
            return openJsonResult;

        if (TryEvalJsonUnquoteFunction(fn, evalArg, out var jsonUnquoteResult))
            return jsonUnquoteResult;

        if (TryEvalToNumberFunction(fn, evalArg, out var toNumberResult))
            return toNumberResult;

        handled = false;
        return null;
    }

    private static bool TryEvalSqlServerJsonModifyFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && !definition.AllowsCall)
        {
            throw SqlUnsupported.ForDialect(dialect, "JSON_MODIFY");
        }

        if (definition is null)
            throw SqlUnsupported.ForDialect(dialect, "JSON_MODIFY");

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("JSON_MODIFY() espera JSON, path e novo valor.");

        var json = evalArg(0);
        var pathValue = evalArg(1)?.ToString();
        var newValue = evalArg(2);
        if (IsNullish(json) || string.IsNullOrWhiteSpace(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        if (!TryParseSqlServerJsonModifyPath(pathValue!, out var tokens, out var append, out var strict))
        {
            result = null;
            return true;
        }

        var exists = TryGetJsonNodeAtPath(root, tokens, out var existingNode);
        if (append)
        {
            if (!exists || existingNode is not System.Text.Json.Nodes.JsonArray array)
            {
                if (strict)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                result = root.ToJsonString();
                return true;
            }

            array.Add(CreateJsonNodeFromValue(newValue));
            result = root.ToJsonString();
            return true;
        }

        if (IsNullish(newValue))
        {
            if (strict)
            {
                if (!exists)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                if (!TrySetJsonPathValue(ref root, tokens, null))
                {
                    result = null;
                    return true;
                }
            }
            else if (exists)
            {
                TryRemoveJsonPathValue(root, tokens);
            }

            result = root.ToJsonString();
            return true;
        }

        if (strict && !exists)
            throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

        if (!TrySetJsonPathValue(ref root, tokens, newValue))
        {
            if (strict)
                throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

            result = root.ToJsonString();
            return true;
        }

        result = root.ToJsonString();
        return true;
    }

    private static bool TryEvalJsonAccessShimFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("__JSON_ACCESS_JSON", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!TryGetJsonAndPathArguments(evalArg, out var json, out var path))
        {
            result = null;
            return true;
        }

        var value = TryReadJsonPathValue(json!, path!);
        result = fn.Name.Equals("__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)
            ? value?.ToString()
            : value;
        return true;
    }

    internal static bool TryEvalJsonExtractionFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        EnsureJsonExtractionSupported(fn.Name, dialect);
        var json = evalArg(0);
        if (IsNullish(json))
        {
            result = null;
            return true;
        }

        if (fn.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && fn.Args.Count == 1)
        {
            result = TryEvalJsonQueryWithoutPath(json!);
            return true;
        }

        var path = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(path))
        {
            result = null;
            return true;
        }

        result = TryEvalJsonExtractionValue(fn, json!, path!);
        return true;
    }

    private static void EnsureJsonExtractionSupported(string functionName, ISqlDialect dialect)
    {
        if (dialect.TryGetScalarFunctionDefinition(functionName, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw SqlUnsupported.ForDialect(dialect, functionName.ToUpperInvariant());
        }

        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_EXTRACT", out var jsonExtractDefinition)
                || jsonExtractDefinition is null
                || !jsonExtractDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_QUERY", out var jsonQueryDefinition)
                || jsonQueryDefinition is null
                || !jsonQueryDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_VALUE", out var jsonValueDefinition)
                || jsonValueDefinition is null
                || !jsonValueDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");
    }

    private static object? TryEvalJsonExtractionValue(FunctionCallExpr fn, object json, string path)
    {
        try
        {
            if (fn.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json, path, out var element))
                    return null;

                return element.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? element.GetRawText()
                    : null;
            }

            var value = TryReadJsonPathValue(json, path);
            return fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                ? ApplyJsonValueReturningClause(fn, value)
                : value;
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    private static object? TryEvalJsonQueryWithoutPath(object json)
    {
        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
            return null;

        return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }

    private static bool TryEvalOpenJsonFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
            || openJsonDefinition is null)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.OPENJSON);

        var json = evalArg(0);
        result = IsNullish(json) ? null : json?.ToString();
        return true;
    }

    private static bool TryEvalJsonUnquoteFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_UNQUOTE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value!.ToString() ?? string.Empty;
        result = text.Length >= 2 && ((text[0] == '"' && text[^1] == '"') || (text[0] == '\'' && text[^1] == '\''))
            ? text[1..^1]
            : text;
        return true;
    }

    internal static bool TryEvalToNumberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TO_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            result = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (value is decimal or double or float)
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }

        var text = value?.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            result = null;
            return true;
        }

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integerValue))
        {
            result = integerValue;
            return true;
        }

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
        {
            result = decimalValue;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryGetJsonAndPathArguments(
        Func<int, object?> evalArg,
        out object? json,
        out string? path)
    {
        json = evalArg(0);
        path = evalArg(1)?.ToString();
        return !IsNullish(json) && !string.IsNullOrWhiteSpace(path);
    }

    internal static void LogFunctionEvaluationFailure(Exception exception)
    {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine($"{nameof(AstQueryExecutorBase)}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        Console.WriteLine(exception);
    }

    private static object? ApplyJsonValueReturningClause(FunctionCallExpr fn, object? value)
        => QueryJsonFunctionHelper.ApplyJsonValueReturningClause(fn, value);

    private object? TryEvalConcatFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out bool handled)
        => QueryConcatFunctionHelper.TryEvalConcatFunctions(
            fn,
            evalArg,
            Dialect!.ConcatReturnsNullOnNullInput,
            out handled);

    private static object? TryReadJsonPathValue(object json, string path)
        => QueryJsonFunctionHelper.TryReadJsonPathValue(json, path);

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

    private TemporalUnit GetTemporalUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => ResolveTemporalUnit(GetDateAddUnit(expr, row, group, ctes));

    internal static TemporalUnit ResolveTemporalUnit(string unit)
        => _temporalUnits.TryGetValue(unit, out var resolved)
            ? resolved
            : TemporalUnit.Unknown;

    internal static bool TryCoerceDecimal(object? value, out decimal result)
    {
        result = default;

        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    internal static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
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

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && TryParseCachedDateTime(text, DateTimeStyles.AssumeLocal, out dt);
    }

    internal static bool TryCoerceTimeSpan(object? baseVal, out TimeSpan span)
    {
        span = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        if (baseVal is TimeSpan ts)
        {
            span = ts;
            return true;
        }

        if (baseVal is DateTime dt)
        {
            span = dt.TimeOfDay;
            return true;
        }

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && TryParseCachedTimeSpan(text, out span);
    }

    internal static bool TryParseCachedDateTime(string text, DateTimeStyles styles, out DateTime dt)
    {
        var cacheKey = BuildDateTimeParseCacheKey(text, styles);
        if (_dateTimeParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParse(
            text,
            CultureInfo.InvariantCulture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseCachedDateTime(string text, CultureInfo culture, DateTimeStyles styles, out DateTime dt)
    {
        if (string.IsNullOrEmpty(culture.Name))
            return TryParseCachedDateTime(text, styles, out dt);

        var cacheKey = BuildDateTimeParseCacheKey(text, culture.Name, styles);
        if (_dateTimeParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParse(
            text,
            culture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseExactCachedDateTime(string text, string format, DateTimeStyles styles, out DateTime dt)
    {
        var cacheKey = BuildExactDateTimeParseCacheKey(text, format, styles);
        if (_dateTimeExactParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParseExact(
            text,
            format,
            CultureInfo.InvariantCulture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeExactParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseCachedDateTimeOffset(string text, DateTimeStyles styles, out DateTimeOffset dto)
    {
        var cacheKey = BuildDateTimeOffsetParseCacheKey(text, styles);
        if (_dateTimeOffsetParseCache.TryGetValue(cacheKey, out var cached))
        {
            dto = cached.Value;
            return cached.Success;
        }

        var success = DateTimeOffset.TryParse(
            text,
            CultureInfo.InvariantCulture,
            styles,
            out dto);

        CacheTemporalParseEntry(_dateTimeOffsetParseCache, cacheKey, new DateTimeOffsetParseCacheEntry(success, dto));
        return success;
    }

    internal static bool TryParseExactCachedDateTimeOffset(string text, string format, DateTimeStyles styles, out DateTimeOffset dto)
    {
        var cacheKey = BuildExactDateTimeOffsetParseCacheKey(text, format, styles);
        if (_dateTimeOffsetExactParseCache.TryGetValue(cacheKey, out var cached))
        {
            dto = cached.Value;
            return cached.Success;
        }

        var success = DateTimeOffset.TryParseExact(
            text,
            format,
            CultureInfo.InvariantCulture,
            styles,
            out dto);

        CacheTemporalParseEntry(_dateTimeOffsetExactParseCache, cacheKey, new DateTimeOffsetParseCacheEntry(success, dto));
        return success;
    }

    private static bool TryParseCachedTimeSpan(string text, out TimeSpan span)
    {
        if (_timeSpanParseCache.TryGetValue(text, out var cached))
        {
            span = cached.Value;
            return cached.Success;
        }

        var success = false;
        span = default;

        if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed))
        {
            span = parsed;
            success = true;
        }
        else if (TryParseCachedDateTime(text, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            span = parsedDate.TimeOfDay;
            success = true;
        }

        CacheTemporalParseEntry(_timeSpanParseCache, text, new TimeSpanParseCacheEntry(success, span));
        return success;
    }

    private static void CacheTemporalParseEntry<TEntry>(
        System.Collections.Concurrent.ConcurrentDictionary<string, TEntry> cache,
        string text,
        TEntry entry)
    {
        if (cache.Count >= TemporalParseCacheSoftLimit)
            cache.Clear();

        cache[text] = entry;
    }

    private static string BuildDateTimeParseCacheKey(string text, DateTimeStyles styles)
        => $"{(int)styles}:{text}";

    private static string BuildDateTimeParseCacheKey(string text, string cultureName, DateTimeStyles styles)
        => $"{cultureName}:{(int)styles}:{text}";

    private static string BuildDateTimeOffsetParseCacheKey(string text, DateTimeStyles styles)
        => $"{(int)styles}:{text}";

    private static string BuildExactDateTimeParseCacheKey(string text, string format, DateTimeStyles styles)
        => $"{(int)styles}:{format}:{text}";

    private static string BuildExactDateTimeOffsetParseCacheKey(string text, string format, DateTimeStyles styles)
        => $"{(int)styles}:{format}:{text}";

    internal static bool LooksLikeTimeOnly(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return false;

        if (trimmed.Contains('T') || trimmed.Contains('t'))
            return false;

        if (trimmed.Contains('-') || trimmed.Contains('/'))
            return false;

        return trimmed.Contains(':');
    }

    internal static DateTime ApplyDateDelta(DateTime dt, TemporalUnit unit, int amount) => unit switch
    {
        TemporalUnit.Year => dt.AddYears(amount),
        TemporalUnit.Month => dt.AddMonths(amount),
        TemporalUnit.Day => dt.AddDays(amount),
        TemporalUnit.Hour => dt.AddHours(amount),
        TemporalUnit.Minute => dt.AddMinutes(amount),
        TemporalUnit.Second => dt.AddSeconds(amount),
        _ => dt
    };

    internal static DateTime TruncateDateTime(DateTime dateTime, TemporalUnit unit) => unit switch
    {
        TemporalUnit.Year => new DateTime(dateTime.Year, 1, 1),
        TemporalUnit.Month => new DateTime(dateTime.Year, dateTime.Month, 1),
        TemporalUnit.Day => dateTime.Date,
        TemporalUnit.Hour => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0),
        TemporalUnit.Minute => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0),
        TemporalUnit.Second => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second),
        _ => dateTime
    };

    internal static int? GetTemporalPartValue(DateTime dateTime, TemporalUnit unit) => unit switch
    {
        TemporalUnit.Year => dateTime.Year,
        TemporalUnit.Month => dateTime.Month,
        TemporalUnit.Day => dateTime.Day,
        TemporalUnit.Hour => dateTime.Hour,
        TemporalUnit.Minute => dateTime.Minute,
        TemporalUnit.Second => dateTime.Second,
        _ => null
    };

    internal static bool TryParseDateModifier(string modifier, out TemporalUnit unit, out int amount)
    {
        unit = TemporalUnit.Unknown;
        amount = 0;

        var match = _dateModifierRegex.Match(modifier.Trim());
        if (!match.Success)
            return false;

        if (!int.TryParse(match.Groups["amount"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        unit = ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    private object? EvalCall(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Aggregate?
        if (group is not null && AggregateFunctionCatalog.Contains(fn.Name))
            return EvalAggregate(fn, group, ctes);

        if (fn.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return ParseIntervalValue(fn, row, group, ctes);

        // se não for agregado, trata como função "normal" reaproveitando EvalFunction
        // (Distinct em função escalar não faz sentido aqui, então ignoramos)
        var shim = fn.ResolvedScalarFunction is not null
            ? new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(fn.ResolvedScalarFunction)
            : new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(
                Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para função escalar."));
        return EvalFunction(shim, row, group, ctes);
    }

    internal static bool IsNullish(object? v) => v is null || v is DBNull;

    private static bool IsRowCountHelperSelect(SqlSelectQuery q)
    {
        if (q.SelectItems.Count != 1)
            return false;

        if (q.Table is not null
            && !string.Equals(q.Table.Name, "DUAL", StringComparison.OrdinalIgnoreCase))
            return false;

        var raw = q.SelectItems[0].Raw.Trim();
        return raw.Equals("CHANGES()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROW_COUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROWCOUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);
    }

    private IntervalValue? ParseIntervalValue(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        if (TryParseSplitIntervalArguments(fn, row, group, ctes, out var splitValue, out var splitUnit))
        {
            var splitSpan = TryConvertIntervalToTimeSpan(splitValue, splitUnit);
            return splitSpan is null ? null : new IntervalValue(splitSpan.Value);
        }

        var raw = Eval(fn.Args[0], row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (!TryParseIntervalLiteral(raw!, out var value, out var unit))
            return null;

        var span = TryConvertIntervalToTimeSpan(value, unit);
        return span is null ? null : new IntervalValue(span.Value);
    }

    private bool TryParseSplitIntervalArguments(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out decimal value,
        out TemporalUnit unit)
    {
        value = 0m;
        unit = TemporalUnit.Unknown;

        if (fn.Args.Count < 2)
            return false;

        unit = GetTemporalUnit(fn.Args[1], row, group, ctes);
        if (unit == TemporalUnit.Unknown)
            return false;

        var rawValue = Eval(fn.Args[0], row, group, ctes);
        if (rawValue is null || rawValue is DBNull)
            return false;

        if (rawValue is decimal dec)
        {
            value = dec;
            return true;
        }

        if (!decimal.TryParse(rawValue.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out value))
            return false;

        return true;
    }

    private static bool TryParseIntervalLiteral(string raw, out decimal value, out TemporalUnit unit)
    {
        value = 0;
        unit = TemporalUnit.Unknown;

        var normalized = raw.Trim();
        if (normalized.Contains('\\'))
            normalized = normalized.Replace("\\", string.Empty);

        var match = _intervalLiteralRegex.Match(normalized);
        if (!match.Success)
            return false;

        if (!decimal.TryParse(match.Groups["num"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out value))
            return false;

        unit = ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    private static TimeSpan? TryConvertIntervalToTimeSpan(decimal value, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Day => TimeSpan.FromDays((double)value),
            TemporalUnit.Hour => TimeSpan.FromHours((double)value),
            TemporalUnit.Minute => TimeSpan.FromMinutes((double)value),
            TemporalUnit.Second => TimeSpan.FromSeconds((double)value),
            _ => (TimeSpan?)null
        };

    private object? EvalAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var name = fn.Name.ToUpperInvariant();

        if (TryEvalAggregateCount(fn, group, ctes, name, out var countValue))
            return countValue;

        if (name is "JSON_GROUP_OBJECT" or "JSON_OBJECTAGG")
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            if (MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect)
                && dialect.Version < 56
                && name == "JSON_OBJECTAGG")
            {
                throw SqlUnsupported.ForDialect(dialect, name);
            }

            return EvalJsonGroupObjectAggregate(fn, group, ctes);
        }

        if (name is "JSON_OBJECT_AGG" or "JSON_OBJECT_AGG_STRICT" or "JSON_OBJECT_AGG_UNIQUE" or "JSON_OBJECT_AGG_UNIQUE_STRICT"
            or "JSONB_OBJECT_AGG" or "JSONB_OBJECT_AGG_STRICT" or "JSONB_OBJECT_AGG_UNIQUE" or "JSONB_OBJECT_AGG_UNIQUE_STRICT")
            return EvalJsonGroupObjectAggregate(fn, group, ctes);

        if (name is "CORR" or "CORR_K" or "CORR_S" or "COVAR_POP" or "COVAR_SAMP" or "COVARIANCE" or "COVARIANCE_SAMP" or "CORRELATION")
        {
            var normalized = name switch
            {
                "COVARIANCE" => "COVAR_POP",
                "COVARIANCE_SAMP" => "COVAR_SAMP",
                "CORRELATION" => "CORR",
                _ => name
            };
            return EvalCorrelationAggregate(fn, group, ctes, normalized);
        }

        if (name is "GROUP_ID")
            return 0;

        if (name.StartsWith("APPROX_", StringComparison.OrdinalIgnoreCase))
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            var definition = fn.ResolvedScalarFunction;
            if (definition is null || !definition.AllowsCall)
            {
                throw SqlUnsupported.ForDialect(dialect, name);
            }

            return EvalApproxAggregate(fn, group, ctes, name);
        }

        if (name.StartsWith("REGR_", StringComparison.OrdinalIgnoreCase))
            return EvalRegressionAggregate(fn, group, ctes, name);

        if (name.StartsWith("STATS_", StringComparison.OrdinalIgnoreCase))
            return null;

        if (name is "STD" or "STDDEV" or "STDDEV_POP" or "STDDEV_SAMP")
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            var normalizedName = name == "STD" ? "STDDEV_POP" : name;
            return EvalStdDevAggregate(fn, group, ctes, normalizedName);
        }

        if (name is "RATIO_TO_REPORT")
            return null;

        if (name is "MEDIAN" or "PERCENTILE" or "PERCENTILE_CONT" or "PERCENTILE_DISC")
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            if (dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
                && (name is "PERCENTILE_CONT" or "PERCENTILE_DISC")
                && dialect.Version < 2012)
            {
                throw SqlUnsupported.ForDialect(dialect, name);
            }

            return EvalPercentileAggregate(fn, group, ctes, name);
        }

        if (name is "CHECKSUM_AGG")
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            if (!(fn.ResolvedScalarFunction?.AllowsCall
                ?? (dialect.TryGetScalarFunctionDefinition(fn, out var checksumDefinition)
                    && checksumDefinition is not null
                    && checksumDefinition.AllowsCall)))
                throw SqlUnsupported.ForDialect(dialect, name);
        }

        if (name is "GROUP_CONCAT" or "STRING_AGG" or "LISTAGG")
        {
            var separator = GetAggregateSeparator(fn, group, ctes);
            return EvalSimpleStringAggregate(fn, group, ctes, separator, GetStringAggregateDefaultSeparator(name));
        }

        var values = TryGetAggregateValues(fn, group, ctes);
        if (values is null)
            return null;

        if (values.Count == 0)
        {
            // MySQL: SUM/AVG/MIN/MAX sobre conjunto vazio (ou tudo NULL) => NULL
            return name == "TOTAL" ? 0d : null;
        }

        return EvalCollectedAggregateValues(fn, group, ctes, name, values);
    }

    private object? EvalCollectedAggregateValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        IReadOnlyList<object?> values)
    {
        var separator = GetAggregateSeparator(fn, group, ctes);
        return name switch
        {
            "SUM" => AggregateNumericValues(values, AggregateNumericOperation.Sum),
            "AVG" => AggregateNumericValues(values, AggregateNumericOperation.Average),
            "MIN" => AggregateNumericValues(values, AggregateNumericOperation.Min),
            "MAX" => AggregateNumericValues(values, AggregateNumericOperation.Max),
            "CHECKSUM_AGG" => AggregateChecksumValues(values, binary: false),
            "GROUP_CONCAT" => EvalStringAggregate(values, separator, ","),
            "STRING_AGG" => EvalStringAggregate(values, separator, ","),
            "LISTAGG" => EvalStringAggregate(values, separator, string.Empty),
            "ANY_VALUE" => AggregateAnyValue(values),
            "BIT_AND" => AggregateBitwiseValues(values, BitwiseAggregateOperation.And),
            "BIT_OR" => AggregateBitwiseValues(values, BitwiseAggregateOperation.Or),
            "BIT_XOR" => AggregateBitwiseValues(values, BitwiseAggregateOperation.Xor),
            "JSON_ARRAYAGG" => EvalJsonArrayAggregate(values),
            "JSON_AGG" => EvalJsonArrayAggregate(values),
            "JSONB_AGG" => EvalJsonArrayAggregate(values),
            "ARRAY_AGG" => AggregateCollect(values),
            "BOOL_AND" => AggregateBoolValues(values, useAnd: true),
            "EVERY" => AggregateBoolValues(values, useAnd: true),
            "BOOL_OR" => AggregateBoolValues(values, useAnd: false),
            "COLLECT" => AggregateCollect(values),
            "TOTAL" => AggregateTotal(values),
            "STDEV" => AggregateVariance(values, sample: true) is double stdev ? Math.Sqrt(stdev) : null,
            "STDEVP" => AggregateVariance(values, sample: false) is double stdevp ? Math.Sqrt(stdevp) : null,
            "VAR" => AggregateVariance(values, sample: true),
            "VARP" => AggregateVariance(values, sample: false),
            "VAR_POP" => AggregateVariance(values, sample: false),
            "VARIANCE" => AggregateVariance(values, sample: false),
            "VARIANCE_SAMP" => AggregateVariance(values, sample: true),
            "VAR_SAMP" => AggregateVariance(values, sample: true),
            "CV" => AggregateCoefficientOfVariation(values),
            _ => null
        };
    }

    private static int? AggregateChecksumValues(IReadOnlyList<object?> values, bool binary)
    {
        var hash = new HashCode();
        var hasValue = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            hasValue = true;
            if (value is byte[] bytes)
            {
                foreach (var b in bytes)
                    hash.Add(b);
                continue;
            }

            if (value is string text)
            {
                var normalized = binary ? text : text.ToUpperInvariant();
                foreach (var ch in normalized)
                    hash.Add(ch);
                continue;
            }

            hash.Add(value);
        }

        if (!hasValue)
            return null;

        return hash.ToHashCode();
    }

    private static object? AggregateTotal(IReadOnlyList<object?> values)
    {
        var total = 0d;
        var hasValue = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            total += Convert.ToDouble(value, CultureInfo.InvariantCulture);
            hasValue = true;
        }

        return hasValue ? total : 0d;
    }

    private object? EvalJsonGroupObjectAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count < 2)
            return null;

        var obj = new System.Text.Json.Nodes.JsonObject();
        foreach (var row in group.Rows)
        {
            var keyValue = Eval(fn.Args[0], row, null, ctes);
            if (IsNullish(keyValue))
                continue;

            var key = keyValue?.ToString() ?? string.Empty;
            var value = Eval(fn.Args[1], row, null, ctes);
            obj[key] = CreateJsonNodeFromValue(value);
        }

        return obj.ToJsonString();
    }

    private object? EvalPercentileAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<double>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var value = Eval(fn.Args[0], row, null, ctes);
            if (IsNullish(value))
                continue;

            if (TryConvertNumericToDouble(value, out var numeric))
                values.Add(numeric);
        }

        if (values.Count == 0)
            return null;

        values.Sort();

        var percentile = 0.5d;
        if (fn.Args.Count > 1)
        {
            var percentileValue = Eval(fn.Args[1], group.Rows[0], null, ctes);
            if (IsNullish(percentileValue) || !TryConvertNumericToDouble(percentileValue, out percentile))
                return null;
        }

        if (percentile < 0d)
            percentile = 0d;
        else if (percentile > 1d)
            percentile = 1d;
        var isDiscrete = name.Equals("PERCENTILE_DISC", StringComparison.OrdinalIgnoreCase);
        if (name.Equals("MEDIAN", StringComparison.OrdinalIgnoreCase))
            percentile = 0.5d;

        if (isDiscrete)
        {
            var index = (int)Math.Ceiling(percentile * values.Count) - 1;
            if (index < 0)
                index = 0;
            if (index >= values.Count)
                index = values.Count - 1;
            return values[index];
        }

        var rank = percentile * (values.Count - 1);
        var lowerIndex = (int)Math.Floor(rank);
        var upperIndex = (int)Math.Ceiling(rank);
        if (lowerIndex == upperIndex)
            return values[lowerIndex];

        var fraction = rank - lowerIndex;
        return values[lowerIndex] + (values[upperIndex] - values[lowerIndex]) * fraction;
    }

    private object? EvalCorrelationAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = Eval(fn.Args[0], row, null, ctes);
            var yValue = Eval(fn.Args[1], row, null, ctes);
            if (IsNullish(xValue) || IsNullish(yValue))
                continue;

            try
            {
                var x = Convert.ToDouble(xValue, CultureInfo.InvariantCulture);
                var y = Convert.ToDouble(yValue, CultureInfo.InvariantCulture);
                pairs.Add((x, y));
            }
            catch
            {
                return null;
            }
        }

        if (pairs.Count == 0)
            return null;

        var meanX = pairs.Average(p => p.X);
        var meanY = pairs.Average(p => p.Y);
        var sumXY = pairs.Sum(p => (p.X - meanX) * (p.Y - meanY));

        if (name is "COVAR_POP")
            return sumXY / pairs.Count;

        if (name is "COVAR_SAMP")
            return pairs.Count < 2 ? null : sumXY / (pairs.Count - 1);

        var sumXX = pairs.Sum(p =>
        {
            var dx = p.X - meanX;
            return dx * dx;
        });
        var sumYY = pairs.Sum(p =>
        {
            var dy = p.Y - meanY;
            return dy * dy;
        });

        if (sumXX == 0d || sumYY == 0d)
            return null;

        return sumXY / Math.Sqrt(sumXX * sumYY);
    }

    private object? EvalApproxAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        if (name is "APPROX_MEDIAN")
            return EvalPercentileAggregate(fn, group, ctes, "MEDIAN");

        if (name is "APPROX_PERCENTILE" or "APPROX_PERCENTILE_AGG" or "APPROX_PERCENTILE_DETAIL")
            return EvalPercentileAggregate(fn, group, ctes, "PERCENTILE_CONT");

        if (name is "APPROX_COUNT_DISTINCT" or "APPROX_COUNT_DISTINCT_AGG" or "APPROX_COUNT_DISTINCT_DETAIL")
        {
            var set = new HashSet<object?>();
            foreach (var row in group.Rows)
            {
                var value = Eval(fn.Args[0], row, null, ctes);
                if (!IsNullish(value))
                    set.Add(value);
            }
            return set.Count;
        }

        return null;
    }

    private object? EvalRegressionAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = Eval(fn.Args[0], row, null, ctes);
            var yValue = Eval(fn.Args[1], row, null, ctes);
            if (IsNullish(xValue) || IsNullish(yValue))
                continue;

            try
            {
                var x = Convert.ToDouble(xValue, CultureInfo.InvariantCulture);
                var y = Convert.ToDouble(yValue, CultureInfo.InvariantCulture);
                pairs.Add((x, y));
            }
            catch
            {
                return null;
            }
        }

        if (pairs.Count == 0)
            return null;

        var meanX = pairs.Average(p => p.X);
        var meanY = pairs.Average(p => p.Y);
        var sumXX = pairs.Sum(p =>
        {
            var dx = p.X - meanX;
            return dx * dx;
        });
        var sumYY = pairs.Sum(p =>
        {
            var dy = p.Y - meanY;
            return dy * dy;
        });
        var sumXY = pairs.Sum(p => (p.X - meanX) * (p.Y - meanY));

        return name switch
        {
            "REGR_COUNT" => pairs.Count,
            "REGR_AVGX" => meanX,
            "REGR_AVGY" => meanY,
            "REGR_SXX" => sumXX,
            "REGR_SYY" => sumYY,
            "REGR_SXY" => sumXY,
            "REGR_SLOPE" => sumXX == 0 ? null : sumXY / sumXX,
            "REGR_INTERCEPT" => sumXX == 0 ? null : meanY - (sumXY / sumXX) * meanX,
            "REGR_ICPT" => sumXX == 0 ? null : meanY - (sumXY / sumXX) * meanX,
            "REGR_R2" => (sumXX == 0 || sumYY == 0) ? null : (sumXY * sumXY) / (sumXX * sumYY),
            _ => null
        };
    }

    private object? EvalStdDevAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        var values = TryGetAggregateValues(fn, group, ctes);
        if (values is null)
            return null;

        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            if (IsNullish(values[i]))
                continue;

            count++;
            var x = Convert.ToDouble(values[i], CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        var denominator = name == "STDDEV_SAMP" ? count - 1 : count;
        if (denominator <= 0)
            return null;

        var variance = m2 / denominator;
        return Math.Sqrt(variance);
    }

    private static object? AggregateAnyValue(IReadOnlyList<object?> values)
    {
        foreach (var value in values)
        {
            if (!IsNullish(value))
                return value;
        }

        return null;
    }

    private enum BitwiseAggregateOperation
    {
        And,
        Or,
        Xor
    }

    private static object? AggregateBitwiseValues(IReadOnlyList<object?> values, BitwiseAggregateOperation operation)
    {
        var hasValue = false;
        var acc = 0L;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            var next = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            if (!hasValue)
            {
                acc = next;
                hasValue = true;
                continue;
            }

            acc = operation switch
            {
                BitwiseAggregateOperation.And => acc & next,
                BitwiseAggregateOperation.Or => acc | next,
                BitwiseAggregateOperation.Xor => acc ^ next,
                _ => acc
            };
        }

        return hasValue ? acc : null;
    }

    private static object? EvalJsonArrayAggregate(IReadOnlyList<object?> values)
    {
        if (values.Count == 0)
            return null;

        return BuildJsonArray(values);
    }

    private static object? AggregateCollect(IReadOnlyList<object?> values)
    {
        if (values.Count == 0)
            return null;

        var filtered = new List<object?>(values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!IsNullish(value))
                filtered.Add(value);
        }

        return filtered.Count == 0 ? null : filtered.ToArray();
    }

    private static object? AggregateVariance(IReadOnlyList<object?> values, bool sample)
    {
        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            count++;
            var x = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        if (sample && count < 2)
            return null;

        var divisor = sample ? count - 1 : count;
        return m2 / divisor;
    }

    private static object? AggregateCoefficientOfVariation(IReadOnlyList<object?> values)
    {
        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            count++;
            var x = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        if (mean == 0d)
            return null;

        var variance = m2 / count;

        var stdDev = Math.Sqrt(variance);
        return stdDev / mean;
    }

    private static object? AggregateBoolValues(IReadOnlyList<object?> values, bool useAnd)
    {
        var hasValue = false;
        var acc = useAnd;

        foreach (var value in values)
        {
            if (IsNullish(value))
                continue;

            hasValue = true;
            var current = value!.ToBool();
            acc = useAnd ? acc && current : acc || current;
        }

        return hasValue ? acc : null;
    }

    private static object? AggregateNumericValues(IReadOnlyList<object?> values, AggregateNumericOperation operation)
    {
        if (values.Count == 0)
            return null;

        var useDouble = false;
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] is float or double)
            {
                useDouble = true;
                break;
            }
        }

        if (useDouble)
        {
            var numericValues = new double[values.Count];
            for (var i = 0; i < values.Count; i++)
                numericValues[i] = Convert.ToDouble(values[i], CultureInfo.InvariantCulture);

            double sum = 0d;
            double min = numericValues[0];
            double max = numericValues[0];
            for (var i = 0; i < numericValues.Length; i++)
            {
                var current = numericValues[i];
                sum += current;
                if (current < min)
                    min = current;
                if (current > max)
                    max = current;
            }

            return operation switch
            {
                AggregateNumericOperation.Sum => sum,
                AggregateNumericOperation.Average => sum / numericValues.Length,
                AggregateNumericOperation.Min => min,
                AggregateNumericOperation.Max => max,
                _ => null
            };
        }

        var decimalValues = new decimal[values.Count];
        for (var i = 0; i < values.Count; i++)
            decimalValues[i] = values[i]!.ToDec();

        decimal decimalSum = 0m;
        decimal decimalMin = decimalValues[0];
        decimal decimalMax = decimalValues[0];
        for (var i = 0; i < decimalValues.Length; i++)
        {
            var current = decimalValues[i];
            decimalSum += current;
            if (current < decimalMin)
                decimalMin = current;
            if (current > decimalMax)
                decimalMax = current;
        }

        return operation switch
        {
            AggregateNumericOperation.Sum => decimalSum,
            AggregateNumericOperation.Average => decimalSum / decimalValues.Length,
            AggregateNumericOperation.Min => decimalMin,
            AggregateNumericOperation.Max => decimalMax,
            _ => null
        };
    }

    private enum AggregateNumericOperation
    {
        Sum,
        Average,
        Min,
        Max
    }

    private object? GetAggregateSeparator(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => fn.Args.Count > 1 && group.Rows.Count > 0
            ? Eval(fn.Args[1], group.Rows[0], null, ctes)
            : null;

    private object? EvalAggregate(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
        var name = fn.Name.ToUpperInvariant();
        var filteredGroup = ApplyAggregateFilter(fn, group, ctes);
        var useOrdinalTextComparison = dialect.TextComparison == StringComparison.Ordinal;

        // COUNT(DISTINCT ...)
        if (name == "COUNT" && fn.Distinct)
            return EvalCountDistinct(fn, filteredGroup, ctes, useOrdinalTextComparison);

        if (name is "GROUP_CONCAT" or "STRING_AGG" or "LISTAGG")
        {
            var definition = fn.ResolvedScalarFunction;
            if (definition is not null
                && !definition.AllowsCall)
                throw SqlUnsupported.ForDialect(dialect, name);

            if (definition is null)
                throw SqlUnsupported.ForDialect(dialect, name);

            return EvalStringAggregateForCallExpr(fn, filteredGroup, ctes, name);
        }

        // para os outros casos (sem DISTINCT), reaproveita o existente
        var shim = fn.ResolvedScalarFunction is not null
            ? new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(fn.ResolvedScalarFunction)
            : new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(
                Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação."));
        return EvalAggregate(shim, filteredGroup, ctes);
    }

    private EvalGroup ApplyAggregateFilter(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Filter is null)
            return group;

        if (group.Rows.Count == 0)
            return new EvalGroup([]);

        var filteredRows = new List<EvalRow>(group.Rows.Count);
        for (var i = 0; i < group.Rows.Count; i++)
        {
            var row = group.Rows[i];
            if (Eval(fn.Filter, row, null, ctes).ToBool())
                filteredRows.Add(row);
        }

        return new EvalGroup(filteredRows);
    }

    private long EvalCountDistinct(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        bool useOrdinalTextComparison)
    {
        // COUNT(DISTINCT *) não faz sentido no MySQL; se acontecer, trata como COUNT(*)
        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
            return group.Rows.Count;

        var set = CreateDistinctStringSet(useOrdinalTextComparison, group.Rows.Count);
        foreach (var row in group.Rows)
        {
            if (TryBuildCountDistinctKey(fn, row, ctes, useOrdinalTextComparison, out var key))
                set.Add(key);
        }

        return set.Count;
    }

    private bool TryBuildCountDistinctKey(
        CallExpr fn,
        EvalRow row,
        IDictionary<string, Source> ctes,
        bool useOrdinalTextComparison,
        out string key)
    {
        key = string.Empty;

        if (fn.Args.Count == 1)
        {
            var singleValue = Eval(fn.Args[0], row, null, ctes);
            if (!TryGetStringAggregateKeyAndText(singleValue, useOrdinalTextComparison, out _, out var singleKey))
                return false;

            key = singleKey;
            return true;
        }

        var builder = new StringBuilder();

        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = Eval(fn.Args[i], row, null, ctes);
            if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out _, out var normalized))
                return false;

            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(normalized);
        }

        key = builder.ToString();
        return true;
    }

    private static HashSet<string> CreateDistinctStringSet(bool useOrdinalTextComparison, int estimatedCount)
    {
        var comparer = useOrdinalTextComparison ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;
        return new HashSet<string>(comparer);
    }

    private static string NormalizeDistinctKey(object? v, ISqlDialect? dialect = null)
        => QueryRowValueHelper.NormalizeDistinctKey(v, dialect);

    private static bool TryGetStringAggregateText(object? value, out string text)
    {
        text = string.Empty;

        if (IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                return true;
        }
    }

    private static bool TryGetStringAggregateKeyAndText(
        object? value,
        bool useOrdinalTextComparison,
        out string text,
        out string distinctKey)
    {
        text = string.Empty;
        distinctKey = string.Empty;

        if (IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                distinctKey = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                distinctKey = text;
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                distinctKey = text;
                return true;
        }
    }

    private static string? EvalStringAggregate(IReadOnlyList<object?> values, object? separatorObj, string defaultSeparator)
    {
        if (values.Count == 0)
            return null;

        var separator = separatorObj?.ToString() ?? defaultSeparator;
        var builder = new StringBuilder(EstimateStringAggregateCapacity(values.Count, separator.Length));
        for (var i = 0; i < values.Count; i++)
        {
            if (i > 0)
                builder.Append(separator);

            builder.Append(values[i]?.ToString() ?? string.Empty);
        }

        return builder.ToString();
    }

    private string? EvalStringAggregateForCallExpr(CallExpr fn, EvalGroup group, IDictionary<string, Source> ctes, string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var hasDirectValueSelector = TryCreateStringAggregateValueSelector(fn.Args[0], out var valueSelector);
        var separator = GetAggregateSeparator(fn, group, ctes);
        var rows = GetStringAggregateRows(fn, group, ctes);
        var useDirectValueSelector = hasDirectValueSelector && !(name == "LISTAGG" && separator is null);
        if (rows.Count == 1)
        {
            var singleValue = useDirectValueSelector
                ? valueSelector!(rows[0])
                : Eval(fn.Args[0], rows[0], null, ctes);
            if (!TryGetStringAggregateText(singleValue, out var singleText))
                return null;

            return singleText;
        }

        return EvalStringAggregateRows(
            fn,
            rows,
            ctes,
            separator,
            GetStringAggregateDefaultSeparator(name),
            useDirectValueSelector ? valueSelector : null);
    }

    private object? GetAggregateSeparator(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => fn.Args.Count > 1 && group.Rows.Count > 0
            ? Eval(fn.Args[1], group.Rows[0], null, ctes)
            : null;

    private List<EvalRow> GetStringAggregateRows(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var orderBy = fn.WithinGroupOrderBy;
        if (orderBy is not { Count: > 0 })
            return group.Rows;

        var sortedRows = group.Rows;
        if (sortedRows.Count < 2)
            return sortedRows;

        if (orderBy.Count == 1)
        {
            var order = orderBy[0];
            var hasDirectOrderSelector = TryCreateStringAggregateValueSelector(order.Expr, out var orderValueSelector);
            var keyedRows = new (EvalRow Row, object? Key)[sortedRows.Count];
            for (var i = 0; i < sortedRows.Count; i++)
            {
                var row = sortedRows[i];
                keyedRows[i] = (row, hasDirectOrderSelector
                    ? orderValueSelector!(row)
                    : Eval(order.Expr, row, null, ctes));
            }

            Array.Sort(keyedRows, (left, right) =>
            {
                var comparison = CompareSql(left.Key, right.Key);
                return order.Desc ? -comparison : comparison;
            });

            for (var i = 0; i < keyedRows.Length; i++)
                sortedRows[i] = keyedRows[i].Row;

            return sortedRows;
        }

        var orderValueSelectors = new Func<EvalRow, object?>?[orderBy.Count];
        for (var i = 0; i < orderBy.Count; i++)
            if (TryCreateStringAggregateValueSelector(orderBy[i].Expr, out var selector))
                orderValueSelectors[i] = selector;

        var keyedRowsMulti = new (EvalRow Row, object?[] Keys)[sortedRows.Count];

        for (var rowIndex = 0; rowIndex < sortedRows.Count; rowIndex++)
        {
            var row = sortedRows[rowIndex];
            var values = new object?[orderBy.Count];
            for (var i = 0; i < orderBy.Count; i++)
                values[i] = orderValueSelectors[i] is null
                    ? Eval(orderBy[i].Expr, row, null, ctes)
                    : orderValueSelectors[i]!(row);

            keyedRowsMulti[rowIndex] = (row, values);
        }

        Array.Sort(keyedRowsMulti, (left, right) =>
        {
            var leftValues = left.Keys;
            var rightValues = right.Keys;

            for (var i = 0; i < orderBy.Count; i++)
            {
                var comparison = CompareSql(leftValues[i], rightValues[i]);
                if (comparison == 0)
                    continue;

                return orderBy[i].Desc ? -comparison : comparison;
            }

            return 0;
        });

        for (var i = 0; i < keyedRowsMulti.Length; i++)
            sortedRows[i] = keyedRowsMulti[i].Row;

        return sortedRows;
    }

    private string? EvalStringAggregateRows(
        CallExpr fn,
        IEnumerable<EvalRow> rows,
        IDictionary<string, Source> ctes,
        object? separatorObj,
        string defaultSeparator,
        Func<EvalRow, object?>? valueSelector = null)
    {
        var separator = separatorObj?.ToString() ?? defaultSeparator;
        var estimatedCount = GetKnownRowCount(rows, defaultValue: 1);
        var useOrdinalTextComparison = Dialect?.TextComparison == StringComparison.Ordinal;
        StringBuilder? builder = null;
        var hasValue = false;
        HashSet<string>? distinct = fn.Distinct
            ? CreateDistinctStringSet(useOrdinalTextComparison, estimatedCount)
            : null;
        var estimatedCapacity = EstimateStringAggregateCapacity(estimatedCount, separator.Length);

        if (rows is IList<EvalRow> rowList)
        {
            if (valueSelector is null)
            {
                for (var i = 0; i < rowList.Count; i++)
                {
                    var value = Eval(fn.Args[0], rowList[i], null, ctes);
                    if (distinct is null)
                    {
                        if (!TryGetStringAggregateText(value, out var segmentText))
                            continue;

                        if (!hasValue)
                        {
                            builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText.Length));
                            builder.Append(segmentText);
                            hasValue = true;
                            continue;
                        }

                        builder!.Append(separator);
                        builder.Append(segmentText);
                        continue;
                    }

                    if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out var segmentText01, out var distinctKey))
                        continue;

                    if (!distinct.Add(distinctKey))
                        continue;

                    if (!hasValue)
                    {
                        builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText01.Length));
                        builder.Append(segmentText01);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(segmentText01);
                }
            }
            else
            {
                for (var i = 0; i < rowList.Count; i++)
                {
                    var value = valueSelector(rowList[i]);
                    if (distinct is null)
                    {
                        if (!TryGetStringAggregateText(value, out var segmentText1))
                            continue;

                        if (!hasValue)
                        {
                            builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText1.Length));
                            builder.Append(segmentText1);
                            hasValue = true;
                            continue;
                        }

                        builder!.Append(separator);
                        builder.Append(segmentText1);
                        continue;
                    }

                    if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out var segmentText11, out var distinctKey))
                        continue;

                    if (!distinct.Add(distinctKey))
                        continue;

                    if (!hasValue)
                    {
                        builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText11.Length));
                        builder.Append(segmentText11);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(segmentText11);
                }
            }
        }
        else
        {
            if (valueSelector is null)
            {
                foreach (var row in rows)
                {
                    var value = Eval(fn.Args[0], row, null, ctes);
                    if (distinct is null)
                    {
                        if (!TryGetStringAggregateText(value, out var segmentText2))
                            continue;

                        if (!hasValue)
                        {
                            builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText2.Length));
                            builder.Append(segmentText2);
                            hasValue = true;
                            continue;
                        }

                        builder!.Append(separator);
                        builder.Append(segmentText2);
                        continue;
                    }

                    if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out var segmentText21, out var distinctKey))
                        continue;

                    if (!distinct.Add(distinctKey))
                        continue;

                    if (!hasValue)
                    {
                        builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText21.Length));
                        builder.Append(segmentText21);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(segmentText21);
                }
            }
            else
            {
                foreach (var row in rows)
                {
                    var value = valueSelector(row);
                    if (distinct is null)
                    {
                        if (!TryGetStringAggregateText(value, out var segmentText3))
                            continue;

                        if (!hasValue)
                        {
                            builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText3.Length));
                            builder.Append(segmentText3);
                            hasValue = true;
                            continue;
                        }

                        builder!.Append(separator);
                        builder.Append(segmentText3);
                        continue;
                    }

                    if (!TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison, out var segmentText31, out var distinctKey))
                        continue;

                    if (!distinct.Add(distinctKey))
                        continue;

                    if (!hasValue)
                    {
                        builder = new StringBuilder(Math.Max(estimatedCapacity, segmentText31.Length));
                        builder.Append(segmentText31);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(segmentText31);
                }
            }
        }

        return hasValue ? builder!.ToString() : null;
    }

    private static string GetStringAggregateDefaultSeparator(string name)
        => name == "LISTAGG" ? string.Empty : ",";

    private static int EstimateStringAggregateCapacity(int rowCount, int separatorLength)
    {
        if (rowCount <= 1)
            return 16;

        var estimated = rowCount * Math.Max(8, separatorLength + 6);
        return Math.Min(estimated, 64 * 1024);
    }

    private static int GetKnownRowCount(IEnumerable<EvalRow> rows, int defaultValue = 0)
    {
        if (rows is ICollection<EvalRow> collection)
            return collection.Count;

        if (rows is IReadOnlyCollection<EvalRow> readOnlyCollection)
            return readOnlyCollection.Count;

        return defaultValue;
    }

    private string? EvalSimpleStringAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        object? separatorObj,
        string defaultSeparator)
    {
        if (fn.Args.Count == 0)
            return null;

        var hasDirectValueSelector = TryCreateStringAggregateValueSelector(fn.Args[0], out var valueSelector);
        if (group.Rows.Count == 1)
        {
            var singleValue = hasDirectValueSelector
                ? valueSelector!(group.Rows[0])
                : Eval(fn.Args[0], group.Rows[0], null, ctes);
            if (!TryGetStringAggregateText(singleValue, out var singleText))
                return null;

            return singleText;
        }

        var separator = separatorObj?.ToString() ?? defaultSeparator;
        StringBuilder? builder = null;
        var hasValue = false;
        var estimatedCapacity = EstimateStringAggregateCapacity(group.Rows.Count, separator.Length);

        if (!hasDirectValueSelector)
        {
            for (var i = 0; i < group.Rows.Count; i++)
            {
                var value = Eval(fn.Args[0], group.Rows[i], null, ctes);
                if (IsNullish(value))
                    continue;

                var text = value?.ToString() ?? string.Empty;
                if (!hasValue)
                {
                    builder = new StringBuilder(Math.Max(estimatedCapacity, text.Length));
                    builder.Append(text);
                    hasValue = true;
                    continue;
                }

                builder!.Append(separator);
                builder.Append(text);
            }
        }
        else
        {
            for (var i = 0; i < group.Rows.Count; i++)
            {
                var value = valueSelector!(group.Rows[i]);
                if (IsNullish(value))
                    continue;

                var text = value?.ToString() ?? string.Empty;
                if (!hasValue)
                {
                    builder = new StringBuilder(Math.Max(estimatedCapacity, text.Length));
                    builder.Append(text);
                    hasValue = true;
                    continue;
                }

                builder!.Append(separator);
                builder.Append(text);
            }
        }

        return hasValue ? builder!.ToString() : null;
    }

    private static bool TryCreateStringAggregateValueSelector(
        SqlExpr expr,
        out Func<EvalRow, object?> selector)
    {
        switch (expr)
        {
            case ColumnExpr column:
                selector = row => ResolveColumn(column.Qualifier, column.Name, row);
                return true;
            case IdentifierExpr identifier:
                selector = row => ResolveIdentifier(identifier.Name, row);
                return true;
            case LiteralExpr literal:
                selector = _ => literal.Value;
                return true;
            default:
                selector = null!;
                return false;
        }
    }

    private bool TryEvalAggregateCount(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        out object? value)
    {
        if (name != "COUNT" && name != "COUNT_BIG")
        {
            value = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            value = (long)group.Rows.Count;
            return true;
        }

        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
        {
            value = (long)group.Rows.Count;
            return true;
        }

        long c = 0;
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v)) c++;
        }
        value = c;
        return true;
    }

    private List<object?>? TryGetAggregateValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<object?>(group.Rows.Count);
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
            var (exprRaw, _) = SplitTrailingAsAlias(si.Raw, si.Alias);
            if (SelectItemContainsAggregate(exprRaw))
                return true;
        }
        return q.Having is not null
            && WalkHasAggregate(q.Having);
    }

    private bool SelectItemContainsAggregate(string exprRaw)
    {
        var looksAggregatedOutsideSubqueries = AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        if (!looksAggregatedOutsideSubqueries)
            return false;

#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            var parsedExpression = ParseScalarExpr(exprRaw);
            return WalkHasAggregate(parsedExpression);
        }
        catch (Exception e)
        {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{GetType().Name}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine(e);

            // fallback: preserve aggregate semantics even when expression parsing fails.
            return AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    private static bool WalkHasAggregate(SqlExpr expr)
        => AggregateExpressionInspector.WalkHasAggregate(expr);

    // ---------------- RESOLUTION HELPERS ----------------

    private object? ResolveParam(
        string name)
    {
        if (TryResolveLocalFunctionValue(name, out var localValue))
            return localValue;

        return QueryRowValueHelper.ResolveParam(_pars, name);
    }

    private static object? ResolveIdentifier(
        string name,
        EvalRow row)
        => QueryRowValueHelper.ResolveIdentifier(name, row);

    private static object? ResolveColumn(
        string? qualifier,
        string col,
        EvalRow row)
        => QueryRowValueHelper.ResolveColumn(qualifier, col, row);

    private static TableResultMock ApplyDistinct(
        TableResultMock res,
        ISqlDialect? dialect)
        => QueryRowValueHelper.ApplyDistinct(res, dialect);

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

    internal sealed record MySqlIndexHintPlan(
        HashSet<string> AllowedIndexNames,
        HashSet<string> MissingForcedIndexes,
        bool HasRowAccessHints,
        IReadOnlyHashSet<string> PrimaryEquivalentIndexNames);

    internal sealed class Source
    {
        internal ITableMock? Physical { get; }
        private readonly TableResultMock? _result;
        private readonly Dictionary<string, TableResultColMock>? _resultColumnMetadataLookup;
        private readonly HashSet<string> _columnNameLookup;
        private readonly string[]? _qualifiedColumnNames;
        private readonly string[]? _resultQualifiedColumnNames;
        private readonly string[]? _sourceQualifiedColumnNames;
        private readonly int[]? _physicalColumnIndexes;
        private readonly Dictionary<string, string>? _qualifiedColumnNameLookup;
        private readonly string? _singlePrimaryKeyColumnName;
        private readonly IReadOnlyList<string>? _requestedPartitionNames;
        /// <summary>
        /// EN: Gets or sets Alias.
        /// PT: Obtém ou define Alias.
        /// </summary>
        public string Alias { get; }
        /// <summary>
        /// EN: Gets or sets Name.
        /// PT: Obtém ou define Name.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// EN: Gets or sets ColumnNames.
        /// PT: Obtém ou define ColumnNames.
        /// </summary>
        public IReadOnlyList<string> ColumnNames { get; }
        public IReadOnlyList<SqlMySqlIndexHint> MySqlIndexHints { get; }
        private Source(
            string name,
            string alias,
            ITableMock physical,
            IReadOnlyList<SqlMySqlIndexHint>? mySqlIndexHints = null,
            IReadOnlyList<string>? requestedPartitionNames = null)
        {
            Alias = alias;
            Name = name;
            Physical = physical;
            _result = null;
            _resultColumnMetadataLookup = null;
            var physicalColumns = new List<KeyValuePair<string, ColumnDef>>(physical.Columns.Count);
            foreach (var column in physical.Columns)
                physicalColumns.Add(column);

            physicalColumns.Sort(static (left, right) => left.Value.Index.CompareTo(right.Value.Index));

            var columnNames = new string[physicalColumns.Count];
            var physicalColumnIndexes = new int[physicalColumns.Count];
            var qualifiedColumnNames = new string[physicalColumns.Count];
            var sourceQualifiedColumnNames = Name.Equals(Alias, StringComparison.OrdinalIgnoreCase)
                ? null
                : new string[physicalColumns.Count];

            for (var i = 0; i < physicalColumns.Count; i++)
            {
                var entry = physicalColumns[i];
                var columnName = entry.Key;
                var column = entry.Value;
                columnNames[i] = columnName;
                physicalColumnIndexes[i] = column.Index;
                qualifiedColumnNames[i] = $"{Alias}.{columnName}";
                if (sourceQualifiedColumnNames is not null)
                    sourceQualifiedColumnNames[i] = $"{Name}.{columnName}";
            }

            ColumnNames = columnNames;
            _columnNameLookup = BuildColumnNameLookup(ColumnNames);
            _physicalColumnIndexes = physicalColumnIndexes;
            _qualifiedColumnNames = qualifiedColumnNames;
            _resultQualifiedColumnNames = null;
            _sourceQualifiedColumnNames = sourceQualifiedColumnNames;
            _qualifiedColumnNameLookup = BuildQualifiedColumnNameLookup(ColumnNames, Alias);
            _singlePrimaryKeyColumnName = TryResolveSinglePrimaryKeyColumnName(physical, out var primaryKeyColumnName)
                ? primaryKeyColumnName
                : null;
            MySqlIndexHints = mySqlIndexHints ?? [];
            _requestedPartitionNames = requestedPartitionNames;
        }
        private Source(string name, string alias, TableResultMock result)
        {
            Alias = alias;
            Name = name;
            _result = result;
            Physical = null;
            _resultColumnMetadataLookup = BuildResultColumnMetadataLookup(result);
            var resultColumns = new List<TableResultColMock>(result.Columns);
            resultColumns.Sort(static (left, right) => left.ColumIndex.CompareTo(right.ColumIndex));

            var columnNames = new string[resultColumns.Count];
            var qualifiedColumnNames = new string[resultColumns.Count];
            var resultQualifiedColumnNames = new string[resultColumns.Count];
            var sourceQualifiedColumnNames = Name.Equals(Alias, StringComparison.OrdinalIgnoreCase)
                ? null
                : new string[resultColumns.Count];

            for (var i = 0; i < resultColumns.Count; i++)
            {
                var column = resultColumns[i];
                columnNames[i] = column.ColumnAlias;
                qualifiedColumnNames[i] = $"{Alias}.{column.ColumnAlias}";
                resultQualifiedColumnNames[i] = $"{Alias}.{column.ColumnAlias}";
                if (sourceQualifiedColumnNames is not null)
                    sourceQualifiedColumnNames[i] = $"{Name}.{column.ColumnAlias}";
            }

            ColumnNames = columnNames;
            _columnNameLookup = BuildColumnNameLookup(ColumnNames);
            _qualifiedColumnNames = qualifiedColumnNames;
            _resultQualifiedColumnNames = resultQualifiedColumnNames;
            _sourceQualifiedColumnNames = sourceQualifiedColumnNames;
            _physicalColumnIndexes = null;
            _qualifiedColumnNameLookup = BuildQualifiedColumnNameLookup(ColumnNames, Alias);
            _singlePrimaryKeyColumnName = null;
            _requestedPartitionNames = null;
            MySqlIndexHints = [];
        }
        /// <summary>
        /// EN: Implements WithAlias.
        /// PT: Implementa WithAlias.
        /// </summary>
        public Source WithAlias(string alias)
        {
            if (Physical is not null)
                return FromPhysical(Name, alias, Physical, MySqlIndexHints, _requestedPartitionNames);
            return FromResult(Name, alias, _result!);
        }

        internal bool TryGetColumnMetadata(string columnName, out TableResultColMock? metadata)
        {
            metadata = null!;
            if (_result is null)
                return false;

            if (_resultColumnMetadataLookup is not null
                && _resultColumnMetadataLookup.TryGetValue(columnName, out metadata))
            {
                return true;
            }

            return false;
        }

        internal bool TryGetQualifiedColumnName(string columnName, out string? qualifiedColumnName)
        {
            qualifiedColumnName = string.Empty;
            if (_qualifiedColumnNameLookup is null)
                return false;

            return _qualifiedColumnNameLookup.TryGetValue(columnName, out qualifiedColumnName);
        }

        internal bool ContainsColumnName(string columnName)
            => _columnNameLookup.Contains(columnName);

        internal string GetQualifiedColumnName(int columnIndex)
            => _qualifiedColumnNames is not null
                ? _qualifiedColumnNames[columnIndex]
                : $"{Alias}.{ColumnNames[columnIndex]}";

        internal bool TryGetSourceQualifiedColumnName(int columnIndex, out string qualifiedColumnName)
        {
            qualifiedColumnName = string.Empty;
            if (_sourceQualifiedColumnNames is null)
                return false;

            qualifiedColumnName = _sourceQualifiedColumnNames[columnIndex];
            return true;
        }

        internal string? GetSourceQualifiedColumnName(int columnIndex)
            => _sourceQualifiedColumnNames is null
                ? null
                : _sourceQualifiedColumnNames[columnIndex];

        internal bool TryGetSinglePrimaryKeyColumnName(out string columnName)
        {
            if (string.IsNullOrWhiteSpace(_singlePrimaryKeyColumnName))
            {
                columnName = string.Empty;
                return false;
            }

            columnName = _singlePrimaryKeyColumnName ?? string.Empty;
            return true;
        }

        private static Dictionary<string, TableResultColMock> BuildResultColumnMetadataLookup(TableResultMock result)
        {
            var lookup = new Dictionary<string, TableResultColMock>(Math.Max(result.Columns.Count * 2, 1), StringComparer.OrdinalIgnoreCase);
            foreach (var column in result.Columns)
            {
                lookup.TryAdd(column.ColumnAlias, column);
                lookup.TryAdd(column.ColumnName, column);
            }

            return lookup;
        }

        private static Dictionary<string, string> BuildQualifiedColumnNameLookup(IReadOnlyList<string> columnNames, string alias)
        {
            var lookup = new Dictionary<string, string>(Math.Max(columnNames.Count * 2, 1), StringComparer.OrdinalIgnoreCase);
            foreach (var columnName in columnNames)
                lookup.TryAdd(columnName, $"{alias}.{columnName}");

            return lookup;
        }

        private static HashSet<string> BuildColumnNameLookup(IReadOnlyList<string> columnNames)
        {
            var lookup = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var columnName in columnNames)
                lookup.Add(columnName);

            return lookup;
        }

        private static bool TryResolveSinglePrimaryKeyColumnName(ITableMock physical, out string columnName)
        {
            columnName = string.Empty;
            var primaryKeyIndexes = physical.PrimaryKeyIndexes;
            if (primaryKeyIndexes.Count != 1)
                return false;

            var pkIndex = default(int);
            foreach (var candidatePkIndex in primaryKeyIndexes)
            {
                pkIndex = candidatePkIndex;
                break;
            }

            if (physical is TableMock tableMock
                && tableMock.ColumnsByIndex.TryGetValue(pkIndex, out var pkColumnName)
                && !string.IsNullOrWhiteSpace(pkColumnName))
            {
                columnName = pkColumnName;
                return true;
            }

            foreach (var column in physical.Columns)
            {
                if (column.Value.Index == pkIndex)
                {
                    columnName = column.Key;
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// EN: Implements Rows.
        /// PT: Implementa Rows.
        /// </summary>
        public IEnumerable<Dictionary<string, object?>> Rows()
        {
            if (Physical is not null)
            {
                foreach (var row in Physical)
                {
                    if (_requestedPartitionNames is { Count: > 0 } requestedPartitions
                        && Physical is TableMock table
                        && !table.MatchesRequestedPartitions(row, requestedPartitions))
                    {
                        continue;
                    }

                    var dict = new Dictionary<string, object?>(Math.Max(ColumnNames.Count, 1), StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < ColumnNames.Count; i++)
                    {
                        var idx = _physicalColumnIndexes![i];
                        dict[_qualifiedColumnNames![i]] = row?.TryGetValue(idx, out var v) == true
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
                    var dict = new Dictionary<string, object?>(Math.Max(_result.Columns.Count, 1), StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < _resultQualifiedColumnNames!.Length; i++)
                    {
                        dict[_resultQualifiedColumnNames[i]] = row.TryGetValue(i, out var v)
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

        public IEnumerable<Dictionary<string, object?>> RowsByIndexes(int index)
        {
            if (Physical is null)
                return Rows();

            return EnumerateRowByIndex(index);
        }

        private IEnumerable<Dictionary<string, object?>> EnumerateRowByIndex(int raw)
        {
            if (raw < 0 || raw >= Physical!.Count)
                yield break;

            var row = Physical[raw];
            if (_requestedPartitionNames is { Count: > 0 } requestedPartitions
                && Physical is TableMock table
                && !table.MatchesRequestedPartitions(row, requestedPartitions))
            {
                yield break;
            }

            var dict = new Dictionary<string, object?>(Math.Max(ColumnNames.Count, 1), StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < ColumnNames.Count; i++)
            {
                var idx = _physicalColumnIndexes![i];
                dict[_qualifiedColumnNames![i]] = row.TryGetValue(idx, out var v)
                    ? v
                    : null;
            }

            yield return dict;
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
                if (_requestedPartitionNames is { Count: > 0 } requestedPartitions
                    && Physical is TableMock table
                    && !table.MatchesRequestedPartitions(row, requestedPartitions))
                {
                    continue;
                }

                var dict = new Dictionary<string, object?>(Math.Max(ColumnNames.Count, 1), StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < ColumnNames.Count; i++)
                {
                    var idx = _physicalColumnIndexes![i];
                    dict[_qualifiedColumnNames![i]] = row.TryGetValue(idx, out var v)
                        ? v
                        : null;
                }

                yield return dict;
            }
        }

        /// <summary>
        /// EN: Implements FromPhysical.
        /// PT: Implementa FromPhysical.
        /// </summary>
        public static Source FromPhysical(
            string tableName,
            string alias,
            ITableMock physical,
            IReadOnlyList<SqlMySqlIndexHint>? mySqlIndexHints = null,
            IReadOnlyList<string>? requestedPartitionNames = null)
            => new(tableName, alias, physical, mySqlIndexHints, requestedPartitionNames);

        /// <summary>
        /// EN: Returns a physical source with a different partition filter.
        /// PT: Retorna uma fonte fisica com filtro de particao diferente.
        /// </summary>
        public Source WithRequestedPartitions(IReadOnlyList<string>? requestedPartitionNames)
        {
            if (Physical is null)
                return this;

            return FromPhysical(Name, Alias, Physical, MySqlIndexHints, requestedPartitionNames);
        }

        /// <summary>
        /// EN: Implements FromResult.
        /// PT: Implementa FromResult.
        /// </summary>
        public static Source FromResult(string tableName, string alias, TableResultMock result)
            => new(tableName, alias, result);

        /// <summary>
        /// EN: Implements FromResult.
        /// PT: Implementa FromResult.
        /// </summary>
        public static Source FromResult(string tableName, TableResultMock result)
            => new(tableName, tableName, result);
    }

    internal readonly record struct InMembershipState(bool Matched, bool HasNullCandidate);
    internal readonly record struct DateTimeParseCacheEntry(bool Success, DateTime Value);
    internal readonly record struct DateTimeOffsetParseCacheEntry(bool Success, DateTimeOffset Value);
    internal readonly record struct TimeSpanParseCacheEntry(bool Success, TimeSpan Value);
    internal enum SqlTruthValue { True, False, Unknown }
    internal enum TemporalUnit { Unknown, Year, Month, Day, Hour, Minute, Second }

    private static readonly IReadOnlyDictionary<string, TemporalUnit> _temporalUnits = new Dictionary<string, TemporalUnit>(StringComparer.OrdinalIgnoreCase)
    {
        [SqlConst.YEAR] = TemporalUnit.Year,
        ["YEARS"] = TemporalUnit.Year,
        ["YY"] = TemporalUnit.Year,
        ["YYYY"] = TemporalUnit.Year,
        ["MONTH"] = TemporalUnit.Month,
        ["MONTHS"] = TemporalUnit.Month,
        ["MM"] = TemporalUnit.Month,
        ["DAY"] = TemporalUnit.Day,
        ["DAYS"] = TemporalUnit.Day,
        ["DD"] = TemporalUnit.Day,
        ["D"] = TemporalUnit.Day,
        ["HOUR"] = TemporalUnit.Hour,
        ["HOURS"] = TemporalUnit.Hour,
        ["HH"] = TemporalUnit.Hour,
        ["MINUTE"] = TemporalUnit.Minute,
        ["MINUTES"] = TemporalUnit.Minute,
        ["MI"] = TemporalUnit.Minute,
        ["N"] = TemporalUnit.Minute,
        ["SECOND"] = TemporalUnit.Second,
        ["SECONDS"] = TemporalUnit.Second,
        ["SS"] = TemporalUnit.Second,
        ["S"] = TemporalUnit.Second
    };

    private static readonly IReadOnlyDictionary<string, DayOfWeek> _oracleDayOfWeekMap = new Dictionary<string, DayOfWeek>(StringComparer.OrdinalIgnoreCase)
    {
        ["SUNDAY"] = DayOfWeek.Sunday,
        ["MONDAY"] = DayOfWeek.Monday,
        ["TUESDAY"] = DayOfWeek.Tuesday,
        ["WEDNESDAY"] = DayOfWeek.Wednesday,
        ["THURSDAY"] = DayOfWeek.Thursday,
        ["FRIDAY"] = DayOfWeek.Friday,
        ["SATURDAY"] = DayOfWeek.Saturday,
        ["SUN"] = DayOfWeek.Sunday,
        ["MON"] = DayOfWeek.Monday,
        ["TUE"] = DayOfWeek.Tuesday,
        ["WED"] = DayOfWeek.Wednesday,
        ["THU"] = DayOfWeek.Thursday,
        ["FRI"] = DayOfWeek.Friday,
        ["SAT"] = DayOfWeek.Saturday
    };

    internal sealed record EvalRow(
        Dictionary<string, object?> Fields,
        Dictionary<string, Source> Sources)
    {
        internal object?[]? OrdinalValues { get; init; }
        internal Dictionary<string, int>? OrdinalIndexes { get; init; }
        internal Source? SingleSource { get; set; }

        /// <summary>
        /// EN: Implements FromProjected.
        /// PT: Implementa FromProjected.
        /// </summary>
        public static EvalRow FromProjected(
            TableResultMock res,
            Dictionary<int, object?> row,
            Dictionary<string, int> aliasToIndex,
            Dictionary<string, object?>? joinFields = null)
        {
            var fields = new Dictionary<string, object?>(
                Math.Max(aliasToIndex.Count + (joinFields?.Count ?? 0) * 2, 1),
                StringComparer.OrdinalIgnoreCase);
            var ordinalValues = new object?[res.Columns.Count];
            foreach (var kv in aliasToIndex)
            {
                var value = row.TryGetValue(kv.Value, out var v) ? v : null;
                fields[kv.Key] = value;
                if (kv.Value >= 0 && kv.Value < ordinalValues.Length)
                    ordinalValues[kv.Value] = value;
            }
            if (joinFields is not null)
            {
                foreach (var pair in joinFields)
                {
                    fields.TryAdd(pair.Key, pair.Value);

                    var dot = pair.Key.IndexOf('.');
                    if (dot <= 0 || dot + 1 >= pair.Key.Length)
                        continue;

                    fields.TryAdd(pair.Key[(dot + 1)..], pair.Value);
                }
            }

            return new EvalRow(fields, new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase))
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = new Dictionary<string, int>(aliasToIndex, StringComparer.OrdinalIgnoreCase),
                SingleSource = null
            };
        }

        /// <summary>
        /// EN: Implements CloneRow.
        /// PT: Implementa CloneRow.
        /// </summary>
        public EvalRow CloneRow()
        {
            var fields = new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase);
            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);

            return new EvalRow(fields, sources)
            {
                OrdinalValues = OrdinalValues is null ? null : [.. OrdinalValues],
                OrdinalIndexes = OrdinalIndexes is null
                    ? null
                    : new Dictionary<string, int>(OrdinalIndexes, StringComparer.OrdinalIgnoreCase),
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Clones the row with extra dictionary capacity for upcoming field and source additions.
        /// PT: Clona a linha com capacidade extra nos dicionarios para futuras adicoes de campos e fontes.
        /// </summary>
        /// <param name="extraFieldCapacity">EN: Extra capacity hint for fields. PT: Capacidade extra sugerida para campos.</param>
        /// <param name="extraSourceCapacity">EN: Extra capacity hint for sources. PT: Capacidade extra sugerida para fontes.</param>
        public EvalRow CloneRow(int extraFieldCapacity, int extraSourceCapacity)
        {
            var fields = new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase);
            fields.EnsureCapacity(fields.Count + extraFieldCapacity);

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + extraSourceCapacity);

            return new EvalRow(fields, sources)
            {
                OrdinalValues = OrdinalValues is null ? null : [.. OrdinalValues],
                OrdinalIndexes = OrdinalIndexes is null
                    ? null
                    : new Dictionary<string, int>(OrdinalIndexes, StringComparer.OrdinalIgnoreCase),
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Merges the current row with fields from a joined row and appends the joined source.
        /// PT: Combina a linha atual com campos de uma linha associada e adiciona a source associada.
        /// </summary>
        /// <param name="rightSource">EN: Joined source to append. PT: Source associada a adicionar.</param>
        /// <param name="rightFields">EN: Fields produced by the joined row. PT: Campos produzidos pela linha associada.</param>
        internal EvalRow MergeJoinRow(Source rightSource, Dictionary<string, object?> rightFields)
        {
            var fields = new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase);
            fields.EnsureCapacity(fields.Count + rightSource.ColumnNames.Count * 2);

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + 1);
            sources[rightSource.Alias] = rightSource;

            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasLeftOrdinalMetadata = OrdinalValues is not null && OrdinalIndexes is not null;
            var rightOrdinalCount = rightSource.ColumnNames.Count;
            if (hasLeftOrdinalMetadata || rightOrdinalCount > 0)
            {
                var leftOrdinalCount = hasLeftOrdinalMetadata ? OrdinalValues!.Length : 0;
                ordinalValues = new object?[leftOrdinalCount + rightOrdinalCount];
                ordinalIndexes = hasLeftOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(Math.Max(1, rightOrdinalCount * 3), StringComparer.OrdinalIgnoreCase);

                if (hasLeftOrdinalMetadata && leftOrdinalCount > 0)
                    Array.Copy(OrdinalValues!, ordinalValues, leftOrdinalCount);

                if (rightOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + rightOrdinalCount * 3);
                    PopulateJoinedSourceColumns(rightSource, rightFields, fields, ordinalValues, ordinalIndexes, leftOrdinalCount, nullValue: null);
                }
            }

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Creates a null-extended join row for an unmatched right source.
        /// PT: Cria uma linha de join com extensao nula para uma source direita sem correspondencia.
        /// </summary>
        /// <param name="rightSource">EN: Right-side source to append. PT: Source do lado direito a adicionar.</param>
        internal EvalRow CreateNullExtendedJoinRow(Source rightSource)
        {
            var fields = new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase);
            fields.EnsureCapacity(fields.Count + rightSource.ColumnNames.Count * 2);

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + 1);
            sources[rightSource.Alias] = rightSource;

            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasLeftOrdinalMetadata = OrdinalValues is not null && OrdinalIndexes is not null;
            var rightOrdinalCount = rightSource.ColumnNames.Count;
            if (hasLeftOrdinalMetadata || rightOrdinalCount > 0)
            {
                var leftOrdinalCount = hasLeftOrdinalMetadata ? OrdinalValues!.Length : 0;
                ordinalValues = new object?[leftOrdinalCount + rightOrdinalCount];
                ordinalIndexes = hasLeftOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(Math.Max(1, rightOrdinalCount * 3), StringComparer.OrdinalIgnoreCase);

                if (hasLeftOrdinalMetadata && leftOrdinalCount > 0)
                    Array.Copy(OrdinalValues!, ordinalValues, leftOrdinalCount);

                if (rightOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + rightOrdinalCount * 3);
                    PopulateJoinedSourceColumns(rightSource, null, fields, ordinalValues, ordinalIndexes, leftOrdinalCount, nullValue: null);
                }
            }

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Attaches outer-row values without overwriting fields already produced by the inner row.
        /// PT: Anexa valores da linha externa sem sobrescrever campos ja produzidos pela linha interna.
        /// </summary>
        /// <param name="outer">EN: Outer row to overlay. PT: Linha externa a sobrepor.</param>
        internal EvalRow AttachOuterRow(EvalRow outer)
        {
            if (outer.Fields.Count == 0
                && outer.Sources.Count == 0
                && outer.OrdinalValues is null
                && outer.OrdinalIndexes is null)
            {
                return this;
            }

            var fields = new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase);
            fields.EnsureCapacity(fields.Count + outer.Fields.Count * 2);

            foreach (var it in outer.Fields)
            {
                fields.TryAdd(it.Key, it.Value);

                var dot = it.Key.IndexOf('.');
                if (dot > 0)
                {
                    var col = it.Key[(dot + 1)..];
                    fields.TryAdd(col, it.Value);
                }
            }

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + outer.Sources.Count);

            foreach (var it in outer.Sources)
                sources.TryAdd(it.Key, it.Value);

            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasInnerOrdinalMetadata = OrdinalValues is not null && OrdinalIndexes is not null;
            var hasOuterOrdinalMetadata = outer.OrdinalValues is not null && outer.OrdinalIndexes is not null;
            if (hasInnerOrdinalMetadata || hasOuterOrdinalMetadata)
            {
                var innerOrdinalCount = hasInnerOrdinalMetadata ? OrdinalValues!.Length : 0;
                var outerOrdinalCount = hasOuterOrdinalMetadata ? outer.OrdinalValues!.Length : 0;
                ordinalValues = new object?[innerOrdinalCount + outerOrdinalCount];
                ordinalIndexes = hasInnerOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                if (hasInnerOrdinalMetadata && innerOrdinalCount > 0)
                    Array.Copy(OrdinalValues!, ordinalValues, innerOrdinalCount);

                if (hasOuterOrdinalMetadata && outerOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + outer.OrdinalIndexes!.Count);
                    AppendOrdinalMetadata(outer, ordinalValues, ordinalIndexes, innerOrdinalCount);
                }
            }

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Returns an empty evaluation row placeholder.
        /// PT: Retorna um placeholder de linha de avaliação vazia.
        /// </summary>
        public static EvalRow Empty()
            => new(
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase))
            {
                OrdinalValues = [],
                OrdinalIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            };

        /// <summary>
        /// EN: Implements AddSource.
        /// PT: Implementa AddSource.
        /// </summary>
        public void AddSource(Source src)
        {
            Sources[src.Alias] = src;
            SingleSource = Sources.Count == 1 ? src : null;
        }

        /// <summary>
        /// EN: Implements AddFields.
        /// PT: Implementa AddFields.
        /// </summary>
        public void AddFields(Dictionary<string, object?> fields)
        {
            foreach (var it in fields)
            {
                Fields[it.Key] = it.Value;
                UpdateOrdinalValue(it.Key, it.Value);
            }

            // also expose unqualified columns (first wins) for convenience
            foreach (var it in fields)
            {
                var dot = it.Key.IndexOf('.');
                if (dot > 0)
                {
                    var col = it.Key[(dot + 1)..];
                    if (Fields.TryAdd(col, it.Value))
                    {
                        UpdateOrdinalValue(col, it.Value);
                    }
                }
            }
        }

        private static void PopulateJoinedSourceColumns(
            Source source,
            Dictionary<string, object?>? sourceFields,
            Dictionary<string, object?> targetFields,
            object?[] ordinalValues,
            Dictionary<string, int> ordinalIndexes,
            int ordinalOffset,
            object? nullValue)
        {
            for (var i = 0; i < source.ColumnNames.Count; i++)
            {
                var columnName = source.ColumnNames[i];
                var qualifiedName = source.GetQualifiedColumnName(i);
                var ordinalIndex = ordinalOffset + i;
                var value = sourceFields is not null && sourceFields.TryGetValue(qualifiedName, out var current)
                    ? current
                    : nullValue;

                targetFields[qualifiedName] = value;
                targetFields.TryAdd(columnName, value);

                ordinalValues[ordinalIndex] = value;
                ordinalIndexes.TryAdd(qualifiedName, ordinalIndex);
                ordinalIndexes.TryAdd(columnName, ordinalIndex);
                var sourceQualifiedName = source.GetSourceQualifiedColumnName(i);
                if (sourceQualifiedName is not null)
                    ordinalIndexes.TryAdd(sourceQualifiedName, ordinalIndex);
            }
        }

        private static void AppendOrdinalMetadata(
            EvalRow sourceRow,
            object?[] ordinalValues,
            Dictionary<string, int> ordinalIndexes,
            int ordinalOffset)
        {
            if (sourceRow.OrdinalValues is null || sourceRow.OrdinalIndexes is null)
                return;

            var sourceOrdinalCount = Math.Min(sourceRow.OrdinalValues.Length, Math.Max(0, ordinalValues.Length - ordinalOffset));
            if (sourceOrdinalCount > 0)
                Array.Copy(sourceRow.OrdinalValues, 0, ordinalValues, ordinalOffset, sourceOrdinalCount);

            foreach (var kv in sourceRow.OrdinalIndexes)
            {
                if (kv.Value < 0 || kv.Value >= sourceOrdinalCount)
                    continue;

                ordinalIndexes.TryAdd(kv.Key, ordinalOffset + kv.Value);
            }
        }

        private void UpdateOrdinalValue(string fieldName, object? value)
        {
            if (OrdinalIndexes is null
                || OrdinalValues is null)
            {
                return;
            }

            if (OrdinalIndexes.TryGetValue(fieldName, out var ordinalIndex)
                && ordinalIndex >= 0
                && ordinalIndex < OrdinalValues.Length)
            {
                OrdinalValues[ordinalIndex] = value;
                return;
            }

            var dot = fieldName.IndexOf('.');
            if (dot <= 0)
                return;

            if (OrdinalIndexes.TryGetValue(fieldName[(dot + 1)..], out ordinalIndex)
                && ordinalIndex >= 0
                && ordinalIndex < OrdinalValues.Length)
            {
                OrdinalValues[ordinalIndex] = value;
            }
        }


        /// <summary>
        /// EN: Gets a field value by qualified or unqualified column name.
        /// PT: Obtém o valor de um campo por nome de coluna qualificado ou não qualificado.
        /// </summary>
        /// <param name="columnName">EN: Column name to read. PT: Nome da coluna a ler.</param>
        /// <returns>EN: The field value when present; otherwise null. PT: O valor do campo quando presente; caso contrário, null.</returns>
        public object? GetByName(string columnName)
            => TryGetValue(columnName, out var value) ? value : null;

        internal bool TryGetValue(string columnName, out object? value)
        {
            var hasOrdinalMetadata = OrdinalIndexes is not null
                && OrdinalValues is not null;

            if (OrdinalIndexes is not null
                && OrdinalValues is not null
                && OrdinalIndexes.TryGetValue(columnName, out var ordinalIndex)
                && ordinalIndex >= 0
                && ordinalIndex < OrdinalValues.Length)
            {
                value = OrdinalValues[ordinalIndex];
                return true;
            }

            var dot = columnName.IndexOf('.');
            if (dot > 0
                && OrdinalIndexes is not null
                && OrdinalValues is not null
                && OrdinalIndexes.TryGetValue(columnName[(dot + 1)..], out ordinalIndex)
                && ordinalIndex >= 0
                && ordinalIndex < OrdinalValues.Length)
            {
                value = OrdinalValues[ordinalIndex];
                return true;
            }

            if (Fields.TryGetValue(columnName, out var direct))
            {
                value = direct;
                return true;
            }

            if (SingleSource is not null)
            {
                var qualifiedColumnName = string.Empty;
                if (SingleSource.TryGetQualifiedColumnName(columnName, out qualifiedColumnName)
                    && qualifiedColumnName?.Length > 0
                    && Fields.TryGetValue(qualifiedColumnName, out direct))
                {
                    value = direct;
                    return true;
                }
            }

            if (hasOrdinalMetadata)
            {
                value = null;
                return false;
            }

            foreach (var hit in Fields)
            {
                var candidate = hit.Key;
                var expectedLength = columnName.Length + 1;
                if (candidate.Length <= expectedLength)
                    continue;

                if (candidate[candidate.Length - expectedLength] != '.')
                    continue;

                if (!candidate.AsSpan(candidate.Length - columnName.Length).Equals(columnName.AsSpan(), StringComparison.OrdinalIgnoreCase))
                    continue;

                value = hit.Value;
                return true;
            }

            value = null;
            return false;
        }

        internal bool TryGetSingleSource(out Source? source)
        {
            if (SingleSource is not null)
            {
                source = SingleSource;
                return true;
            }

            if (Sources.Count != 1)
            {
                source = null;
                return false;
            }

            foreach (var candidate in Sources.Values)
            {
                SingleSource = candidate;
                source = candidate;
                return true;
            }

            source = null;
            return false;
        }

        private static Source? ResolveSingleSourceCache(Dictionary<string, Source> sources)
        {
            if (sources.Count != 1)
                return null;

            foreach (var candidate in sources.Values)
                return candidate;

            return null;
        }
    }

    private static EvalRow AttachOuterRow(
        EvalRow inner,
        EvalRow outer)
        => inner.AttachOuterRow(outer);

    private static IEnumerable<EvalRow> AttachOuterRows(
        IEnumerable<EvalRow> rows,
        EvalRow outer)
    {
        foreach (var row in rows)
            yield return row.AttachOuterRow(outer);
    }

    private static List<MaterializedGroup> MaterializeGroups(IEnumerable<IGrouping<GroupKey, EvalRow>> grouped)
    {
        var materialized = grouped is ICollection<IGrouping<GroupKey, EvalRow>> collection
            ? new List<MaterializedGroup>(collection.Count)
            : new List<MaterializedGroup>();
        foreach (var group in grouped)
        {
            var rows = group is ICollection<EvalRow> rowCollection
                ? new List<EvalRow>(rowCollection.Count)
                : new List<EvalRow>();

            foreach (var row in group)
                rows.Add(row);

            materialized.Add(new MaterializedGroup(group.Key, rows));
        }

        return materialized;
    }

    internal sealed class EvalGroup
    {
        /// <summary>
        /// EN: Gets or sets Rows.
        /// PT: Obtém ou define Rows.
        /// </summary>
        public List<EvalRow> Rows { get; }
        /// <summary>
        /// EN: Implements EvalGroup.
        /// PT: Implementa EvalGroup.
        /// </summary>
        public EvalGroup(List<EvalRow> rows) => Rows = rows;
    }

    private sealed record MaterializedGroup(GroupKey Key, List<EvalRow> Rows);

    private readonly record struct GroupKey(object?[] Values)
    {
        /// <summary>
        /// EN: Implements GroupKeyComparer.
        /// PT: Implementa GroupKeyComparer.
        /// </summary>
        public static readonly IEqualityComparer<GroupKey> Comparer = new GroupKeyComparer();

        private sealed class GroupKeyComparer : IEqualityComparer<GroupKey>
        {
            /// <summary>
            /// EN: Implements Equals.
            /// PT: Implementa Equals.
            /// </summary>
            public bool Equals(GroupKey x, GroupKey y)
            {
                if (x.Values.Length != y.Values.Length) return false;
                for (int i = 0; i < x.Values.Length; i++)
                    if (!x.Values[i].EqualsSql(y.Values[i])) return false;
                return true;
            }

            /// <summary>
            /// EN: Implements GetHashCode.
            /// PT: Implementa GetHashCode.
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
        /// EN: Implements Compare.
        /// PT: Implementa Compare.
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

    private static bool IsSqlServerRowCountIdentifier(string identifier, ISqlDialect? dialect)
        => dialect is not null
           && dialect.TryGetScalarFunctionDefinition(identifier, out var definition)
           && definition is not null
           && definition.AllowsIdentifier;

    private static bool IsFoundRowsEquivalentFunction(string functionName, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return dialect.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition is not null
            && definition.AllowsCall;
    }

    private bool HasSqlCalcFoundRows(SqlSelectQuery query)
        => Dialect?.SupportsSqlCalcFoundRowsModifier == true
           && !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}



