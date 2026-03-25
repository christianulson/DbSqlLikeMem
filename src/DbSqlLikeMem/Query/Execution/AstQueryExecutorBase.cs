using static DbSqlLikeMem.AstQueryGeneralScalarFunctionEvaluator;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Executes the Pratt-based AST (<see cref="SqlSelectQuery"/>) against <see cref="TableMock"/> tables.
/// PT: Executa o AST baseado em Pratt (<see cref="SqlSelectQuery"/>) contra tabelas <see cref="TableMock"/>.
///
/// EN: The executor currently covers SELECT and WITH queries only, matching the scope of <see cref="SqlQueryParser"/>.
/// PT: O executor atualmente cobre apenas consultas SELECT e WITH, acompanhando o escopo de <see cref="SqlQueryParser"/>.
/// </summary>
internal abstract partial class AstQueryExecutorBase(QueryExecutionContext context)
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
    private AstQuerySqlServerCompatibilityFunctionEvaluator? _sqlServerCompatibilityFunctionEvaluator;
    private ISqlDialect? Dialect => _context.Dialect;
    private AstQueryJoinService JoinService
        => _joinService ??= new AstQueryJoinService(
            resolveSource: ResolveSource,
            buildMySqlIndexHintPlan: AstQueryIndexHelper.BuildMySqlIndexHintPlan,
            evalJoinPredicate: (expr, row, ctes) => Eval(expr, row, group: null, ctes).ToBool());
    private AstQuerySourceResolver SourceResolver
        => _sourceResolver ??= new AstQuerySourceResolver(
            _context,
            evalExpression: Eval,
            executeSelect: (select, ctes, outerRow) => ExecuteSelect(select, ctes, outerRow),
            executeUnion: ExecuteUnion);

    private AstQueryPivotHelper PivotHelper
        => _pivotHelper ??= new AstQueryPivotHelper(
            _context,
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
            tryEvalJsonAccessShimFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonAccessShimFunction,
            tryEvalJsonExtractionFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonExtractionFunction,
            tryEvalSqlServerJsonModifyFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalSqlServerJsonModifyFunction,
            tryEvalOpenJsonFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalOpenJsonFunction,
            tryEvalJsonUnquoteFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonUnquoteFunction,
            tryEvalToNumberFunction: AstQueryGeneralScalarFunctionEvaluator.TryEvalToNumberFunction,
            evalTryCast: EvalTryCast,
            evalParseFunction: EvalParseFunction,
            evalCast: EvalCast);

    private AstQueryCastStringAndDateTailEvaluator CastStringAndDateTailEvaluator
        => _castStringAndDateTailEvaluator ??= new AstQueryCastStringAndDateTailEvaluator(
            tryEvalCastConversionFamily: TryEvalCastConversionFamily,
            tryEvalCastConcatAndStringTail: (fn, row, group, ctes, context, evalArg, out result) =>
                AstQueryCastStringAndDateTailEvaluator.TryEvalCastConcatAndStringTail(fn, row, group, ctes, context, evalArg, out result),
            tryEvalCastDateTail: (fn, row, group, ctes, context, evalArg, out result) =>
                AstQueryCastStringAndDateTailEvaluator.TryEvalCastDateTail(fn, row, group, ctes, context, evalArg, Eval, GetTemporalUnit, out result));

    private AstQueryGeneralSystemAndJsonFunctionEvaluator GeneralSystemAndJsonFunctionEvaluator
        => _generalSystemAndJsonFunctionEvaluator ??= new AstQueryGeneralSystemAndJsonFunctionEvaluator(
            tryEvalSessionContextFunction: SqlServerUtilityFunctionEvaluator.TryEvalSessionContextFunction,
            tryEvalJsonUtilityFunctions: AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonUtilityFunctions,
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
            tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
            tryParseCachedDateTimeOffset: TryParseCachedDateTimeOffset);

    private AstQuerySqlServerSessionFunctionEvaluator SqlServerSessionFunctionEvaluator
        => _sqlServerSessionFunctionEvaluator ??= new AstQuerySqlServerSessionFunctionEvaluator(
            getDialect: () => Dialect,
            getContextInfo: _cnn.GetContextInfo,
            hasActiveTransaction: () => _cnn.HasActiveTransaction,
            tryResolveSqlServerRoleMembership: TryResolveSqlServerRoleMembership,
            tryResolveSqlServerServerRoleMembership: TryResolveSqlServerServerRoleMembership);

    private AstQuerySqlServerCompatibilityFunctionEvaluator SqlServerCompatibilityFunctionEvaluator
        => _sqlServerCompatibilityFunctionEvaluator ??= new AstQuerySqlServerCompatibilityFunctionEvaluator(
            SqlServerSessionFunctionEvaluator,
            SqlServerDatabaseFunctionEvaluator,
            SqlServerIdentityFunctionEvaluator,
            SqlServerUtilityFunctionEvaluator,
            Eval,
            GetTemporalUnit,
            ResolveTemporalUnit);

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
        ClearSubqueryEvaluationCaches();
        return UnionExecutionHelper.Execute(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            sqlContextForErrors,
            _context,
            parts1 => ExecuteSelect(parts1, null, null),
            ApplyOrderAndLimit,
            AstQueryPlanMetricsHelper.CountKnownInputTables,
            query => AstQueryPlanMetricsHelper.EstimateRowsRead(_context, query));
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

        var metrics = AstQueryPlanMetricsHelper.BuildPlanRuntimeMetrics(_context, q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(_context, q, metrics);
        var planWarnings = QueryPlanWarningHelper.BuildPlanWarnings(q, metrics);
        var runtimeContext = _context.BuildPlanRuntimeContext();
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
            QueryExecutionContext context,
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
        => SelectPlanIndexRecommendationHelper.Build(context, query, metrics);

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
            res = ApplyDistinct(res, _context);
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
        if (AstQueryDialectIdentifierEvaluator.TryResolveIdentifier(
                _context,
                identifier,
                _evaluationLocalNow,
                _evaluationUtcNow,
                _cnn,
                out var resolved))
        {
            return resolved;
        }

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
            if (name == "JSON_OBJECTAGG"
                && !dialect.TryGetScalarFunctionDefinition(name, out _))
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
            if (!dialect.SupportsSqlServerAggregateFunction(name))
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

    // ---------------- RESOLUTION HELPERS ----------------

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
        ///// <summary>
        ///// EN: Implements GroupKeyComparer.
        ///// PT: Implementa GroupKeyComparer.
        ///// </summary>
        //public static readonly IEqualityComparer<GroupKey> Comparer = new GroupKeyComparer(_context);

        internal sealed class GroupKeyComparer(
            QueryExecutionContext context
            ) : IEqualityComparer<GroupKey>
        {
            /// <summary>
            /// EN: Implements Equals.
            /// PT: Implementa Equals.
            /// </summary>
            public bool Equals(GroupKey x, GroupKey y)
            {
                if (x.Values.Length != y.Values.Length) return false;
                for (int i = 0; i < x.Values.Length; i++)
                    if (!x.Values[i].EqualsSql(y.Values[i], context)) return false;
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

    private bool HasSqlCalcFoundRows(SqlSelectQuery query)
        => Dialect?.SupportsSqlCalcFoundRowsModifier == true
           && !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}





