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
    internal static readonly object _uuidShortCounterLock = new();
    internal static long _uuidShortCounter;

    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private DbConnectionMockBase Cnn => _context.Connection;

    private IDataParameterCollection _pars => _context.DbParameters;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ITableMock> _resolvedBaseTableCache = new(StringComparer.OrdinalIgnoreCase);
    internal sealed record InSubqueryLookupState(
        List<object?> Values,
        HashSet<InLookupScalarKey>? ScalarCandidates,
        List<object?[]>? RowValues,
        HashSet<string>? RowCandidates,
        bool HasNullCandidate);

    internal sealed record CorrelatedCountLookupState(
        IReadOnlyDictionary<string, int> Counts,
        IReadOnlyList<CorrelatedLookupKeyPair> KeyPairs,
        SqlExpr? InnerFilterExpr);

    internal sealed record CorrelatedExistsLookupState(
        HashSet<string> Presence,
        IReadOnlyList<CorrelatedLookupKeyPair> KeyPairs,
        SqlExpr? InnerFilterExpr);

    internal sealed record CorrelatedLookupKeyPair(
        SqlExpr InnerExpr,
        SqlExpr OuterExpr);

    internal readonly record struct InLookupScalarKey(string Kind, string Value);
    private readonly AstSubqueryEvaluationCache _subqueryEvaluationCache = new();
    private AstQuerySubqueryLookupEvaluator? _subqueryLookupEvaluator;
    private AstQuerySubqueryComparisonEvaluator? _subqueryComparisonEvaluator;
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
    private AstQuerySqlServerDatabaseFunctionEvaluator? _sqlServerDatabaseFunctionEvaluator;
    private AstQuerySqlServerIdentityFunctionEvaluator? _sqlServerIdentityFunctionEvaluator;
    private AstQuerySqlServerUtilityFunctionEvaluator? _sqlServerUtilityFunctionEvaluator;
    private AstQuerySqlServerSessionFunctionEvaluator? _sqlServerSessionFunctionEvaluator;
    private AstQuerySqlServerCompatibilityFunctionEvaluator? _sqlServerCompatibilityFunctionEvaluator;
    private int _evalDepth;

    private AstQuerySubqueryLookupEvaluator SubqueryLookupEvaluator
        => _subqueryLookupEvaluator ??= new AstQuerySubqueryLookupEvaluator(
            _subqueryEvaluationCache,
            _context,
            Eval,
            ParseExpr,
            BuildFrom,
            (tableSource, scope) => ResolveSource(tableSource, scope),
            IndexHelper,
            AttachOuterRow,
            (select, scope, outerRow) => ExecuteSelect(select, scope, outerRow),
            PartitionHelper);
    private AstQueryJoinService JoinService
        => _joinService ??= new AstQueryJoinService(
            resolveSource: ResolveSource,
            buildMySqlIndexHintPlan: AstQueryIndexHelper.BuildMySqlIndexHintPlan,
            evalJoinValue: (expr, row, ctes) => Eval(expr, row, group: null, ctes),
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
            AstQueryRowSourceHelper.CreateSourceEvalRow);

    private AstQueryHavingHelper HavingHelper
        => _havingHelper ??= new AstQueryHavingHelper(
            () => context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para HAVING."),
            ParseExpr,
            SelectAliasParserHelper.SplitTrailingAsAlias);

    private AstQueryPartitionHelper PartitionHelper
        => _partitionHelper ??= new AstQueryPartitionHelper(
            expr =>
            {
                switch (expr)
                {
                    case LiteralExpr l:
                        return (true, l.Value);
                    case ParameterExpr p:
                        if (TryResolveLocalFunctionValue(p.Name, out var localValue))
                            return (true, localValue);
                        return (true, QueryRowValueHelper.ResolveParam(_context, p.Name));
                    default:
                        return (false, null);
                }
            });

    private AstQuerySubqueryComparisonEvaluator SubqueryComparisonEvaluator
        => _subqueryComparisonEvaluator ??= new AstQuerySubqueryComparisonEvaluator(
            _subqueryEvaluationCache,
            _context,
            Eval,
            ParseExpr,
            (tableSource, scope) => ResolveSource(tableSource, scope),
            BuildFrom,
            AttachOuterRow,
            (select, scope, outerRow) => ExecuteSelect(select, scope, outerRow),
            PartitionHelper,
            IndexHelper,
            BuildCorrelatedExistsPatternSource,
            GetOrEvaluateSubqueryFirstColumnValuesForOperation);

    private Source BuildCorrelatedExistsPatternSource(
        string cacheKey,
        SqlTableSource tableSource,
        IDictionary<string, Source> ctes)
    {
        var physical = _resolvedBaseTableCache.GetOrAdd(
            cacheKey,
            _ =>
            {
                if (_context.Connection.TryGetTable(tableSource.Name!, out var table, tableSource.DbName)
                    && table is not null)
                {
                    _context.Connection.Metrics.IncrementTableHint(tableSource.Name!.NormalizeName());
                    return table;
                }

                return _context.Connection.GetTable(tableSource.Name!, tableSource.DbName);
            });

        return Source.FromPhysical(
            tableSource.Name!.NormalizeName(),
            tableSource.Alias ?? tableSource.Name!,
            physical,
            tableSource.MySqlIndexHints,
            tableSource.PartitionNames);
    }

    private AstQueryIndexHelper IndexHelper
        => _indexHelper ??= new AstQueryIndexHelper(
            collectColumnEqualities: (where, src) => PartitionHelper.TryCollectColumnEqualities(where, src, out var equalities) ? equalities : null,
            incrementIndexLookupMetric: () =>
            {
                if (Cnn.Metrics.Enabled)
                    Cnn.Metrics.IndexLookups++;
            },
            incrementIndexHintMetric: indexName =>
            {
                if (Cnn.Metrics.Enabled)
                    Cnn.Metrics.IncrementIndexHint(indexName);
            },
            recordPrimaryKeyHintMetric: TryRecordPrimaryKeyHintMetric);

    private AstQueryFunctionEvaluator FunctionEvaluator
        => _functionEvaluator ??= new AstQueryFunctionEvaluator(
            isAggregateFunction: AggregateFunctionCatalog.Contains,
            evalAggregate: (fn, group, ctes) => _context.EvalAggregate(fn, group, ctes, Eval),
            tryEvalUserDefinedScalarFunction: TryEvalUserDefinedScalarFunction,
            tryEvalBoundScalarFunction: TryEvalBoundScalarFunction,
            tryEvalNonSqlServerScalarFunctionFamily: TryEvalNonSqlServerScalarFunctionFamily,
            tryEvalSqlServerAndCompatibilityFunctionFamily: TryEvalSqlServerAndCompatibilityFunctionFamily,
            tryEvalGeneralScalarFunctionFamily: TryEvalGeneralScalarFunctionFamily,
            tryEvalCastStringAndDateTail: TryEvalCastStringAndDateTail);

    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => _context.EvalCase(c, row, group, ctes, Eval);

    private object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => _context.EvalFunction(
            fn,
            row,
            group,
            ctes,
            _context.EvaluationLocalNow,
            _context.EvaluationUtcNow,
            i => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null,
            FunctionEvaluator);

    internal static bool TryConvertNumericToDouble(object? value, out double result)
        => AstQueryBinaryArithmeticHelper.TryConvertNumericToDouble(value, out result);

    internal static bool TryConvertNumericToDecimal(object? value, out decimal result)
        => AstQueryBinaryArithmeticHelper.TryConvertNumericToDecimal(value, out result);

    private static bool TryEvalBoundScalarFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => context.TryEvalBoundScalarFunction(fn, evalArg, out result);

    private bool TryEvalNonSqlServerScalarFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit =
            (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
                => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

        if (_context.TryEvaluate(
            fn,
            row,
            group,
            ctes,
            evalArg,
            Eval,
            getTemporalUnit,
            TryConvertNumericToInt64,
            TryConvertNumericToDouble,
            TryCoerceDateTime,
            TryParseExactCachedDateTime,
            out result))
        {
            return true;
        }

        if (AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, getTemporalUnit, AstQueryExecutionRuntimeHelper.ResolveTemporalUnit, out result))
        {
            return true;
        }

        if (AstQueryFirebirdContextFunctionEvaluator.TryEvaluate(_context, fn, evalArg, out result))
        {
            return true;
        }

        if (AstQueryOracleDb2ScalarFunctionEvaluator.TryEvaluate(_context, fn, evalArg, TryCoerceDateTime, out result)
            || AstQueryPostgresScalarFunctionEvaluator.TryEvaluateyPostgresScalarFunction(_context, fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalSqlServerAndCompatibilityFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => SqlServerCompatibilityFunctionEvaluator.TryEvaluate(context, fn, row, group, ctes, evalArg, out result);

    private bool TryEvalGeneralScalarFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => context.TryEvaluate(fn, row, group, ctes, AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction, evalArg, Eval, ParseIntervalValue, out result);

    private bool TryEvalCastStringAndDateTail(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => CastStringAndDateTailEvaluator.TryEvaluate(context, fn, row, group, ctes, evalArg, out result);

    private bool TryEvalCastConversionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => CastConversionFamilyEvaluator.TryEvaluate(fn, context, evalArg, out result);

    private bool TryEvalUserDefinedScalarFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
        => _context.TryEvalUserDefinedScalarFunction(
            fn,
            row,
            group,
            ctes,
            _localParameterScopes,
            Eval,
            out result);

    private bool TryResolveLocalFunctionValue(string name, out object? value)
        => _context.TryResolveLocalFunctionValue(name, _localParameterScopes, out value);

    private AstQueryCastConversionFamilyEvaluator CastConversionFamilyEvaluator
        => _castConversionFamilyEvaluator ??= new AstQueryCastConversionFamilyEvaluator(
            tryEvalJsonAccessShimFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonAccessShimFunction,
            tryEvalJsonExtractionFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction,
            tryEvalSqlServerJsonModifyFunction: AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerJsonModifyFunction,
            tryEvalOpenJsonFunction: AstQuerySqlServerUtilityFunctionEvaluator.TryEvalOpenJsonFunction,
            tryEvalJsonUnquoteFunction: AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction,
            tryEvalToNumberFunction: AstQueryToNumberFunctionEvaluator.TryEvalToNumberFunction);

    private AstQueryCastStringAndDateTailEvaluator CastStringAndDateTailEvaluator
        => _castStringAndDateTailEvaluator ??= new AstQueryCastStringAndDateTailEvaluator(
            tryEvalCastConversionFamily: TryEvalCastConversionFamily,
            tryEvalCastConcatAndStringTail: AstQueryCastStringAndDateTailEvaluator.TryEvalCastConcatAndStringTail,
            tryEvalCastDateTail: (fn, row, group, ctes, context, evalArg, out result) =>
            {
                Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit =
                    (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
                        => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

                return AstQueryCastStringAndDateTailEvaluator.TryEvalCastDateTail(
                    fn,
                    row,
                    group,
                    ctes,
                    context,
                    evalArg,
                    Eval,
                    getTemporalUnit,
                    out result);
            });

    private AstQuerySqlServerDatabaseFunctionEvaluator SqlServerDatabaseFunctionEvaluator
        => _sqlServerDatabaseFunctionEvaluator ??= new AstQuerySqlServerDatabaseFunctionEvaluator(
            resolveDatabaseProperty: _context.TryResolveSqlServerDatabaseProperty,
            resolveDatabasePrincipalId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerDatabasePrincipalId,
            resolveColumnProperty: _context.TryResolveSqlServerColumnProperty,
            resolveColumnLength: _context.TryResolveSqlServerColumnLength,
            resolveColumnName: _context.TryResolveSqlServerColumnName,
            resolveObjectId: _context.TryResolveSqlServerObjectId,
            resolveObjectProperty: _context.TryResolveSqlServerObjectProperty,
            resolveObjectName: _context.TryResolveSqlServerObjectName,
            resolveObjectSchemaName: _context.TryResolveSqlServerObjectSchemaName,
            resolveTypeProperty: AstQuerySqlServerResolutionHelper.TryResolveSqlServerTypeProperty,
            getDatabaseName: () => Cnn.Database);

    private AstQuerySqlServerIdentityFunctionEvaluator SqlServerIdentityFunctionEvaluator
        => _sqlServerIdentityFunctionEvaluator ??= new AstQuerySqlServerIdentityFunctionEvaluator(
            getDialect: () => context.Dialect,
            getLastInsertId: Cnn.GetLastInsertId,
            resolveSystemTypeId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeId,
            resolveSystemTypeName: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeName);

    private AstQuerySqlServerUtilityFunctionEvaluator SqlServerUtilityFunctionEvaluator
        => _sqlServerUtilityFunctionEvaluator ??= new AstQuerySqlServerUtilityFunctionEvaluator(
            getDialect: () => context.Dialect,
            tryConvertNumericToDecimal: TryConvertNumericToDecimal,
            tryCoerceDateTime: TryCoerceDateTime,
            tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
            tryParseCachedDateTimeOffset: TryParseCachedDateTimeOffset);

    private AstQuerySqlServerSessionFunctionEvaluator SqlServerSessionFunctionEvaluator
        => _sqlServerSessionFunctionEvaluator ??= new AstQuerySqlServerSessionFunctionEvaluator(
            getDialect: () => context.Dialect,
            getContextInfo: Cnn.GetContextInfo,
            hasActiveTransaction: () => Cnn.HasActiveTransaction || _context.HasActiveTransaction,
            tryResolveSqlServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerRoleMembership,
            tryResolveSqlServerServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerServerRoleMembership);

    private AstQuerySqlServerCompatibilityFunctionEvaluator SqlServerCompatibilityFunctionEvaluator
        => _sqlServerCompatibilityFunctionEvaluator ??= new AstQuerySqlServerCompatibilityFunctionEvaluator(
            SqlServerSessionFunctionEvaluator,
            SqlServerDatabaseFunctionEvaluator,
            SqlServerIdentityFunctionEvaluator,
            SqlServerUtilityFunctionEvaluator,
            Eval,
            CreateTemporalUnitResolver(),
            AstQueryExecutionRuntimeHelper.ResolveTemporalUnit);

    private Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> CreateTemporalUnitResolver()
        => (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
            => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // Custom schema functions are resolved through the current connection when available.
    private SqlExpr ParseExpr(string raw)
    {
        var db = context.Connection.Db ?? throw new InvalidOperationException("Banco SQL não disponível para parse de expressão.");
        var dialect = context.Dialect ?? throw new InvalidOperationException("Dialecto SQL não disponível para parse de expressão.");
        return SqlExpressionParser.ParseWhere(
            raw,
            db,
            dialect,
            null,
            customFunctionSupported: name => Cnn.TryGetFunction(name, out _));
    }

    private SqlExpr ParseScalarExpr(string raw)
    {
        var db = context.Connection.Db ?? throw new InvalidOperationException("Banco SQL não disponível para parse de expressão.");
        var dialect = context.Dialect ?? throw new InvalidOperationException("Dialecto SQL não disponível para parse de expressão.");
        return SqlExpressionParser.ParseScalar(
            raw,
            db,
            dialect,
            _pars,
            customFunctionSupported: name => Cnn.TryGetFunction(name, out _));
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
        return _context.ExecuteUnion(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            sqlContextForErrors,
            parts1 => ExecuteSelect(parts1, null, null),
            (result, query, ctes, trace) => context.ApplyQueryOrderLimit(
                result,
                query,
                ctes,
                ParseExpr,
                (expr, row) => Eval(expr, row, group: null, ctes),
                (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
                trace),
            AstQueryPlanMetricsHelper.CountKnownInputTables
            );
    }

    /// <summary>
    /// EN: Implements ExecuteSelect.
    /// PT: Implementa ExecuteSelect.
    /// </summary>
    public TableResultMock ExecuteSelect(SqlSelectQuery q)
    {
        var sw = Stopwatch.StartNew();
        ClearSubqueryEvaluationCaches();
        QueryDebugTraceBuilder? debugTrace = Cnn.IsDebugTraceCaptureEnabled
            ? new QueryDebugTraceBuilder(SqlConst.SELECT)
            : null;
        var hasSqlCalcFoundRows = HasSqlCalcFoundRows(q);
        var result = ExecuteSelect(q, null, null, debugTrace);
        sw.Stop();

        if (!hasSqlCalcFoundRows)
            Cnn.SetLastSelectRows(result.Count);

        var metrics = _context.BuildPlanRuntimeMetrics(q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(_context, q, metrics);
        var planWarnings = QueryPlanWarningHelper.BuildPlanWarnings(q, metrics);
        var runtimeContext = _context.BuildPlanRuntimeContext();
        if (Cnn.Db.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatSelect(
                q,
                metrics,
                indexRecommendations,
                planWarnings,
                runtimeContext: runtimeContext);
            result.ExecutionPlan = plan;
            Cnn.RegisterExecutionPlan(plan);
        }
        if (debugTrace is not null)
            Cnn.RegisterDebugTrace(debugTrace.Build());
        return result;
    }

    private TableResultMock ExecuteSelect(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(selectQuery, nameof(selectQuery));

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

        if (TryEvaluateSimpleUnionCount(selectQuery, ctes, outerRow, out var fastCountResult))
            return fastCountResult;

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
                (int)Math.Min(int.MaxValue, _context.GetKnownSourceRows(selectQuery.Table)),
                fromRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(fromStart)),
                SqlSourceFormattingHelper.FormatSource(selectQuery.Table));
            rows = fromRows;
        }

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
                    $"Join({AstQuerySelectExecutionHelper.FormatJoinTypeForDebug(j.Type)})",
                    inputRows,
                    joinedRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(joinStart)),
                    SqlSourceFormattingHelper.FormatJoinDebugDetails(j));
                rows = joinedRows;
            }
        }

        if (outerRow is not null)
            rows = AttachOuterRows(rows, outerRow);

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

        var needsGrouping = selectQuery.GroupBy.Count > 0
            || selectQuery.Having is not null
            || AstQueryAggregateAnalysisHelper.ContainsAggregate(
                selectQuery,
                ParseScalarExpr,
                AggregateExpressionInspector.WalkHasAggregate);
        if (needsGrouping)
        {
            var groupedRows = rows as List<EvalRow> ?? [.. rows];
            if (debugTrace is null && TryEvaluateSimpleStringAggregate(selectQuery, groupedRows, ctes, out var fastStringAggregateResult))
                return fastStringAggregateResult;

            return ExecuteGroup(selectQuery, ctes, groupedRows, debugTrace);
        }

        var projectedRows = rows as List<EvalRow> ?? [.. rows];
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var projected = ProjectRows(selectQuery, projectedRows, ctes);
        debugTrace?.AddStep(
            "Project",
            projectedRows.Count,
            projected.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(selectQuery.SelectItems));

        if (selectQuery.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = projected.Count;
            projected = _context.ApplyDistinct(projected);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            Cnn.SetLastSelectRows(projected.Count);

        projected = context.ApplyQueryOrderLimit(
            projected,
            selectQuery,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            debugTrace);
        projected = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
        return projected;
    }

    private bool TryEvaluateSimpleUnionCount(
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

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseScalarCountAggregate(exprRaw, ParseExpr, out var countArg, out var isCountBig) || countArg is not StarExpr)
            return false;

        var union = query.Table.DerivedUnion;
        if (union.RowLimit is not null
            || union.Parts.Count != 2
            || union.AllFlags.Count != 1)
            return false;

        if (union.AllFlags[0])
        {
            long allCount = 0;
            foreach (var part in union.Parts)
            {
                if (!TryCountSimpleRows(part, ctes, outerRow, out var partCount))
                    return false;

                allCount += partCount;
            }

            return CreateSimpleUnionCountResult(query, exprRaw, isCountBig, allCount, out result);
        }

        if (!TryCountSimpleRows(union.Parts[0], ctes, outerRow, out _)
            || !TryCountSimpleRows(union.Parts[1], ctes, outerRow, out _))
            return false;

        var leftRows = ExecuteSelect(union.Parts[0], ctes, outerRow);
        var rightRows = ExecuteSelect(union.Parts[1], ctes, outerRow);
        if (leftRows.Columns.Count != rightRows.Columns.Count)
            return false;

        var seenRows = new HashSet<Dictionary<int, object?>>(new SqlRowDictionaryComparer(context));
        long distinctCount = 0;
        for (var i = 0; i < leftRows.Count; i++)
        {
            if (seenRows.Add(leftRows[i]))
                distinctCount++;
        }

        for (var i = 0; i < rightRows.Count; i++)
        {
            if (seenRows.Add(rightRows[i]))
                distinctCount++;
        }

        return CreateSimpleUnionCountResult(query, exprRaw, isCountBig, distinctCount, out result);
    }

    private bool CreateSimpleUnionCountResult(
        SqlSelectQuery query,
        string exprRaw,
        bool isCountBig,
        long count,
        out TableResultMock result)
    {
        var tableAlias = query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty;
        var columnAlias = SelectPlanProjectionHelper.InferColumnAlias(exprRaw);
        var countValue = AstQueryAggregateEvaluator.CreateCountAggregateResult(context, isCountBig, count);
        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    tableAlias,
                    columnAlias,
                    0,
                    countValue is int ? DbType.Int32 : DbType.Int64,
                    isNullable: false)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = countValue });
        result.JoinFields.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        if (query.OrderBy.Count > 0 || query.RowLimit is not null)
        {
            var orderCtes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = context.ApplyQueryOrderLimit(
                result,
                query,
                orderCtes,
                ParseExpr,
                (expr, row) => Eval(expr, row, group: null, orderCtes),
                (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture));
        }

        return true;
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
            return false;

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseStringAggregateCall(exprRaw, ParseScalarExpr, out var aggregateCall))
            return false;

        var aggregateDefinition = aggregateCall.ResolvedScalarFunction;
        if (aggregateDefinition is null
            && !context.Dialect.TryGetScalarFunctionDefinition(aggregateCall, out aggregateDefinition))
            return false;

        if (aggregateDefinition is not null
            && !aggregateDefinition.AllowsCall)
            return false;

        if (aggregateDefinition is null)
            return false;

        if (aggregateCall.Distinct)
            return false;

        var firstRow = rows.Count > 0 ? rows[0] : EvalRow.Empty();
        var aggregateGroup = new EvalGroup(rows);
        object? resultValue;
        using (var positionalScope = context.BeginPositionalParameterScope())
        {
            resultValue = context.EvalAggregate(aggregateCall, aggregateGroup, ctes, Eval);
        }

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
            Cnn.SetLastSelectRows(result.Count);

        if (query.Distinct)
            result = _context.ApplyDistinct(result);

        result = context.ApplyQueryOrderLimit(
            result,
            query,
            ctes,
            ParseExpr,
            (expr, row) =>
            {
                using var positionalScope = _context.BeginPositionalParameterScope();
                return Eval(expr, row, group: null, ctes);
            },
            (expr, scope) =>
            {
                using var positionalScope = _context.BeginPositionalParameterScope();
                return Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture);
            });
        result = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(result, query);
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
            return false;

        if (outerRow is null && query.Where is null)
        {
            if (query.Table is null)
            {
                count = 1;
                return true;
            }

            if (AstQueryPlanMetricsHelper.HasKnownPhysicalTable(query.Table))
            {
                count = _context.GetKnownSourceRows(query.Table);
                return true;
            }
        }

        if (outerRow is null
            && query.Table is not null
            && query.Where is not null
            && TryCountRowsFromPrimaryKey(query, ctes, out count))
            return true;

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
                using var positionalScope = _context.BeginPositionalParameterScope();
                if (Eval(query.Where, AttachOuterRow(candidate, outerRow), group: null, ctes).ToBool())
                    count++;
            }

            return true;
        }

        foreach (var candidate in rows)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
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
        if (!IndexHelper.TryCountRowsFromIndex(src, query.Table, query.Where, hasOrderBy: query.OrderBy.Count > 0, hasGroupBy: false, out count))
            return false;

        return true;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var sourceRows = rows as List<EvalRow> ?? [.. rows];
        var keyExprs = AstQuerySelectGroupKeyHelper.BuildGroupByKeyExpressions(q, ParseExpr);

        GroupKey BuildGroupKey(EvalRow row)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            var values = new object?[keyExprs.Length];
            for (var i = 0; i < keyExprs.Length; i++)
                values[i] = Eval(keyExprs[i], row, group: null, ctes);

            return new GroupKey(values);
        }

        var groupStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var grouped = MaterializeGroups(sourceRows.GroupBy(
            BuildGroupKey,
            new GroupKey.GroupKeyComparer(context)));
        debugTrace?.AddStep(
            "Group",
            sourceRows.Count,
            grouped.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(groupStart)),
            QueryDebugTraceFormattingHelper.FormatGroupDebugDetails(q));

        if (q.Having is null)
            return ProjectGrouped(q, grouped, ctes, debugTrace);

        var aliasExprs = new List<(string Alias, SqlExpr Ast)>(q.SelectItems.Count);
        for (var i = 0; i < q.SelectItems.Count; i++)
        {
            var selectItem = q.SelectItems[i];
            var (exprRaw, alias) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
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

        return ProjectGrouped(q, grouped, ctes, debugTrace);
    }

    private IEnumerable<EvalRow> ApplyRowPredicate(
        IEnumerable<EvalRow> rows,
        SqlExpr predicate,
        IDictionary<string, Source> ctes)
    {
        foreach (var row in rows)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            if (Eval(predicate, row, group: null, ctes).ToBool())
                yield return row;
        }
    }

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
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            var firstEvalCtx = BuildHavingEvaluationContext(firstGroup, aliasExprs, ctes, out var firstEvalGroup);
            HavingHelper.EnsureHavingIdentifiersAreBound(havingExpr, firstEvalCtx, context.Dialect!);
            if (Eval(havingExpr, firstEvalCtx, firstEvalGroup, ctes).ToBool())
                filtered.Add(firstGroup);
        }

        for (var i = 1; i < grouped.Count; i++)
        {
            var group = grouped[i];
            using var positionalScope = _context.BeginPositionalParameterScope();
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

    private IEnumerable<EvalRow> BuildFrom(
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (from is null)
        {
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
            yield return AstQueryRowSourceHelper.CreateSourceEvalRow(src, r);
    }

    private void TryRecordPrimaryKeyHintMetric(
        ITableMock table,
        MySqlIndexHintPlan? hintPlan)
    {
        if (hintPlan is null || !Cnn.Metrics.Enabled)
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
            Cnn.Metrics.IncrementIndexHint(hintedPrimaryEquivalent!);
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
        var selectPlan = _context.BuildSelectPlan(
            q,
            rows,
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        context.ComputeWindowSlots(
            Eval,
            selectPlan.WindowSlots,
            rows,
            ctes);

        var columnCount = selectPlan.Columns.Count;
        var projectedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        foreach (var r in rows)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
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

        var selectPlan = _context.BuildSelectPlan(
            q,
            representativeRows,
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        var columnCount = selectPlan.Columns.Count;
        var groupedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        foreach (var g in groupsList)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            var eg = new EvalGroup(g.Rows);
            var outRow = new Dictionary<int, object?>(groupedColumnCount);

            var first = g.Rows.Count > 0 ? g.Rows[0] : EvalRow.Empty();
            for (int i = 0; i < groupedColumnCount; i++)
            {
                var value = selectPlan.Evaluators[i](first, eg);
                outRow[i] = value;
            }

            res.Add(outRow);
            res.JoinFields.Add(first.Fields);
        }

        if (q.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = res.Count;
            res = _context.ApplyDistinct(res);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(q.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(q))
            Cnn.SetLastSelectRows(res.Count);

        // ORDER / LIMIT
        debugTrace?.AddStep(
            "Project",
            groupsList.Count,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(q.SelectItems));
        res = _context.ApplyQueryOrderLimit(
            res,
            q,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            debugTrace);
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

        var extract = new FunctionCallExpr("JSON_EXTRACT", [ja.Target, ja.Path])
            .BindScalarFunctionDefinition(_context.Dialect);
        return ja.Unquote
            ? new FunctionCallExpr("JSON_UNQUOTE", [extract])
                .BindScalarFunctionDefinition(_context.Dialect)
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
        var cacheKey = AstQuerySubqueryLookupSupport.BuildCorrelatedSubqueryCacheKey(SqlConst.SCALAR, sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddScalar(
            cacheKey,
            _ =>
            {
                var query = sq.Parsed ?? throw new InvalidOperationException(
                    "EVAL subquery: SubqueryExpr sem AST parseado (Parsed vazio).");
                if (TryEvaluateScalarSubqueryFast(query, row, ctes, out var fastValue))
                    return fastValue;

                var r = ExecuteSelect(AstQuerySubqueryLookupSupport.LimitToSingleRow(query), ctes, row);
                return r.Count > 0 && r[0].TryGetValue(0, out var v) ? v : null;
            });
    }

    private bool TryEvaluateScalarSubqueryFast(
        SqlSelectQuery query,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out object? value)
        => SubqueryLookupEvaluator.TryEvaluateScalarSubqueryFast(query, row, ctes, out value);

    private bool TryCountRowsFromSimpleEqualityScan(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out long count)
        => SubqueryLookupEvaluator.TryCountRowsFromSimpleEqualityScan(query, ctes, out count);

    private InSubqueryLookupState GetOrEvaluateInSubqueryLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => SubqueryLookupEvaluator.GetOrEvaluateInSubqueryLookup(sq, row, ctes);

    private InSubqueryLookupState GetOrEvaluateInSubqueryRowLookup(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => SubqueryLookupEvaluator.GetOrEvaluateInSubqueryRowLookup(sq, row, ctes);

    private List<object?>? GetOrEvaluateSubqueryFirstColumnValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => SubqueryLookupEvaluator.GetOrEvaluateSubqueryFirstColumnValuesForOperation(sq, operation, row, ctes);

    private List<object?[]> GetOrEvaluateSubqueryRowValuesForOperation(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => SubqueryLookupEvaluator.GetOrEvaluateSubqueryRowValuesForOperation(sq, operation, row, ctes);

    private void ClearSubqueryEvaluationCaches()
        => SubqueryLookupEvaluator.Clear();

    private bool TryEvaluateFirebirdTemporalLiteral(object? value, out object? result)
    {
        result = null;
        if (value is not string text
            || !_context.Dialect.Name.Equals("firebird", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        result = text.Trim().ToUpperInvariant() switch
        {
            "NOW" => _context.EvaluationLocalNow,
            "TODAY" => _context.EvaluationLocalNow.Date,
            "TOMORROW" => _context.EvaluationLocalNow.Date.AddDays(1),
            "YESTERDAY" => _context.EvaluationLocalNow.Date.AddDays(-1),
            _ => null
        };

        return result is not null;
    }

    // ---------------- EXPRESSION EVAL ----------------

    private object? Eval(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var topLevelEval = _evalDepth++ == 0;
        if (topLevelEval && !_context.HasPositionalParameterScope)
            _context.ResetPositionalParameterCursor();

        try
        {
        switch (expr)
        {
            case LiteralExpr l:
                return TryEvaluateFirebirdTemporalLiteral(l.Value, out var firebirdTemporalLiteralValue)
                    ? firebirdTemporalLiteralValue
                    : l.Value;

            case ParameterExpr p:
                if (TryResolveLocalFunctionValue(p.Name, out var localParameterValue))
                    return localParameterValue;
                return QueryRowValueHelper.ResolveParam(_context, p.Name);

            case IdentifierExpr id:
                if (TryResolveLocalFunctionValue(id.Name, out var localIdentifierValue))
                    return localIdentifierValue;

                return EvalIdentifier(id, row);

            case ColumnExpr col:
                return QueryRowValueHelper.ResolveColumn(col.Qualifier, col.Name, row);

            case StarExpr:
                // only meaningful inside COUNT(*)
                return "*";

            case IsNullExpr isn:
                return EvalIsNull(isn, row, group, ctes);

            case LikeExpr like:
                return _context.EvalLike(like, row, group, ctes, Eval);

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
                return AstQueryExpressionEvaluationHelper.EvalNot(
                    u,
                    row,
                    group,
                    ctes,
                    Eval,
                    (InExpr a, EvalRow b, EvalGroup? c, IDictionary<string, Source> d)
                        => _context.EvalNotIn(
                            a,
                            b,
                            c,
                            d,
                            Eval,
                            GetOrEvaluateInSubqueryLookup,
                            GetOrEvaluateInSubqueryRowLookup)
                    );

            case BinaryExpr b:
                return _context.EvalBinary(
                    b,
                    row,
                    group,
                    ctes,
                    Eval,
                    SubqueryComparisonEvaluator.TryEvaluateCorrelatedCountComparisonFast);

            case InExpr i:
                return _context.EvalIn(
                    i,
                    row,
                    group,
                    ctes,
                    Eval,
                    GetOrEvaluateInSubqueryLookup,
                    GetOrEvaluateInSubqueryRowLookup);

            case ExistsExpr ex:
                return SubqueryComparisonEvaluator.EvalExists(ex, row, ctes);

            case QuantifiedComparisonExpr qc:
                return SubqueryComparisonEvaluator.EvalQuantifiedComparison(qc, row, group, ctes);


            case CaseExpr c:
                if (ShouldTraceGroupedCaseWhenCase(c))
                {
                    Console.WriteLine(
                        $"[CaseDebug][ast] base={(c.BaseExpr is null ? "NULL" : c.BaseExpr.GetType().Name)} whenCount={c.Whens.Count} else={(c.ElseExpr is null ? "NULL" : c.ElseExpr.GetType().Name)}");
                }
                return EvalCase(c, row, group, ctes);

            case JsonAccessExpr ja:
                return AstQueryExpressionEvaluationHelper.EvalJsonAccess(
                    ja,
                    row,
                    group,
                    ctes,
                    MapJsonAccess,
                    Eval);
            case FunctionCallExpr fn:
                return EvalFunction(fn, row, group, ctes);
            case CallExpr ce:
                return EvalCall(ce, row, group, ctes);
            case BetweenExpr b:
                return AstQueryExpressionEvaluationHelper.EvalBetween(b, row, group, ctes, Eval);
            case SubqueryExpr sq:
                return EvalScalarSubquery(sq, ctes, row);
            case RowExpr re:
                return AstQueryExpressionEvaluationHelper.EvalRowExpression(re, row, group, ctes, Eval);

            case RawSqlExpr:
                // unsupported expression (e.g. CAST(x AS CHAR)): best-effort: null
                return null;

            default:
                throw new InvalidOperationException($"Expr não suportada no executor: {expr.GetType().Name}");
        }
        }
        finally
        {
            _evalDepth--;
        }
    }

    private static bool ShouldTraceGroupedCaseWhenCase(CaseExpr c)
        => ContainsParameter(c, "cutoff");

    private static bool ContainsParameter(SqlExpr expression, string parameterName)
        => expression switch
        {
            ParameterExpr parameter => parameter.Name.TrimStart('@', ':', '?')
                .Equals(parameterName, StringComparison.OrdinalIgnoreCase),
            BinaryExpr binary => ContainsParameter(binary.Left, parameterName) || ContainsParameter(binary.Right, parameterName),
            UnaryExpr unary => ContainsParameter(unary.Expr, parameterName),
            CaseExpr caseExpr => (caseExpr.BaseExpr is not null && ContainsParameter(caseExpr.BaseExpr, parameterName))
                || caseExpr.Whens.Any(when => ContainsParameter(when.When, parameterName) || ContainsParameter(when.Then, parameterName))
                || (caseExpr.ElseExpr is not null && ContainsParameter(caseExpr.ElseExpr, parameterName)),
            FunctionCallExpr functionCall => functionCall.Args.Any(arg => ContainsParameter(arg, parameterName)),
            CallExpr call => call.Args.Any(arg => ContainsParameter(arg, parameterName)),
            LikeExpr likeExpr => ContainsParameter(likeExpr.Left, parameterName)
                || ContainsParameter(likeExpr.Pattern, parameterName)
                || (likeExpr.Escape is not null && ContainsParameter(likeExpr.Escape, parameterName)),
            InExpr inExpr => ContainsParameter(inExpr.Left, parameterName)
                || inExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            IsNullExpr isNullExpr => ContainsParameter(isNullExpr.Expr, parameterName),
            BetweenExpr betweenExpr => ContainsParameter(betweenExpr.Expr, parameterName)
                || ContainsParameter(betweenExpr.Low, parameterName)
                || ContainsParameter(betweenExpr.High, parameterName),
            RowExpr rowExpr => rowExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            _ => false
        };

    private static string FormatDebugValue(object? value)
        => value is null or DBNull
            ? "NULL"
            : $"{value} ({value.GetType().Name})";

    private object? EvalIdentifier(IdentifierExpr identifier, EvalRow row)
    {
        if (QueryRowValueHelper.TryResolveIdentifier(identifier.Name, row, out var resolvedColumn))
        {
            return resolvedColumn;
        }

        if (_context.TryResolveIdentifier(
                identifier,
                _context.EvaluationLocalNow,
                _context.EvaluationUtcNow,
                Cnn,
                out var resolved))
        {
            return resolved;
        }

        return null;
    }

    private object? EvalIsNull(
        IsNullExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var isNull = expression.Expr switch
        {
            JsonAccessExpr jsonAccess => EvalJsonAccessIsNull(jsonAccess, row, group, ctes),
            _ => IsNullish(Eval(expression.Expr, row, group, ctes))
        };
        return expression.Negated ? !isNull : isNull;
    }

    private bool EvalJsonAccessIsNull(
        JsonAccessExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var json = Eval(expression.Target, row, group, ctes);
        if (json is null or DBNull)
            return true;

        if (json is JsonElement element && element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return true;

        if (json is JsonDocument document && document.RootElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return true;

        var path = Eval(expression.Path, row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(path))
            return true;

        var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json, path!);
        return IsNullish(value);
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

    internal static byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes)
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
            return _context.EvalAggregate(
                fn,
                group,
                ctes,
                Eval);

        if (fn.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return ParseIntervalValue(fn, row, group, ctes);

        if (TryEvalUserDefinedScalarFunction(
            new FunctionCallExpr(fn.Name, fn.Args, fn.Distinct),
            row,
            group,
            ctes,
            out var userDefinedResult))
        {
            return userDefinedResult;
        }

        // se não for agregado, trata como função "normal" reaproveitando EvalFunction
        // (Distinct em função escalar não faz sentido aqui, então ignoramos)
        var shim = fn.ResolvedScalarFunction is not null
            ? new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(fn.ResolvedScalarFunction)
            : new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(
                _context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para função escalar."));
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

        unit = AstQueryExecutionRuntimeHelper.GetTemporalUnit(fn.Args[1], row, group, ctes, Eval);
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

        unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    internal static DateTime ApplyDateDelta(DateTime dt, TemporalUnit unit, int amount) => unit switch
    {
        TemporalUnit.Year => dt.AddYears(amount),
        TemporalUnit.Month => dt.AddMonths(amount),
        TemporalUnit.Week => dt.AddDays(amount * 7L),
        TemporalUnit.Day => dt.AddDays(amount),
        TemporalUnit.Weekday => dt.AddDays(amount),
        TemporalUnit.Yearday => dt.AddDays(amount),
        TemporalUnit.Hour => dt.AddHours(amount),
        TemporalUnit.Minute => dt.AddMinutes(amount),
        TemporalUnit.Second => dt.AddSeconds(amount),
        TemporalUnit.Millisecond => dt.AddMilliseconds(amount),
        TemporalUnit.Microsecond => dt.AddTicks(amount * 10L),
        TemporalUnit.Nanosecond => dt.AddTicks((long)Math.Round(amount / 100m, MidpointRounding.AwayFromZero)),
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
        TemporalUnit.Millisecond => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond, dateTime.Kind),
        TemporalUnit.Microsecond => new DateTime(dateTime.Ticks - (dateTime.Ticks % 10), dateTime.Kind),
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
        TemporalUnit.Millisecond => dateTime.Millisecond,
        TemporalUnit.Microsecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L),
        TemporalUnit.Nanosecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L),
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

        unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    private static TimeSpan? TryConvertIntervalToTimeSpan(decimal value, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Day => TimeSpan.FromDays((double)value),
            TemporalUnit.Hour => TimeSpan.FromHours((double)value),
            TemporalUnit.Minute => TimeSpan.FromMinutes((double)value),
            TemporalUnit.Second => TimeSpan.FromSeconds((double)value),
            TemporalUnit.Microsecond => TimeSpan.FromTicks((long)decimal.Truncate(value * 10m)),
            TemporalUnit.Nanosecond => TimeSpan.FromTicks((long)Math.Round(value / 100m, MidpointRounding.AwayFromZero)),
            _ => (TimeSpan?)null
        };

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

    private object? EvalAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => _context.EvalAggregate(
            fn,
            group,
            ctes,
            Eval);


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
            if (_result is not null)
            {
                if (_resultColumnMetadataLookup is not null
                    && _resultColumnMetadataLookup.TryGetValue(columnName, out metadata))
                {
                    return true;
                }

                return false;
            }

            if (Physical is null)
                return false;

            if (Physical.Columns.TryGetValue(columnName, out var physicalColumn))
            {
                metadata = new TableResultColMock(
                    tableAlias: Alias,
                    columnAlias: columnName,
                    columnName: columnName,
                    columIndex: physicalColumn.Index,
                    dbType: physicalColumn.DbType,
                    isNullable: physicalColumn.Nullable,
                    isJsonFragment: false);
                return true;
            }

            var dotIndex = columnName.LastIndexOf('.');
            if (dotIndex >= 0 && dotIndex + 1 < columnName.Length)
            {
                var unqualifiedColumnName = columnName[(dotIndex + 1)..];
                if (Physical.Columns.TryGetValue(unqualifiedColumnName, out physicalColumn))
                {
                    metadata = new TableResultColMock(
                        tableAlias: Alias,
                        columnAlias: unqualifiedColumnName,
                        columnName: unqualifiedColumnName,
                        columIndex: physicalColumn.Index,
                        dbType: physicalColumn.DbType,
                        isNullable: physicalColumn.Nullable,
                        isJsonFragment: false);
                    return true;
                }
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
            if (physical is TableMock tableMock)
            {
                var pkIndexes = tableMock.PkIndexArray;
                if (pkIndexes.Length != 1)
                    return false;

                columnName = tableMock.GetColumnByIndex(pkIndexes[0]).Name;
                return true;
            }

            var primaryKeyIndexes = physical.PrimaryKeyIndexes;
            if (primaryKeyIndexes.Count != 1)
                return false;

            var pkIndex = default(int);
            foreach (var candidatePkIndex in primaryKeyIndexes)
            {
                pkIndex = candidatePkIndex;
                break;
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
                        if (_sourceQualifiedColumnNames is not null)
                            dict[_sourceQualifiedColumnNames[i]] = dict[_qualifiedColumnNames[i]];
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
                        if (_sourceQualifiedColumnNames is not null)
                            dict[_sourceQualifiedColumnNames[i]] = dict[_resultQualifiedColumnNames[i]];
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

        internal long CountRowsByIndexes(IEnumerable<int> indexes)
        {
            if (Physical is null)
                return Rows().Count();

            var emitted = new HashSet<int>();
            var count = 0L;
            foreach (var raw in indexes)
            {
                if (raw < 0 || raw >= Physical.Count)
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

                count++;
            }

            return count;
        }

        internal long CountRowsByIndex(int index)
        {
            if (Physical is null)
                return Rows().Count();

            if (index < 0 || index >= Physical.Count)
                return 0;

            var row = Physical[index];
            if (_requestedPartitionNames is { Count: > 0 } requestedPartitions
                && Physical is TableMock table
                && !table.MatchesRequestedPartitions(row, requestedPartitions))
            {
                return 0;
            }

            return 1;
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
    internal enum TemporalUnit { Unknown, Year, Month, Day, Week, Weekday, Yearday, Hour, Minute, Second, Millisecond, Microsecond, Nanosecond }

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
        ["WEEK"] = TemporalUnit.Week,
        ["WEEKDAY"] = TemporalUnit.Weekday,
        ["YEARDAY"] = TemporalUnit.Yearday,
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
        ["S"] = TemporalUnit.Second,
        ["MILLISECOND"] = TemporalUnit.Millisecond,
        ["MICROSECOND"] = TemporalUnit.Microsecond,
        ["MICROSECONDS"] = TemporalUnit.Microsecond,
        ["MCS"] = TemporalUnit.Microsecond
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
        internal IReadOnlyList<KeyValuePair<string, object?>>? CorrelatedCacheFields { get; set; }
        internal Dictionary<string, IReadOnlyList<KeyValuePair<string, object?>>>? CorrelatedCacheFieldViews { get; set; }
        internal Dictionary<string, string>? CorrelatedCacheKeys { get; set; }
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
                var sourceQualifiedName = source.GetSourceQualifiedColumnName(i);
                if (sourceQualifiedName is not null)
                    targetFields.TryAdd(sourceQualifiedName, value);

                ordinalValues[ordinalIndex] = value;
                ordinalIndexes.TryAdd(qualifiedName, ordinalIndex);
                ordinalIndexes.TryAdd(columnName, ordinalIndex);
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

    /// <summary>
    /// EN: Implements EvalGroup.
    /// PT: Implementa EvalGroup.
    /// </summary>
    internal sealed class EvalGroup(List<EvalRow> rows)
    {
        /// <summary>
        /// EN: Gets or sets Rows.
        /// PT: Obtém ou define Rows.
        /// </summary>
        public List<EvalRow> Rows { get; } = rows;
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
        => _context.Dialect?.SupportsSqlCalcFoundRowsModifier == true
           && !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}
