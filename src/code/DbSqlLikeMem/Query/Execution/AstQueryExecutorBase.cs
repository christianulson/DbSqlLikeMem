namespace DbSqlLikeMem;

/// <summary>
/// EN: Executes the Pratt-based AST (<see cref="SqlSelectQuery"/>) against <see cref="TableMock"/> tables.
/// PT-br: Executa o AST baseado em Pratt (<see cref="SqlSelectQuery"/>) contra tabelas <see cref="TableMock"/>.
///
/// EN: The executor currently covers SELECT and WITH queries only, matching the scope of <see cref="SqlQueryParser"/>.
/// PT-br: O executor atualmente cobre apenas consultas SELECT e WITH, acompanhando o escopo de <see cref="SqlQueryParser"/>.
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
    internal static readonly object _uuidShortCounterLock = new();
    internal static long _uuidShortCounter;

    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private DbConnectionMockBase Cnn => _context.Connection;

    private IDataParameterCollection _pars => _context.DbParameters;
    private const int ParsedExprCacheMaxSize = 512;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ITableMock> _resolvedBaseTableCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SqlExpr> _parsedExprCache = new(StringComparer.Ordinal);
    private int _parsedExprCacheMisses;

    private sealed class SqlExprReferenceComparer : IEqualityComparer<SqlExpr>
    {
        public bool Equals(SqlExpr? x, SqlExpr? y) => ReferenceEquals(x, y);
        public int GetHashCode(SqlExpr obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
        public static readonly SqlExprReferenceComparer Instance = new();
    }
    private readonly Dictionary<SqlExpr, Func<EvalRow, bool>?> _compiledPredicateCache = new(SqlExprReferenceComparer.Instance);
    private static readonly object? _trueBoxed = true;
    private static readonly object? _falseBoxed = false;
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
            ExecuteQuery,
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
            ExecuteQuery,
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

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // Custom schema functions are resolved through the current connection when available.
    private void TrimParsedExprCache()
    {
        if (_parsedExprCache.Count <= ParsedExprCacheMaxSize)
            return;

        var removeCount = _parsedExprCache.Count - ParsedExprCacheMaxSize;
        foreach (var key in _parsedExprCache.Keys.Take(removeCount).ToArray())
            _parsedExprCache.Remove(key);
    }

    private SqlExpr ParseExpr(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            throw new InvalidOperationException("Raw SQL expression cannot be empty");

        if (_parsedExprCache.TryGetValue(raw, out var cached))
            return cached;

        var db = context.Connection.Db ?? throw new InvalidOperationException("Banco SQL não disponível para parse de expressão.");
        var dialect = context.Dialect ?? throw new InvalidOperationException("Dialecto SQL não disponível para parse de expressão.");
        var parsed = SqlExpressionParser.ParseWhere(
            raw,
            db,
            dialect,
            null,
            customFunctionSupported: name => Cnn.TryGetFunction(name, out _));

        _parsedExprCache[raw] = parsed;
        if (++_parsedExprCacheMisses % 64 == 0)
            TrimParsedExprCache();
        return parsed;
    }

    private SqlExpr ParseScalarExpr(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            throw new InvalidOperationException("Raw SQL expression cannot be empty");

        if (_parsedExprCache.TryGetValue(raw, out var cached))
            return cached;

        var db = context.Connection.Db ?? throw new InvalidOperationException("Banco SQL não disponível para parse de expressão.");
        var dialect = context.Dialect ?? throw new InvalidOperationException("Dialecto SQL não disponível para parse de expressão.");
        var parsed = SqlExpressionParser.ParseScalar(
            raw,
            db,
            dialect,
            _pars,
            customFunctionSupported: name => Cnn.TryGetFunction(name, out _));

        _parsedExprCache[raw] = parsed;
        if (++_parsedExprCacheMisses % 64 == 0)
            TrimParsedExprCache();
        return parsed;
    }

    private IReadOnlyList<SqlIndexRecommendation> BuildIndexRecommendations(
            QueryExecutionContext context,
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
        => SelectPlanIndexRecommendationHelper.Build(context, query, metrics);

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

    // ---------------- DIALECT HOOKS ----------------

    /// <summary>
    /// EN: Dialect mapping for JSON access operators (-> / ->> etc).
    /// Default implementation matches current MySQL best-effort behavior.
    /// SqlServer/Postgre/Oracle should override.
    /// PT-br: Mapeamento de dialeto para operadores de acesso JSON (-> / ->> etc).
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
    /// PT-br: Mapeamento de dialeto para avaliação de subconsulta escalar.
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
                if (query is SqlSelectQuery selectQuery
                    && TryEvaluateScalarSubqueryFast(selectQuery, row, ctes, out var fastValue))
                    return fastValue;

                var r = ExecuteQuery(AstQuerySubqueryLookupSupport.LimitToSingleRow(query), ctes, row);
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

    internal static bool IsNullish(object? v) => v is null || v is DBNull;

    private Func<EvalRow, bool>? CompilePredicate(SqlExpr expr)
    {
        if (_compiledPredicateCache.TryGetValue(expr, out var cached))
            return cached;

        var compiled = TryCompilePredicate(expr);
        _compiledPredicateCache[expr] = compiled;
        return compiled;
    }

    private static Func<EvalRow, bool>? TryCompilePredicate(SqlExpr expr)
    {
        switch (expr)
        {
            case BinaryExpr b when b.Op == SqlBinaryOp.And:
            {
                var left = TryCompilePredicate(b.Left);
                var right = TryCompilePredicate(b.Right);
                if (left is not null && right is not null)
                    return row => left(row) && right(row);
                return null;
            }

            case BinaryExpr b when b.Op == SqlBinaryOp.Or:
            {
                var left = TryCompilePredicate(b.Left);
                var right = TryCompilePredicate(b.Right);
                if (left is not null && right is not null)
                    return row => left(row) || right(row);
                return null;
            }

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
            {
                var inner = TryCompilePredicate(u.Expr);
                if (inner is not null)
                    return row => !inner(row);
                return null;
            }

            case BinaryExpr b when b.Right is LiteralExpr rightLit:
            {
                var leftReader = TryBuildColumnReader(b.Left);
                if (leftReader is not null)
                    return BuildBinaryCompiled(b.Op, leftReader, rightLit.Value);
                return null;
            }

            case BinaryExpr b when b.Left is LiteralExpr leftLit:
            {
                var rightReader = TryBuildColumnReader(b.Right);
                if (rightReader is not null)
                    return BuildBinaryCompiled(b.Op, rightReader, leftLit.Value);
                return null;
            }

            default:
                return null;
        }
    }

    private static Func<EvalRow, object?>? TryBuildColumnReader(SqlExpr expr)
    {
        if (expr is ColumnExpr col)
        {
            var name = string.IsNullOrWhiteSpace(col.Qualifier) ? col.Name : $"{col.Qualifier}.{col.Name}";
            return row => ReadColumnValue(row, name);
        }
        return null;
    }

    private static object? ReadColumnValue(EvalRow row, string columnName)
    {
        if (row.OrdinalIndexes is not null && row.OrdinalIndexes.TryGetValue(columnName, out var idx)
            && row.OrdinalValues is not null && idx >= 0 && idx < row.OrdinalValues.Length)
            return row.OrdinalValues[idx];
        row.Fields.TryGetValue(columnName, out var v);
        return v;
    }

    private static Func<EvalRow, bool> BuildBinaryCompiled(SqlBinaryOp op, Func<EvalRow, object?> readColumn, object? literal)
    {
        return op switch
        {
            SqlBinaryOp.Eq => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return literal is null || literal is DBNull;
                if (literal is null || literal is DBNull) return false;
                return val.EqualsSql(literal);
            },

            SqlBinaryOp.Neq => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return !(literal is null || literal is DBNull);
                if (literal is null || literal is DBNull) return true;
                return !val.EqualsSql(literal);
            },

            SqlBinaryOp.Greater => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return false;
                if (literal is null || literal is DBNull) return false;
                return CompareSql(val, literal) > 0;
            },

            SqlBinaryOp.GreaterOrEqual => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return false;
                if (literal is null || literal is DBNull) return false;
                return CompareSql(val, literal) >= 0;
            },

            SqlBinaryOp.Less => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return false;
                if (literal is null || literal is DBNull) return false;
                return CompareSql(val, literal) < 0;
            },

            SqlBinaryOp.LessOrEqual => row =>
            {
                var val = readColumn(row);
                if (val is null || val is DBNull) return false;
                if (literal is null || literal is DBNull) return false;
                return CompareSql(val, literal) <= 0;
            },

            _ => null!
        };
    }

    private static int CompareSql(object? left, object? right)
    {
        if (ReferenceEquals(left, right)) return 0;
        if (left is null) return -1;
        if (right is null) return 1;
        if (left.GetType() == right.GetType() && left is IComparable c)
            return c.CompareTo(right);
        if (left is IComparable lc)
        {
            try { return lc.CompareTo(right); }
            catch { }
        }
        return StringComparer.OrdinalIgnoreCase.Compare(left.ToString(), right.ToString());
    }

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
        private readonly Dictionary<string, int> _sourceOrdinalIndexes;
        private readonly Dictionary<string, Source> _sourceDict;
        /// <summary>
        /// EN: Gets or sets Alias.
        /// PT-br: Obtém ou define Alias.
        /// </summary>
        public string Alias { get; }
        /// <summary>
        /// EN: Gets or sets Name.
        /// PT-br: Obtém ou define Name.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// EN: Gets or sets ColumnNames.
        /// PT-br: Obtém ou define ColumnNames.
        /// </summary>
        public IReadOnlyList<string> ColumnNames { get; }
        public IReadOnlyList<SqlMySqlIndexHint> MySqlIndexHints { get; }

        /// <summary>
        /// EN: Pre-built ordinal index dictionary for this source (cached, not allocated per row).
        /// PT-br: Dicionario de indices ordinais pre-construido para esta source (cache, nao alocado por linha).
        /// </summary>
        internal Dictionary<string, int> SourceOrdinalIndexes => _sourceOrdinalIndexes;

        /// <summary>
        /// EN: Pre-built source dictionary for EvalRow.Sources (cached, not allocated per row).
        /// PT-br: Dicionario de sources pre-construido para EvalRow.Sources (cache, nao alocado por linha).
        /// </summary>
        internal Dictionary<string, Source> SourceDict => _sourceDict;

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
            _sourceOrdinalIndexes = BuildSourceOrdinalIndexes(columnNames, qualifiedColumnNames, sourceQualifiedColumnNames, Alias);
            _sourceDict = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase) { [Alias] = this };
            if (!Name.Equals(Alias, StringComparison.OrdinalIgnoreCase))
                _sourceDict[Name] = this;
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
            _sourceOrdinalIndexes = BuildSourceOrdinalIndexes(columnNames, qualifiedColumnNames, sourceQualifiedColumnNames, Alias);
            _sourceDict = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase) { [Alias] = this };
            if (!Name.Equals(Alias, StringComparison.OrdinalIgnoreCase))
                _sourceDict[Name] = this;
        }
        /// <summary>
        /// EN: Implements WithAlias.
        /// PT-br: Implementa WithAlias.
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
        /// PT-br: Implementa Rows.
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
                return _result?.Count ?? 0;

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
                return _result is not null && index < _result.Count ? 1 : 0;

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

            // Single-row yield; allocation is acceptable.
            // Not using SqlRowPool here because caller manages the dict lifecycle.
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
            var reusable = new Dictionary<string, object?>(Math.Max(ColumnNames.Count, 1), StringComparer.OrdinalIgnoreCase);
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

                reusable.Clear();
                for (var i = 0; i < ColumnNames.Count; i++)
                {
                    var idx = _physicalColumnIndexes![i];
                    reusable[_qualifiedColumnNames![i]] = row.TryGetValue(idx, out var v)
                        ? v
                        : null;
                }

                yield return reusable;
            }
        }

        /// <summary>
        /// EN: Implements FromPhysical.
        /// PT-br: Implementa FromPhysical.
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
        /// PT-br: Retorna uma fonte fisica com filtro de particao diferente.
        /// </summary>
        public Source WithRequestedPartitions(IReadOnlyList<string>? requestedPartitionNames)
        {
            if (Physical is null)
                return this;

            return FromPhysical(Name, Alias, Physical, MySqlIndexHints, requestedPartitionNames);
        }

        /// <summary>
        /// EN: Implements FromResult.
        /// PT-br: Implementa FromResult.
        /// </summary>
        public static Source FromResult(string tableName, string alias, TableResultMock result)
            => new(tableName, alias, result);

        /// <summary>
        /// EN: Implements FromResult.
        /// PT-br: Implementa FromResult.
        /// </summary>
        public static Source FromResult(string tableName, TableResultMock result)
            => new(tableName, tableName, result);

        private static Dictionary<string, int> BuildSourceOrdinalIndexes(
            string[] columnNames,
            string[] qualifiedColumnNames,
            string[]? sourceQualifiedColumnNames,
            string alias)
        {
            var ordinals = new Dictionary<string, int>(columnNames.Length * 3, StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < columnNames.Length; i++)
            {
                ordinals.TryAdd(qualifiedColumnNames[i], i);
                ordinals.TryAdd(columnNames[i], i);
                if (sourceQualifiedColumnNames is not null)
                    ordinals.TryAdd(sourceQualifiedColumnNames[i], i);
            }
            return ordinals;
        }
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

    internal sealed class EvalRow
    {
        private Dictionary<string, object?>? _fields;
        public Dictionary<string, object?> Fields
        {
            get
            {
                if (_fields is null)
                {
                    _fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    if (FieldKeys is not null && OrdinalValues is not null && OrdinalIndexes is not null)
                    {
                        foreach (var key in FieldKeys)
                            if (OrdinalIndexes.TryGetValue(key, out var idx) && idx >= 0 && idx < OrdinalValues.Length)
                                _fields[key] = OrdinalValues[idx];
                    }
                    else if (OrdinalValues is not null && OrdinalIndexes is not null)
                    {
                        foreach (var kvp in OrdinalIndexes)
                            if (kvp.Value >= 0 && kvp.Value < OrdinalValues.Length)
                                _fields[kvp.Key] = OrdinalValues[kvp.Value];
                    }
                }
                return _fields;
            }
            set => _fields = value;
        }
        public Dictionary<string, Source> Sources { get; }
        internal object?[]? OrdinalValues { get; set; }
        internal Dictionary<string, int>? OrdinalIndexes { get; set; }
        internal string[]? FieldKeys { get; set; }
        internal IReadOnlyList<KeyValuePair<string, object?>>? CorrelatedCacheFields { get; set; }
        internal Dictionary<string, IReadOnlyList<KeyValuePair<string, object?>>>? CorrelatedCacheFieldViews { get; set; }
        internal Dictionary<string, string>? CorrelatedCacheKeys { get; set; }
        internal Source? SingleSource { get; set; }

        /// <summary>
        /// EN: Initializes a new EvalRow with the given fields and sources.
        /// PT-br: Inicializa um novo EvalRow com os campos e fontes fornecidos.
        /// </summary>
        public EvalRow(Dictionary<string, object?> fields, Dictionary<string, Source> sources)
        {
            Fields = fields;
            Sources = sources;
        }

        /// <summary>
        /// EN: Implements FromProjected.
        /// PT-br: Implementa FromProjected.
        /// </summary>
        public static EvalRow FromProjected(
            TableResultMock res,
            Dictionary<int, object?> row,
            Dictionary<string, int> aliasToIndex,
            Dictionary<string, object?>? joinFields = null)
        {
            var fields = SqlRowPool.Get(Math.Max(aliasToIndex.Count + (joinFields?.Count ?? 0) * 2, 1));
            // OrdinalValues here flows into EvalRow used by comparers; not returned to pool.
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

            return new EvalRow(fields, new Dictionary<string, Source>(Math.Max(1, aliasToIndex.Count), StringComparer.OrdinalIgnoreCase))
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = new Dictionary<string, int>(aliasToIndex, StringComparer.OrdinalIgnoreCase),
                SingleSource = null
            };
        }

        /// <summary>
        /// EN: Implements CloneRow.
        /// PT-br: Implementa CloneRow.
        /// </summary>
        public EvalRow CloneRow()
        {
            // Clone does not release old Fields/OrdinalValues to pool because
            // the source EvalRow may still be referenced elsewhere in the pipeline.
            var fields = SqlRowPool.Get(Fields.Count);
            foreach (var kvp in Fields)
                fields[kvp.Key] = kvp.Value;
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
        /// PT-br: Clona a linha com capacidade extra nos dicionarios para futuras adicoes de campos e fontes.
        /// </summary>
        /// <param name="extraFieldCapacity">EN: Extra capacity hint for fields. PT-br: Capacidade extra sugerida para campos.</param>
        /// <param name="extraSourceCapacity">EN: Extra capacity hint for sources. PT-br: Capacidade extra sugerida para fontes.</param>
        public EvalRow CloneRow(int extraFieldCapacity, int extraSourceCapacity)
        {
            // Does not release old Fields/OrdinalValues (same reason as CloneRow()).
            var fields = SqlRowPool.Get(Fields.Count + extraFieldCapacity);
            foreach (var kvp in Fields)
                fields[kvp.Key] = kvp.Value;

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
        /// PT-br: Combina a linha atual com campos de uma linha associada e adiciona a source associada.
        /// </summary>
        /// <param name="rightSource">EN: Joined source to append. PT-br: Source associada a adicionar.</param>
        /// <param name="rightFields">EN: Fields produced by the joined row. PT-br: Campos produzidos pela linha associada.</param>
        internal EvalRow MergeJoinRow(Source rightSource, Dictionary<string, object?> rightFields)
        {
            var oldFields = Fields;
            var fields = SqlRowPool.Get(oldFields.Count + rightSource.ColumnNames.Count * 2);
            foreach (var kvp in oldFields)
                fields[kvp.Key] = kvp.Value;
            SqlRowPool.Return(oldFields);

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + 1);
            sources[rightSource.Alias] = rightSource;

            var leftOrdinals = OrdinalValues;
            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasLeftOrdinalMetadata = leftOrdinals is not null && OrdinalIndexes is not null;
            var rightOrdinalCount = rightSource.ColumnNames.Count;
            if (hasLeftOrdinalMetadata || rightOrdinalCount > 0)
            {
                var leftOrdinalCount = hasLeftOrdinalMetadata ? leftOrdinals!.Length : 0;
                ordinalValues = OrdinalPool.Rent(leftOrdinalCount + rightOrdinalCount);
                ordinalIndexes = hasLeftOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(Math.Max(1, rightOrdinalCount * 3), StringComparer.OrdinalIgnoreCase);

                if (hasLeftOrdinalMetadata && leftOrdinalCount > 0)
                    Array.Copy(leftOrdinals!, ordinalValues, leftOrdinalCount);

                if (rightOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + rightOrdinalCount * 3);
                    PopulateJoinedSourceColumns(rightSource, rightFields, fields, ordinalValues, ordinalIndexes, leftOrdinalCount, nullValue: null);
                }
            }

            OrdinalPool.Return(leftOrdinals);

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Creates a null-extended join row for an unmatched right source.
        /// PT-br: Cria uma linha de join com extensao nula para uma source direita sem correspondencia.
        /// </summary>
        /// <param name="rightSource">EN: Right-side source to append. PT-br: Source do lado direito a adicionar.</param>
        internal EvalRow CreateNullExtendedJoinRow(Source rightSource)
        {
            var oldFields = Fields;
            var fields = SqlRowPool.Get(oldFields.Count + rightSource.ColumnNames.Count * 2);
            foreach (var kvp in oldFields)
                fields[kvp.Key] = kvp.Value;
            SqlRowPool.Return(oldFields);

            var sources = new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + 1);
            sources[rightSource.Alias] = rightSource;

            var leftOrdinals = OrdinalValues;
            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasLeftOrdinalMetadata = leftOrdinals is not null && OrdinalIndexes is not null;
            var rightOrdinalCount = rightSource.ColumnNames.Count;
            if (hasLeftOrdinalMetadata || rightOrdinalCount > 0)
            {
                var leftOrdinalCount = hasLeftOrdinalMetadata ? leftOrdinals!.Length : 0;
                ordinalValues = OrdinalPool.Rent(leftOrdinalCount + rightOrdinalCount);
                ordinalIndexes = hasLeftOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(Math.Max(1, rightOrdinalCount * 3), StringComparer.OrdinalIgnoreCase);

                if (hasLeftOrdinalMetadata && leftOrdinalCount > 0)
                    Array.Copy(leftOrdinals!, ordinalValues, leftOrdinalCount);

                if (rightOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + rightOrdinalCount * 3);
                    PopulateJoinedSourceColumns(rightSource, null, fields, ordinalValues, ordinalIndexes, leftOrdinalCount, nullValue: null);
                }
            }

            OrdinalPool.Return(leftOrdinals);

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        /// <summary>
        /// EN: Attaches outer-row values without overwriting fields already produced by the inner row.
        /// PT-br: Anexa valores da linha externa sem sobrescrever campos ja produzidos pela linha interna.
        /// </summary>
        /// <param name="outer">EN: Outer row to overlay. PT-br: Linha externa a sobrepor.</param>
        internal EvalRow AttachOuterRow(EvalRow outer)
        {
            if (outer.Fields.Count == 0
                && outer.Sources.Count == 0
                && outer.OrdinalValues is null
                && outer.OrdinalIndexes is null)
            {
                return this;
            }

            var oldFields = Fields;
            var fields = SqlRowPool.Get(oldFields.Count + outer.Fields.Count * 2);
            foreach (var kvp in oldFields)
                fields[kvp.Key] = kvp.Value;
            SqlRowPool.Return(oldFields);

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

            var innerOrdinals = OrdinalValues;
            object?[]? ordinalValues = null;
            Dictionary<string, int>? ordinalIndexes = null;
            var hasInnerOrdinalMetadata = innerOrdinals is not null && OrdinalIndexes is not null;
            var hasOuterOrdinalMetadata = outer.OrdinalValues is not null && outer.OrdinalIndexes is not null;
            if (hasInnerOrdinalMetadata || hasOuterOrdinalMetadata)
            {
                var innerOrdinalCount = hasInnerOrdinalMetadata ? innerOrdinals!.Length : 0;
                var outerOrdinalCount = hasOuterOrdinalMetadata ? outer.OrdinalValues!.Length : 0;
                ordinalValues = OrdinalPool.Rent(innerOrdinalCount + outerOrdinalCount);
                ordinalIndexes = hasInnerOrdinalMetadata
                    ? new Dictionary<string, int>(OrdinalIndexes!, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                if (hasInnerOrdinalMetadata && innerOrdinalCount > 0)
                    Array.Copy(innerOrdinals!, ordinalValues, innerOrdinalCount);

                if (hasOuterOrdinalMetadata && outerOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + outer.OrdinalIndexes!.Count);
                    AppendOrdinalMetadata(outer, ordinalValues, ordinalIndexes, innerOrdinalCount);
                }
            }

            OrdinalPool.Return(innerOrdinals);

            return new EvalRow(fields, sources)
            {
                OrdinalValues = ordinalValues,
                OrdinalIndexes = ordinalIndexes,
                SingleSource = ResolveSingleSourceCache(sources)
            };
        }

        private static readonly Dictionary<string, Source> _emptySources = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// EN: Returns an empty evaluation row placeholder.
        /// PT-br: Retorna um placeholder de linha de avaliação vazia.
        /// </summary>
        public static EvalRow Empty()
            => new(SqlRowPool.Get(), new Dictionary<string, Source>(_emptySources, StringComparer.OrdinalIgnoreCase))
            {
                OrdinalValues = [],
                OrdinalIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            };

        /// <summary>
        /// EN: Implements AddSource.
        /// PT-br: Implementa AddSource.
        /// </summary>
        public void AddSource(Source src)
        {
            Sources[src.Alias] = src;
            SingleSource = Sources.Count == 1 ? src : null;
        }

        /// <summary>
        /// EN: Implements AddFields.
        /// PT-br: Implementa AddFields.
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
        /// PT-br: Obtém o valor de um campo por nome de coluna qualificado ou não qualificado.
        /// </summary>
        /// <param name="columnName">EN: Column name to read. PT-br: Nome da coluna a ler.</param>
        /// <returns>EN: The field value when present; otherwise null. PT-br: O valor do campo quando presente; caso contrário, null.</returns>
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
    /// PT-br: Implementa EvalGroup.
    /// </summary>
    internal sealed class EvalGroup(List<EvalRow> rows)
    {
        /// <summary>
        /// EN: Gets or sets Rows.
        /// PT-br: Obtém ou define Rows.
        /// </summary>
        public List<EvalRow> Rows { get; } = rows;
    }

    private sealed record MaterializedGroup(GroupKey Key, List<EvalRow> Rows);

    internal readonly struct GroupKey
    {
        private readonly int _count;
        private readonly object? _v0, _v1, _v2, _v3;
        private readonly object?[]? _extra;

        public GroupKey(object?[] values)
        {
            _count = values.Length;
            if (_count > 0) _v0 = values[0];
            if (_count > 1) _v1 = values[1];
            if (_count > 2) _v2 = values[2];
            if (_count > 3) _v3 = values[3];
            _extra = _count > 4 ? new object?[_count - 4] : null;
            if (_extra is not null)
                Array.Copy(values, 4, _extra, 0, _count - 4);
        }

        public readonly int Length => _count;

        public readonly object? this[int index]
        {
            get
            {
                if ((uint)index >= (uint)_count)
                    throw new IndexOutOfRangeException();
                return index switch
                {
                    0 => _v0, 1 => _v1, 2 => _v2, 3 => _v3,
                    _ => _extra![index - 4]
                };
            }
        }

        internal sealed class GroupKeyComparer(
            QueryExecutionContext context
            ) : IEqualityComparer<GroupKey>
        {
            /// <summary>
            /// EN: Implements Equals.
            /// PT-br: Implementa Equals.
            /// </summary>
            public bool Equals(GroupKey x, GroupKey y)
            {
                if (x.Length != y.Length) return false;
                for (int i = 0; i < x.Length; i++)
                    if (!x[i].EqualsSql(y[i], context)) return false;
                return true;
            }

            /// <summary>
            /// EN: Implements GetHashCode.
            /// PT-br: Implementa GetHashCode.
            /// </summary>
            public int GetHashCode(GroupKey obj)
            {
                var h = 17;
                for (int i = 0; i < obj.Length; i++)
                    h = (h * 31) + obj[i].GetHashCodeSql(context);
                return h;
            }
        }
    }

    private sealed class ArrayObjectComparer : IComparer<object?>
    {
        /// <summary>
        /// EN: Implements Compare.
        /// PT-br: Implementa Compare.
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

    internal static class SqlRowPool
    {
        private static readonly System.Collections.Concurrent.ConcurrentBag<Dictionary<string, object?>> _bag = new();

        public static Dictionary<string, object?> Get(int capacity = 0, IEqualityComparer<string>? comparer = null)
        {
            if (_bag.TryTake(out var dict))
            {
                dict.Clear();
                return dict;
            }
            return new Dictionary<string, object?>(capacity, comparer ?? StringComparer.OrdinalIgnoreCase);
        }

        public static void Return(Dictionary<string, object?> dict)
        {
            dict.Clear();
            _bag.Add(dict);
        }
    }

    internal static class OrdinalPool
    {
        private static readonly System.Collections.Concurrent.ConcurrentBag<object?[]> _bag = new();

        public static object?[] Rent(int minLength)
        {
            if (minLength <= 0)
                return [];

            if (_bag.TryTake(out var arr) && arr.Length >= minLength)
            {
                Array.Clear(arr, 0, minLength);
                return arr;
            }
            return new object?[minLength];
        }

        public static void Return(object?[]? arr)
        {
            if (arr is null || arr.Length == 0)
                return;

            Array.Clear(arr, 0, arr.Length);
            _bag.Add(arr);
        }
    }

}
