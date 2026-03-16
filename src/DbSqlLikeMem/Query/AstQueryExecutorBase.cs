using DbSqlLikeMem.Interfaces;
using System.Diagnostics;
using System.IO.Compression;
using DbSqlLikeMem.Models;
using System.Security.Cryptography;
using System.Net;
using System.Text;
using System.Text.Json;

namespace DbSqlLikeMem;

/// <summary>
/// New query engine that executes the new Pratt-based AST (<see cref="SqlSelectQuery"/>) directly
/// against <see cref="TableMock"/> tables.
///
/// Scope: SELECT/WITH only (as per SqlQueryParser).
/// </summary>
internal abstract class AstQueryExecutorBase(
    DbConnectionMockBase cnn,
    IDataParameterCollection pars,
    object dialect)
    : IAstQueryExecutor
{
    private const string SqlServerForJsonColumnName = "JSON_F52E2B61-18A1-11d1-B105-00805F49916B";
    private static readonly Regex _sqlCalcFoundRowsRegex = new(
        @"\bSQL_CALC_FOUND_ROWS\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _cacheKeyWherePredicateRegex = new(
        @"\bWHERE\s+(?<predicate>.+?)(?=(?:\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex _cacheKeyHavingPredicateRegex = new(
        @"\bHAVING\s+(?<predicate>.+?)(?=(?:\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex _qualifiedSqlIdentifierRegex = new(
        @"(?<![A-Za-z0-9_$])([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)(?![A-Za-z0-9_$])",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _subqueryAliasDeclarationRegex = new(
        @"\b(?:FROM|JOIN|APPLY)\s+(?:[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?([A-Z_][A-Z0-9_$]*)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _simpleAliasTokenRegex = new(
        @"^[A-Z_][A-Z0-9_$]*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _dateModifierRegex = new(
        @"^(?<amount>[+-]?\d+)\s*(?<unit>\w+)s?$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex _intervalLiteralRegex = new(
        @"^(?<num>-?\d+(?:\.\d+)?)\s*(?<unit>[a-zA-Z]+)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Random _sharedRandom = new();
    private static readonly object _randomLock = new();
    private static readonly DateTime _unixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly object _uuidShortCounterLock = new();
    private static long _uuidShortCounter;

    private readonly DbConnectionMockBase _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly IDataParameterCollection _pars = pars ?? throw new ArgumentNullException(nameof(pars));
    private readonly object _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    private readonly AstSubqueryEvaluationCache _subqueryEvaluationCache = new();
    private ISqlDialect? Dialect
        => _cnn.UseAutoSqlDialect
            ? _cnn.ExecutionDialect
            : _dialect as ISqlDialect;


    private static readonly HashSet<string> _aggFns = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","COUNT_BIG","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG","ANY_VALUE","BIT_AND","BIT_OR","BIT_XOR","JSON_ARRAYAGG","JSON_GROUP_OBJECT","TOTAL","MEDIAN","PERCENTILE","PERCENTILE_CONT","PERCENTILE_DISC","VAR_POP","VAR_SAMP","VARIANCE","VAR","VARP",
        "COLLECT","CORR","CORR_K","CORR_S","COVAR_POP","COVAR_SAMP","CV","JSON_OBJECTAGG","GROUP_ID",
        "CHECKSUM_AGG","STDEV","STDEVP",
        "APPROX_COUNT_DISTINCT","APPROX_COUNT_DISTINCT_AGG","APPROX_COUNT_DISTINCT_DETAIL","APPROX_MEDIAN","APPROX_PERCENTILE","APPROX_PERCENTILE_AGG","APPROX_PERCENTILE_DETAIL",
        "REGR_AVGX","REGR_AVGY","REGR_COUNT","REGR_INTERCEPT","REGR_R2","REGR_SLOPE","REGR_SXX","REGR_SXY","REGR_SYY",
        "STD","STDDEV","STDDEV_POP","STDDEV_SAMP","STATS_BINOMIAL_TEST","STATS_CROSSTAB","STATS_F_TEST","STATS_KS_TEST","STATS_MODE","STATS_MW_TEST","STATS_ONE_WAY_ANOVA",
        "STATS_T_TEST_INDEP","STATS_T_TEST_INDEPU","STATS_T_TEST_ONE","STATS_T_TEST_PAIRED","STATS_WSR_TEST","XMLAGG","RATIO_TO_REPORT",
        "ARRAY_AGG","BOOL_AND","BOOL_OR","EVERY","JSON_AGG","JSONB_AGG",
        "JSON_OBJECT_AGG","JSON_OBJECT_AGG_STRICT","JSON_OBJECT_AGG_UNIQUE","JSON_OBJECT_AGG_UNIQUE_STRICT",
        "JSONB_OBJECT_AGG","JSONB_OBJECT_AGG_STRICT","JSONB_OBJECT_AGG_UNIQUE","JSONB_OBJECT_AGG_UNIQUE_STRICT"
    };
    private static readonly HashSet<string> _sqlAliasReservedTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","JOIN","INNER","LEFT","RIGHT","FULL","CROSS","OUTER","APPLY","ON","WHERE","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","UNION","ALL","AS","USING","WHEN","THEN","ELSE","END"
    };

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // Resolution is delegated to a shared resolver so this base stays focused on query execution.
    private SqlExpr ParseExpr(string raw)
    {
        var dialectInstance = (object)(Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para parse de expressão."));
        return SqlExpressionParserResolver.ParseWhere(raw, dialectInstance);
    }

    private SqlExpr ParseScalarExpr(string raw)
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para parse de expressão escalar.");
        return SqlExpressionParser.ParseScalar(raw, dialect, _pars);
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
        var sw = Stopwatch.StartNew();
        ClearSubqueryEvaluationCaches();
        QueryDebugTraceBuilder? debugTrace = _cnn.IsDebugTraceCaptureEnabled
            ? new QueryDebugTraceBuilder("UNION")
            : null;

        if (parts is null || parts.Count == 0)
            throw new InvalidOperationException("UNION: nenhuma query.");

        if (allFlags is null)
            throw new InvalidOperationException("UNION: allFlags null.");

        if (allFlags.Count != Math.Max(0, parts.Count - 1))
            throw new InvalidOperationException($"UNION: allFlags.Count inválido. parts={parts.Count}, allFlags={allFlags.Count}");

        // Executa cada SELECT
        var tables = new TableResultMock[parts.Count];

        if (parts.Count == 1)
        {
            tables[0] = ExecuteSelect(parts[0], null, null);
        }
        else
        {
            Parallel.For(0, parts.Count, i => tables[i] = ExecuteSelect(parts[i], null, null));
        }

        debugTrace?.AddStep(
            "UnionInputs",
            tables.Sum(static table => table.Count),
            tables.Length,
            TimeSpan.Zero,
            QueryDebugTraceFormattingHelper.FormatUnionInputsDebugDetails(parts, allFlags));

        // Base do resultado
        var result = new TableResultMock
        {
            Columns = tables[0].Columns,
            JoinFields = tables[0].JoinFields
        };

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para UNION.");

        // valida colunas compatíveis
        var total = tables.Count();
        for (int i = 0; i < total; i++)
        {
            if (tables[i].Columns.Count != result.Columns.Count)
            {
                var msg =
                    $"UNION: número de colunas incompatível. " +
                    $"Primeiro={result.Columns.Count}, SELECT[{i}]={tables[i].Columns.Count}.";
                if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                    msg += "\nSQL: " + sqlContextForErrors;

                throw new InvalidOperationException(msg);
            }

            UnionQueryValidationHelper.ValidateUnionColumnTypes(result.Columns, tables[i].Columns, i, sqlContextForErrors, dialect);
        }

        result.Columns = MergeUnionColumnMetadata(tables.Select(static table => table.Columns).ToList());

        // merge
        // - entre 0 e 1 usa allFlags[0]
        // - entre 1 e 2 usa allFlags[1]
        // etc.
        // Começa com o primeiro
        var needsDistinct = allFlags.Any(flag => !flag);
        var seenRows = needsDistinct ? new HashSet<Dictionary<int, object?>>(new SqlRowDictionaryComparer(dialect)) : null;

        foreach (var row in tables[0])
        {
            result.Add(row);
            seenRows?.Add(row);
        }

        for (int i = 1; i < total; i++)
        {
            var isUnionAll = allFlags[i - 1];

            if (isUnionAll)
            {
                foreach (var row in tables[i])
                    result.Add(row);
                continue;
            }

            // UNION => DISTINCT
            foreach (var row in tables[i])
            {
                if (seenRows!.Add(row))
                    result.Add(row);
            }
        }

        debugTrace?.AddStep(
            "UnionCombine",
            tables.Sum(static table => table.Count),
            result.Count,
            TimeSpan.Zero,
            QueryDebugTraceFormattingHelper.FormatUnionCombineDebugDetails(parts, allFlags));

        // ORDER BY/LIMIT final do UNION segue a projeção final do resultado combinado.
        if ((orderBy?.Count ?? 0) > 0 || rowLimit is not null)
        {
            var finalQ = new SqlSelectQuery(
                Ctes: [],
                Distinct: false,
                SelectItems: [],
                Joins: [],
                Where: null,
                OrderBy: orderBy ?? [],
                RowLimit: rowLimit,
                GroupBy: [],
                Having: null);

            var ctes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = ApplyOrderAndLimit(result, finalQ, ctes, debugTrace);
        }

        sw.Stop();

        var unionInputTables = parts.Sum(CountKnownInputTables);
        var unionEstimatedRead = parts.Sum(EstimateRowsRead);
        var unionMetrics = new SqlPlanRuntimeMetrics(
            InputTables: unionInputTables,
            EstimatedRowsRead: unionEstimatedRead,
            ActualRows: result.Count,
            ElapsedMs: sw.ElapsedMilliseconds);
        var runtimeContext = BuildPlanMockRuntimeContext();

        var plan = SqlExecutionPlanFormatter.FormatUnion(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            unionMetrics,
            runtimeContext);
        result.ExecutionPlan = plan;
        _cnn.RegisterExecutionPlan(plan);
        _cnn.SetLastFoundRows(result.Count);
        if (debugTrace is not null)
            _cnn.RegisterDebugTrace(debugTrace.Build());

        return result;
    }

    private static IList<TableResultColMock> MergeUnionColumnMetadata(IReadOnlyList<IList<TableResultColMock>> columnSets)
    {
        if (columnSets.Count == 0)
            return [];

        var merged = new List<TableResultColMock>(columnSets[0].Count);
        for (var index = 0; index < columnSets[0].Count; index++)
        {
            var first = columnSets[0][index];
            var dbType = first.DbType;
            var isNullable = first.IsNullable;

            for (var setIndex = 1; setIndex < columnSets.Count; setIndex++)
            {
                var current = columnSets[setIndex][index];
                dbType = MergeUnionDbType(dbType, current.DbType);
                isNullable |= current.IsNullable;
            }

            merged.Add(new TableResultColMock(
                first.TableAlias,
                first.ColumnAlias,
                first.ColumnName,
                first.ColumIndex,
                dbType,
                isNullable,
                first.IsJsonFragment));
        }

        return merged;
    }

    private static DbType MergeUnionDbType(DbType left, DbType right)
    {
        if (left == right)
            return left;

        static bool IsFloating(DbType type)
            => type is DbType.Single or DbType.Double;

        static bool IsDecimalLike(DbType type)
            => type is DbType.Decimal or DbType.VarNumeric or DbType.Currency;

        static bool IsIntegerLike(DbType type)
            => type is DbType.Byte or DbType.SByte
                or DbType.Int16 or DbType.UInt16
                or DbType.Int32 or DbType.UInt32
                or DbType.Int64 or DbType.UInt64;

        if ((IsFloating(left) && (IsFloating(right) || IsDecimalLike(right) || IsIntegerLike(right)))
            || (IsFloating(right) && (IsDecimalLike(left) || IsIntegerLike(left))))
        {
            return DbType.Double;
        }

        if (IsDecimalLike(left) && (IsDecimalLike(right) || IsIntegerLike(right)))
            return DbType.Decimal;

        if (IsDecimalLike(right) && IsIntegerLike(left))
            return DbType.Decimal;

        if (IsIntegerLike(left) && IsIntegerLike(right))
            return left is DbType.Int64 or DbType.UInt64 || right is DbType.Int64 or DbType.UInt64
                ? DbType.Int64
                : DbType.Int32;

        return left;
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
            ? new QueryDebugTraceBuilder("SELECT")
            : null;
        var result = ExecuteSelect(q, null, null, debugTrace);
        sw.Stop();

        if (!HasSqlCalcFoundRows(q) && !IsRowCountHelperSelect(q))
            _cnn.SetLastFoundRows(result.Count);

        var metrics = BuildPlanRuntimeMetrics(q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(q, metrics);
        var planWarnings = QueryPlanWarningHelper.BuildPlanWarnings(q, metrics);
        var runtimeContext = BuildPlanMockRuntimeContext();
        var plan = SqlExecutionPlanFormatter.FormatSelect(
            q,
            metrics,
            indexRecommendations,
            planWarnings,
            runtimeContext: runtimeContext);
        result.ExecutionPlan = plan;
        _cnn.RegisterExecutionPlan(plan);
        if (debugTrace is not null)
            _cnn.RegisterDebugTrace(debugTrace.Build());
        return result;
    }

    private SqlPlanMockRuntimeContext BuildPlanMockRuntimeContext()
        => new(
            _cnn.SimulatedLatencyMs,
            _cnn.DropProbability,
            _cnn.Db.ThreadSafe);


    private IReadOnlyList<SqlIndexRecommendation> BuildIndexRecommendations(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        if (metrics.EstimatedRowsRead <= 0)
            return [];

        if (metrics.EstimatedRowsRead < 3)
            return [];

        var sourceMap = BuildSourceMap(query);
        if (sourceMap.Count == 0)
            return [];

        var filterColumnsByTable = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        CollectColumns(query.Where, sourceMap, filterColumnsByTable);
        foreach (var join in query.Joins)
            CollectColumns(join.On, sourceMap, filterColumnsByTable);

        var orderColumnsByTable = BuildOrderByColumns(query, sourceMap);
        var recommendations = new List<SqlIndexRecommendation>();

        foreach (var tableName in sourceMap.Values
            .Select(s => s.Name)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(name => name))
        {
            if (!_cnn.TryGetTable(tableName, out var table) || table is null)
                continue;

            filterColumnsByTable.TryGetValue(tableName, out var filterCols);
            orderColumnsByTable.TryGetValue(tableName, out var orderCols);

            var keyCols = BuildSuggestedKeyColumns(filterCols, orderCols);
            if (keyCols.Count == 0)
                continue;

            if (HasMatchingIndex(table, keyCols) || HasPrimaryKeyPrefix(table, keyCols))
                continue;

            var ddl = $"CREATE INDEX IX_{table.TableName}_{string.Join("_", keyCols)} ON {table.TableName} ({string.Join(", ", keyCols)});";
            var reason = BuildRecommendationReason(filterCols, orderCols, keyCols);

            var estimatedAfter = EstimateRowsReadAfterIndex(metrics.EstimatedRowsRead, filterCols?.Count ?? 0, orderCols?.Count ?? 0);
            var confidence = CalculateRecommendationConfidence(metrics, estimatedAfter, filterCols?.Count ?? 0, orderCols?.Count ?? 0);
            recommendations.Add(new SqlIndexRecommendation(
                table.TableName,
                ddl,
                reason,
                confidence,
                metrics.EstimatedRowsRead,
                estimatedAfter));
        }

        return recommendations;
    }

    private static List<string> BuildSuggestedKeyColumns(
        List<string>? filterCols,
        List<string>? orderCols)
    {
        var keyCols = new List<string>();
        if (filterCols is not null)
        {
            foreach (var col in filterCols)
            {
                if (!keyCols.Contains(col, StringComparer.OrdinalIgnoreCase))
                    keyCols.Add(col);
            }
        }

        if (orderCols is not null)
        {
            foreach (var col in orderCols)
            {
                if (!keyCols.Contains(col, StringComparer.OrdinalIgnoreCase))
                    keyCols.Add(col);
            }
        }

        return keyCols;
    }

    private static bool HasMatchingIndex(ITableMock table, IReadOnlyList<string> keyCols)
    {
        foreach (var idx in table.Indexes.Values)
        {
            if (idx.KeyCols.Count < keyCols.Count)
                continue;

            var matches = true;
            for (var i = 0; i < keyCols.Count; i++)
            {
                if (!string.Equals(idx.KeyCols[i], keyCols[i], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
    }

    private static bool HasPrimaryKeyPrefix(ITableMock table, IReadOnlyList<string> keyCols)
    {
        if (table.PrimaryKeyIndexes.Count == 0)
            return false;

        var pkByOrdinal = table.Columns.Values
            .Where(c => table.PrimaryKeyIndexes.Contains(c.Index))
            .OrderBy(c => c.Index)
            .Select(c => c.Name)
            .ToList();

        if (pkByOrdinal.Count < keyCols.Count)
            return false;

        for (var i = 0; i < keyCols.Count; i++)
        {
            if (!string.Equals(pkByOrdinal[i], keyCols[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static string BuildRecommendationReason(
        List<string>? filterCols,
        List<string>? orderCols,
        IReadOnlyList<string> keyCols)
    {
        var hasFilter = filterCols is { Count: > 0 };
        var hasOrder = orderCols is { Count: > 0 };

        if (hasFilter && hasOrder)
            return SqlExecutionPlanMessages.ReasonFilterAndOrder(
                string.Join(", ", filterCols!),
                string.Join(", ", orderCols!),
                string.Join(", ", keyCols));

        if (hasFilter)
            return SqlExecutionPlanMessages.ReasonFilterOnly(string.Join(", ", filterCols!));

        return SqlExecutionPlanMessages.ReasonOrderOnly(string.Join(", ", orderCols ?? []));
    }

    private static long EstimateRowsReadAfterIndex(long estimatedRowsRead, int filterColumnCount, int orderColumnCount)
    {
        if (estimatedRowsRead <= 0)
            return 0;

        var reductionFactor = 1d;
        if (filterColumnCount > 0)
            reductionFactor *= Math.Pow(0.35d, Math.Min(3, filterColumnCount));

        if (orderColumnCount > 0)
            reductionFactor *= 0.80d;

        var estimated = (long)Math.Ceiling(estimatedRowsRead * reductionFactor);
        return Math.Max(1, Math.Min(estimatedRowsRead, estimated));
    }

    private static int CalculateRecommendationConfidence(
        SqlPlanRuntimeMetrics metrics,
        long estimatedRowsReadAfter,
        int filterColumnCount,
        int orderColumnCount)
    {
        if (metrics.EstimatedRowsRead <= 0)
            return 0;

        var gainPct = (double)(metrics.EstimatedRowsRead - estimatedRowsReadAfter) / metrics.EstimatedRowsRead * 100d;
        var score = 50d;
        score += Math.Min(30d, gainPct * 0.4d);
        if (filterColumnCount > 0)
            score += 10d;
        if (orderColumnCount > 0)
            score += 5d;
        if (metrics.EstimatedRowsRead >= 100)
            score += 5d;

        var rounded = (int)Math.Round(score);
        if (rounded < 1) return 1;
        if (rounded > 99) return 99;
        return rounded;
    }

    private static Dictionary<string, SqlTableSource> BuildSourceMap(SqlSelectQuery query)
    {
        var map = new Dictionary<string, SqlTableSource>(StringComparer.OrdinalIgnoreCase);

        AddPhysicalSource(query.Table, map);
        foreach (var join in query.Joins)
            AddPhysicalSource(join.Table, map);

        return map;
    }

    private static void AddPhysicalSource(
        SqlTableSource? source,
        Dictionary<string, SqlTableSource> map)
    {
        if (source is null || source.Name is null || source.Derived is not null || source.DerivedUnion is not null)
            return;

        map[source.Name] = source;
        if (!string.IsNullOrWhiteSpace(source.Alias))
            map[source.Alias!] = source;
    }

    private static Dictionary<string, List<string>> BuildOrderByColumns(
        SqlSelectQuery query,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap)
    {
        var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var order in query.OrderBy)
        {
            var token = order.Raw?.Trim();
            if (string.IsNullOrWhiteSpace(token))
                continue;

            if (!TryResolveColumn(token!, sourceMap, out var tableName, out var columnName))
                continue;

            if (!result.TryGetValue(tableName, out var list))
            {
                list = [];
                result[tableName] = list;
            }

            if (!list.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                list.Add(columnName);
        }

        return result;
    }

    private static void CollectColumns(
        SqlExpr? expr,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        Dictionary<string, List<string>> columnsByTable)
    {
        if (expr is null)
            return;

        switch (expr)
        {
            case BinaryExpr b:
                CollectColumns(b.Left, sourceMap, columnsByTable);
                CollectColumns(b.Right, sourceMap, columnsByTable);
                break;
            case UnaryExpr u:
                CollectColumns(u.Expr, sourceMap, columnsByTable);
                break;
            case InExpr i:
                CollectColumns(i.Left, sourceMap, columnsByTable);
                foreach (var item in i.Items)
                    CollectColumns(item, sourceMap, columnsByTable);
                break;
            case LikeExpr l:
                CollectColumns(l.Left, sourceMap, columnsByTable);
                CollectColumns(l.Pattern, sourceMap, columnsByTable);
                CollectColumns(l.Escape, sourceMap, columnsByTable);
                break;
            case IsNullExpr n:
                CollectColumns(n.Expr, sourceMap, columnsByTable);
                break;
            case BetweenExpr between:
                CollectColumns(between.Expr, sourceMap, columnsByTable);
                CollectColumns(between.Low, sourceMap, columnsByTable);
                CollectColumns(between.High, sourceMap, columnsByTable);
                break;
            case FunctionCallExpr f:
                foreach (var arg in f.Args)
                    CollectColumns(arg, sourceMap, columnsByTable);
                break;
            case CallExpr c:
                foreach (var arg in c.Args)
                    CollectColumns(arg, sourceMap, columnsByTable);
                break;
            case JsonAccessExpr j:
                CollectColumns(j.Target, sourceMap, columnsByTable);
                break;
            case RowExpr r:
                foreach (var item in r.Items)
                    CollectColumns(item, sourceMap, columnsByTable);
                break;
            case CaseExpr c:
                if (c.BaseExpr is not null)
                    CollectColumns(c.BaseExpr, sourceMap, columnsByTable);
                foreach (var wt in c.Whens)
                {
                    CollectColumns(wt.When, sourceMap, columnsByTable);
                    CollectColumns(wt.Then, sourceMap, columnsByTable);
                }
                if (c.ElseExpr is not null)
                    CollectColumns(c.ElseExpr, sourceMap, columnsByTable);
                break;
            case ColumnExpr col:
                TryAddColumn(col.Qualifier + "." + col.Name, sourceMap, columnsByTable);
                break;
            case IdentifierExpr id:
                TryAddColumn(id.Name, sourceMap, columnsByTable);
                break;
        }
    }

    private static void TryAddColumn(
        string token,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        Dictionary<string, List<string>> columnsByTable)
    {
        if (!TryResolveColumn(token, sourceMap, out var tableName, out var columnName))
            return;

        if (!columnsByTable.TryGetValue(tableName, out var cols))
        {
            cols = [];
            columnsByTable[tableName] = cols;
        }

        if (!cols.Contains(columnName, StringComparer.OrdinalIgnoreCase))
            cols.Add(columnName);
    }

    private static bool TryResolveColumn(
        string token,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        out string tableName,
        out string columnName)
    {
        tableName = string.Empty;
        columnName = string.Empty;

        var parts = token.Split('.').Select(static p => p.Trim()).Where(static p => p.Length > 0).ToArray();
        if (parts.Length == 0)
            return false;

        if (parts.Length == 1)
        {
            var candidates = sourceMap.Values
                .Where(s => !string.IsNullOrWhiteSpace(s.Name))
                .Select(s => s.Name!)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (candidates.Count != 1)
                return false;

            tableName = candidates[0];
            columnName = parts[0];
            return true;
        }

        var sourceKey = parts[^2];
        if (!sourceMap.TryGetValue(sourceKey, out var source) || string.IsNullOrWhiteSpace(source.Name))
            return false;

        tableName = source.Name!;
        columnName = parts[^1];
        return true;
    }

    private SqlPlanRuntimeMetrics BuildPlanRuntimeMetrics(SqlSelectQuery query, int actualRows, long elapsedMs)
        => new(
            InputTables: CountKnownInputTables(query),
            EstimatedRowsRead: EstimateRowsRead(query),
            ActualRows: actualRows,
            ElapsedMs: elapsedMs);

    private static int CountKnownInputTables(SqlSelectQuery query)
    {
        var count = 0;
        if (query.Table is not null && HasKnownPhysicalTable(query.Table))
            count++;

        foreach (var join in query.Joins)
        {
            if (HasKnownPhysicalTable(join.Table))
                count++;
        }

        return count;
    }

    private static string FormatSource(SqlTableSource? source)
    {
        if (source is null)
            return "<none>";

        if (source.Derived is not null)
            return $"subquery AS {source.Alias ?? "<derived>"}";

        if (source.DerivedUnion is not null)
            return $"union-subquery AS {source.Alias ?? "<derived_union>"}";

        if (source.TableFunction is not null)
        {
            var functionName = FormatQualifiedFunctionSource(source);
            var alias = source.Alias ?? source.TableFunction.Name;
            return alias.Equals(source.TableFunction.Name, StringComparison.OrdinalIgnoreCase)
                ? functionName
                : $"{functionName} AS {alias}";
        }

        return FormatQualifiedTableName(source);
    }

    private static string FormatQualifiedFunctionSource(SqlTableSource source)
    {
        var functionName = source.DbName is null
            ? source.TableFunction?.Name ?? "<unknown_function>"
            : $"{source.DbName}.{source.TableFunction?.Name ?? "<unknown_function>"}";

        if (source.TableFunction?.Name.Equals("STRING_SPLIT", StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 3)
        {
            return $"{functionName}(..., ..., enable_ordinal)";
        }

        if (source.TableFunction?.Name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 2)
        {
            var pathShape = TryFormatOpenJsonPathShape(source.TableFunction.Args[1]);
            return source.OpenJsonWithClause is null
                ? $"{functionName}(..., {pathShape})"
                : $"{functionName}(..., {pathShape}) WITH (...)";
        }

        return source.OpenJsonWithClause is null
            ? $"{functionName}(...)"
            : $"{functionName}(...) WITH (...)";
    }

    private static string FormatQualifiedTableName(SqlTableSource source)
    {
        if (source.Name is null)
            return "<unknown_table>";

        return source.DbName is null
            ? source.Name
            : $"{source.DbName}.{source.Name}";
    }

    private static string TryFormatOpenJsonPathShape(SqlExpr pathExpr)
    {
        if (pathExpr is not LiteralExpr { Value: string pathText })
            return "path";

        var trimmed = pathText.Trim();
        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return "strict path";

        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            return "lax path";

        return "path";
    }

    private long EstimateRowsRead(SqlSelectQuery query)
    {
        long total = 0;

        total += GetKnownSourceRows(query.Table);
        foreach (var join in query.Joins)
            total += GetKnownSourceRows(join.Table);

        return total;
    }

    private static bool HasKnownPhysicalTable(SqlTableSource source)
        => source.Name is not null && source.Derived is null && source.DerivedUnion is null && source.TableFunction is null;

    private long GetKnownSourceRows(SqlTableSource? source)
    {
        if (source is null || !HasKnownPhysicalTable(source) || string.IsNullOrWhiteSpace(source.Name))
            return 0;

        if (_cnn.TryGetTable(source.Name!, out var table) && table is not null)
            return table.Count;

        return 0;
    }

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
            var fromRows = rows as List<EvalRow> ?? rows.ToList();
            debugTrace.AddStep(
                "TableScan",
                (int)Math.Min(int.MaxValue, GetKnownSourceRows(selectQuery.Table)),
                fromRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(fromStart)),
                FormatSource(selectQuery.Table));
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
                var joinedRows = rows as List<EvalRow> ?? rows.ToList();
                debugTrace.AddStep(
                    $"Join({FormatJoinTypeForDebug(j.Type)})",
                    inputRows,
                    joinedRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(joinStart)),
                    FormatJoinDebugDetails(j));
                rows = joinedRows;
            }
        }

        // 2.5) Correlated subquery: expose outer row fields/sources to subquery evaluation (EXISTS, IN subselect, etc.)
        if (outerRow is not null)
            rows = rows.Select(r => AttachOuterRow(r, outerRow));

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
                var filteredRows = rows as List<EvalRow> ?? rows.ToList();
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
            return ExecuteGroup(selectQuery, ctes, rows, debugTrace);

        // 5) Project non-grouped
        var projectedRows = rows as List<EvalRow> ?? rows.ToList();
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
        projected = ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
        return projected;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var sourceRows = rows as List<EvalRow> ?? rows.ToList();
        var keyExprs = BuildGroupByKeyExpressions(q);

        var groupStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var grouped = sourceRows.GroupBy(
            r => new GroupKey([.. keyExprs.Select(e => Eval(e, r, group: null, ctes))]),
            GroupKey.Comparer);
        if (debugTrace is not null)
        {
            var groupedList = grouped.ToList();
            debugTrace.AddStep(
                "Group",
                sourceRows.Count,
                groupedList.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(groupStart)),
                QueryDebugTraceFormattingHelper.FormatGroupDebugDetails(q));
            grouped = groupedList;
        }

        // HAVING filter (MySQL: HAVING pode referenciar alias do SELECT)
        if (q.Having is null)
        {
            // Project grouped
            return ProjectGrouped(q, grouped, ctes, debugTrace);
        }

        // pré-parse das expressões do SELECT que têm Alias (ex: COUNT(val) AS C)
        var aliasExprs = q.SelectItems
            .Select(si =>
            {
                // pega alias mesmo se o parser não preencheu si.Alias
                var (exprRaw, alias) = SplitTrailingAsAlias(si.Raw, si.Alias);
                if (string.IsNullOrWhiteSpace(alias))
                    return ((string Alias, SqlExpr Ast)?)null;

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

                return (Alias: alias!, Ast: ast);
            })
            .Where(x => x.HasValue)
            .Select(x => x!.Value)
            .ToList();

        var havingExpr = NormalizeHavingExpression(q.Having, q);

        var havingStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var inputGroups = debugTrace is not null
            ? (grouped as ICollection<IGrouping<GroupKey, EvalRow>>)?.Count ?? grouped.Count()
            : 0;
        grouped = ApplyHavingPredicate(grouped, havingExpr, aliasExprs, ctes);
        if (debugTrace is not null)
        {
            var filteredGroups = grouped.ToList();
            debugTrace.AddStep(
                "Having",
                inputGroups,
                filteredGroups.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(havingStart)),
                SqlExprPrinter.Print(q.Having));
            grouped = filteredGroups;
        }

        // Project grouped
        return ProjectGrouped(q, grouped, ctes, debugTrace);
    }


    private IEnumerable<EvalRow> ApplyRowPredicate(
        IEnumerable<EvalRow> rows,
        SqlExpr predicate,
        IDictionary<string, Source> ctes)
        => rows.Where(r => Eval(predicate, r, group: null, ctes).ToBool());

    private IEnumerable<IGrouping<GroupKey, EvalRow>> ApplyHavingPredicate(
        IEnumerable<IGrouping<GroupKey, EvalRow>> grouped,
        SqlExpr havingExpr,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes)
    {
        return grouped.Where(g =>
        {
            var evalCtx = BuildHavingEvaluationContext(g, aliasExprs, ctes, out var evalGroup);
            EnsureHavingIdentifiersAreBound(havingExpr, evalCtx, Dialect!);
            return Eval(havingExpr, evalCtx, evalGroup, ctes).ToBool();
        });
    }

    private EvalRow BuildHavingEvaluationContext(
        IGrouping<GroupKey, EvalRow> grouped,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes,
        out EvalGroup evalGroup)
    {
        var rows = grouped.ToList();
        evalGroup = new EvalGroup(rows);
        var first = rows[0];

        var ctx = first.CloneRow();
        foreach (var (alias, ast) in aliasExprs)
            ctx.Fields[alias] = Eval(ast, first, evalGroup, ctes);

        return ctx;
    }

    private SqlExpr NormalizeHavingExpression(SqlExpr expr, SqlSelectQuery q)
    {
        var usedOrdinal = false;
        int? outOfRangeOrdinal = null;
        int? nonPositiveOrdinal = null;
        var rewritten = RewriteHavingOrdinals(
            expr,
            q,
            ref usedOrdinal,
            allowOrdinalLiteral: true,
            ref outOfRangeOrdinal,
            ref nonPositiveOrdinal);

        if (nonPositiveOrdinal.HasValue)
            throw new InvalidOperationException("invalid: HAVING ordinal must be >= 1");

        if (outOfRangeOrdinal.HasValue)
            throw new InvalidOperationException($"invalid: HAVING ordinal {outOfRangeOrdinal.Value} out of range");

        if (usedOrdinal)
            return rewritten;

        var hasAggregate = WalkHasAggregate(rewritten);
        var hasIdentifier = EnumerateIdentifiers(rewritten).Any();
        var hasTemporalReference = WalkHasTemporalHavingReference(rewritten, Dialect!);
        if (hasAggregate || hasIdentifier || hasTemporalReference)
            return rewritten;

        throw new InvalidOperationException(
            "invalid: HAVING must reference grouped columns, projected aliases, aggregates, or valid ordinals");
    }

    /// <summary>
    /// EN: Detects whether HAVING expression references dialect temporal zero-arg tokens/functions.
    /// PT: Detecta se a expressão HAVING referencia tokens/funções temporais zero-arg do dialeto.
    /// </summary>
    /// <param name="expr">EN: HAVING expression to inspect. PT: Expressão HAVING a inspecionar.</param>
    /// <param name="dialect">EN: Active SQL dialect. PT: Dialeto SQL ativo.</param>
    /// <returns>EN: True when expression contains temporal reference valid for HAVING semantics. PT: True quando a expressão contém referência temporal válida para semântica de HAVING.</returns>
    private static bool WalkHasTemporalHavingReference(SqlExpr expr, ISqlDialect dialect)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return dialect.TemporalFunctionIdentifierNames.Any(name =>
                    name.Equals(id.Name, StringComparison.OrdinalIgnoreCase));

            case FunctionCallExpr fn:
                if (fn.Args.Count == 0 && dialect.TemporalFunctionCallNames.Any(name =>
                        name.Equals(fn.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    return true;
                }
                return fn.Args.Any(arg => WalkHasTemporalHavingReference(arg, dialect));

            case CallExpr call:
                if (call.Args.Count == 0 && dialect.TemporalFunctionCallNames.Any(name =>
                        name.Equals(call.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    return true;
                }
                return call.Args.Any(arg => WalkHasTemporalHavingReference(arg, dialect));

            case BinaryExpr b:
                return WalkHasTemporalHavingReference(b.Left, dialect)
                    || WalkHasTemporalHavingReference(b.Right, dialect);

            case UnaryExpr u:
                return WalkHasTemporalHavingReference(u.Expr, dialect);

            case IsNullExpr isn:
                return WalkHasTemporalHavingReference(isn.Expr, dialect);

            case LikeExpr like:
                return WalkHasTemporalHavingReference(like.Left, dialect)
                    || WalkHasTemporalHavingReference(like.Pattern, dialect)
                    || (like.Escape != null && WalkHasTemporalHavingReference(like.Escape, dialect));

            case InExpr i:
                if (WalkHasTemporalHavingReference(i.Left, dialect))
                    return true;
                return i.Items.Any(item => WalkHasTemporalHavingReference(item, dialect));

            case RowExpr r:
                return r.Items.Any(item => WalkHasTemporalHavingReference(item, dialect));

            case BetweenExpr bt:
                return WalkHasTemporalHavingReference(bt.Expr, dialect)
                    || WalkHasTemporalHavingReference(bt.Low, dialect)
                    || WalkHasTemporalHavingReference(bt.High, dialect);

            case CaseExpr c:
                if (c.BaseExpr is not null && WalkHasTemporalHavingReference(c.BaseExpr, dialect))
                    return true;

                foreach (var whenThen in c.Whens)
                {
                    if (WalkHasTemporalHavingReference(whenThen.When, dialect))
                        return true;
                    if (WalkHasTemporalHavingReference(whenThen.Then, dialect))
                        return true;
                }

                return c.ElseExpr is not null && WalkHasTemporalHavingReference(c.ElseExpr, dialect);

            default:
                return false;
        }
    }

    private SqlExpr RewriteHavingOrdinals(
        SqlExpr expr,
        SqlSelectQuery q,
        ref bool usedOrdinal,
        bool allowOrdinalLiteral,
        ref int? outOfRangeOrdinal,
        ref int? nonPositiveOrdinal)
    {
        switch (expr)
        {
            case LiteralExpr l when allowOrdinalLiteral && TryLiteralToIntOrdinal(l.Value, out var ord):
                {
                    if (ord < 1)
                    {
                        nonPositiveOrdinal ??= ord;
                        return expr;
                    }

                    var idx = ord - 1;
                    if (idx >= q.SelectItems.Count)
                    {
                        outOfRangeOrdinal ??= ord;
                        return expr;
                    }

                    var selectItem = q.SelectItems[idx];
                    var (exprRaw, _) = SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                    usedOrdinal = true;
                    return ParseExpr(exprRaw);
                }

            case BinaryExpr b:
                var leftCanBeOrdinal = IsOrdinalCandidateSide(b.Op, leftSide: true);
                var rightCanBeOrdinal = IsOrdinalCandidateSide(b.Op, leftSide: false);
                return b with
                {
                    Left = RewriteHavingOrdinals(b.Left, q, ref usedOrdinal, leftCanBeOrdinal, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Right = RewriteHavingOrdinals(b.Right, q, ref usedOrdinal, rightCanBeOrdinal, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            case UnaryExpr u:
                return u with { Expr = RewriteHavingOrdinals(u.Expr, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal) };
            case IsNullExpr isn:
                return isn with { Expr = RewriteHavingOrdinals(isn.Expr, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal) };
            case LikeExpr like:
                return like with
                {
                    Left = RewriteHavingOrdinals(like.Left, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Pattern = RewriteHavingOrdinals(like.Pattern, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Escape = like.Escape is null
                        ? null
                        : RewriteHavingOrdinals(like.Escape, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            case InExpr i:
                var rewrittenInItems = new List<SqlExpr>(i.Items.Count);
                foreach (var item in i.Items)
                {
                    rewrittenInItems.Add(RewriteHavingOrdinals(item, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal));
                }
                return i with
                {
                    Left = RewriteHavingOrdinals(i.Left, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Items = [.. rewrittenInItems]
                };
            case RowExpr r:
                var rewrittenRowItems = new List<SqlExpr>(r.Items.Count);
                foreach (var item in r.Items)
                {
                    rewrittenRowItems.Add(RewriteHavingOrdinals(item, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal));
                }
                return r with { Items = [.. rewrittenRowItems] };
            case BetweenExpr bt:
                return bt with
                {
                    Expr = RewriteHavingOrdinals(bt.Expr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Low = RewriteHavingOrdinals(bt.Low, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    High = RewriteHavingOrdinals(bt.High, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            case FunctionCallExpr fn:
                var rewrittenFnArgs = new List<SqlExpr>(fn.Args.Count);
                foreach (var arg in fn.Args)
                {
                    rewrittenFnArgs.Add(RewriteHavingOrdinals(arg, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal));
                }
                return fn with { Args = [.. rewrittenFnArgs] };
            case CallExpr call:
                var rewrittenCallArgs = new List<SqlExpr>(call.Args.Count);
                foreach (var arg in call.Args)
                {
                    rewrittenCallArgs.Add(RewriteHavingOrdinals(arg, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal));
                }
                return call with { Args = [.. rewrittenCallArgs] };
            case CaseExpr c:
                var rewrittenWhens = new List<CaseWhenThen>(c.Whens.Count);
                foreach (var when in c.Whens)
                {
                    rewrittenWhens.Add(when with
                    {
                        When = RewriteHavingOrdinals(when.When, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                        Then = RewriteHavingOrdinals(when.Then, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                    });
                }
                return c with
                {
                    BaseExpr = c.BaseExpr is null ? null : RewriteHavingOrdinals(c.BaseExpr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Whens = [.. rewrittenWhens],
                    ElseExpr = c.ElseExpr is null ? null : RewriteHavingOrdinals(c.ElseExpr, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            default:
                return expr;
        }
    }

    private static bool IsOrdinalCandidateSide(SqlBinaryOp op, bool leftSide)
        => op switch
        {
            SqlBinaryOp.Eq => true,
            SqlBinaryOp.Neq => true,
            SqlBinaryOp.Greater => leftSide,
            SqlBinaryOp.GreaterOrEqual => leftSide,
            SqlBinaryOp.Less => !leftSide,
            SqlBinaryOp.LessOrEqual => !leftSide,
            SqlBinaryOp.NullSafeEq => true,
            _ => false
        };

    private static bool TryLiteralToIntOrdinal(object? value, out int ordinal)
    {
        switch (value)
        {
            case decimal m when m >= int.MinValue && m <= int.MaxValue && decimal.Truncate(m) == m:
                ordinal = (int)m;
                return true;
            case int i:
                ordinal = i;
                return true;
            case long l when l >= int.MinValue && l <= int.MaxValue:
                ordinal = (int)l;
                return true;
            case short s:
                ordinal = s;
                return true;
            case byte b:
                ordinal = b;
                return true;
            default:
                ordinal = 0;
                return false;
        }
    }

    private static void EnsureHavingIdentifiersAreBound(SqlExpr expr, EvalRow row, ISqlDialect dialect)
    {
        foreach (var name in EnumerateIdentifiers(expr))
        {
            if (IsHavingTemporalIdentifier(name, dialect))
                continue;

            if (IsIdentifierBound(row, name))
                continue;

            throw new InvalidOperationException($"invalid: HAVING reference '{name}' was not found in grouped projection");
        }
    }

    /// <summary>
    /// EN: Checks whether a HAVING identifier maps to a temporal token supported as identifier by the active dialect.
    /// PT: Verifica se um identificador no HAVING mapeia para token temporal suportado como identificador pelo dialeto ativo.
    /// </summary>
    /// <param name="name">EN: Identifier collected from HAVING expression. PT: Identificador coletado da expressão HAVING.</param>
    /// <param name="dialect">EN: Active SQL dialect. PT: Dialeto SQL ativo.</param>
    /// <returns>EN: True when identifier is a dialect temporal token allowed without projection binding. PT: True quando o identificador é token temporal do dialeto permitido sem binding de projeção.</returns>
    private static bool IsHavingTemporalIdentifier(string name, ISqlDialect dialect)
        => dialect.TemporalFunctionIdentifierNames.Any(token => token.Equals(name, StringComparison.OrdinalIgnoreCase));

    private static bool IsIdentifierBound(EvalRow row, string name)
    {
        var dot = name.IndexOf('.');
        if (dot >= 0)
        {
            var qualifier = name[..dot].Trim();
            var col = name[(dot + 1)..].Trim();
            if (qualifier.Length == 0 || col.Length == 0)
                return false;

            if (row.Fields.ContainsKey($"{qualifier}.{col}"))
                return true;

            if (row.Sources.TryGetValue(qualifier, out var src))
            {
                var hit = src.ColumnNames.FirstOrDefault(c => c.Equals(col, StringComparison.OrdinalIgnoreCase));
                return hit is not null && row.Fields.ContainsKey($"{src.Alias}.{hit}");
            }

            return false;
        }

        if (row.Fields.ContainsKey(name))
            return true;

        return row.Fields.Keys.Any(k => k.EndsWith($".{name}", StringComparison.OrdinalIgnoreCase));
    }

    private static IEnumerable<string> EnumerateIdentifiers(SqlExpr expr)
    {
        var identifiers = new List<string>();
        AppendIdentifiers(expr, identifiers);
        return identifiers;
    }

    private static void AppendIdentifiers(SqlExpr expr, List<string> identifiers)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                identifiers.Add(id.Name);
                return;
            case ColumnExpr col:
                identifiers.Add(FormatIdentifierColumn(col));
                return;
            case UnaryExpr unary:
                AppendIdentifiers(unary.Expr, identifiers);
                return;
            case IsNullExpr isNull:
                AppendIdentifiers(isNull.Expr, identifiers);
                return;
            case BinaryExpr binary:
                AppendBinaryIdentifiers(binary.Left, binary.Right, identifiers);
                return;
            case LikeExpr like:
                AppendLikeIdentifiers(like, identifiers);
                return;
            case InExpr inExpr:
                AppendInIdentifiers(inExpr, identifiers);
                return;
            case RowExpr row:
                AppendIdentifierSequence(row.Items, identifiers);
                return;
            case CaseExpr @case:
                AppendCaseIdentifiers(@case, identifiers);
                return;
            case FunctionCallExpr function:
                AppendIdentifierSequence(function.Args, identifiers);
                return;
            case CallExpr call:
                AppendIdentifierSequence(call.Args, identifiers);
                return;
            case JsonAccessExpr jsonAccess:
                AppendBinaryIdentifiers(jsonAccess.Target, jsonAccess.Path, identifiers);
                return;
            case BetweenExpr between:
                AppendBetweenIdentifiers(between, identifiers);
                return;
            default:
                return;
        }
    }

    private static string FormatIdentifierColumn(ColumnExpr column)
        => string.IsNullOrWhiteSpace(column.Qualifier)
            ? column.Name
            : $"{column.Qualifier}.{column.Name}";

    private static void AppendBinaryIdentifiers(SqlExpr left, SqlExpr right, List<string> identifiers)
    {
        AppendIdentifiers(left, identifiers);
        AppendIdentifiers(right, identifiers);
    }

    private static void AppendLikeIdentifiers(LikeExpr like, List<string> identifiers)
    {
        AppendIdentifiers(like.Left, identifiers);
        AppendIdentifiers(like.Pattern, identifiers);
        if (like.Escape is not null)
            AppendIdentifiers(like.Escape, identifiers);
    }

    private static void AppendInIdentifiers(InExpr inExpr, List<string> identifiers)
    {
        AppendIdentifiers(inExpr.Left, identifiers);
        AppendIdentifierSequence(inExpr.Items, identifiers);
    }

    private static void AppendCaseIdentifiers(CaseExpr @case, List<string> identifiers)
    {
        if (@case.BaseExpr is not null)
            AppendIdentifiers(@case.BaseExpr, identifiers);

        foreach (var when in @case.Whens)
        {
            AppendIdentifiers(when.When, identifiers);
            AppendIdentifiers(when.Then, identifiers);
        }

        if (@case.ElseExpr is not null)
            AppendIdentifiers(@case.ElseExpr, identifiers);
    }

    private static void AppendBetweenIdentifiers(BetweenExpr between, List<string> identifiers)
    {
        AppendIdentifiers(between.Expr, identifiers);
        AppendIdentifiers(between.Low, identifiers);
        AppendIdentifiers(between.High, identifiers);
    }

    private static void AppendIdentifierSequence(IEnumerable<SqlExpr> expressions, List<string> identifiers)
    {
        foreach (var expression in expressions)
            AppendIdentifiers(expression, identifiers);
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
            yield return new EvalRow(
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));
            yield break;
        }

        var src = ResolveSource(from, ctes);
        var sourceRows = TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var r in sourceRows)
        {
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var it in r)
                fields[it.Key] = it.Value;

            var sources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            {
                [src.Alias] = src
            };

            yield return new EvalRow(fields, sources);
        }
    }

    private IEnumerable<Dictionary<string, object?>>? TryRowsFromIndex(
        Source src,
        SqlTableSource? tableSource,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (src.Physical is null || src.Physical.Indexes.Count == 0)
            return null;

        var hintPlan = BuildMySqlIndexHintPlan(tableSource?.MySqlIndexHints, src.Physical, hasOrderBy, hasGroupBy);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");

        if (where is null)
            return null;

        if (!TryCollectColumnEqualities(where, src, out var equalsByColumn)
            || equalsByColumn.Count == 0)
            return null;

        IndexDef? best = null;
        foreach (var ix in src.Physical.Indexes.Values)
        {
            if (ix.KeyCols.Count == 0)
                continue;

            if (hintPlan is not null && !hintPlan.AllowedIndexNames.Contains(ix.Name.NormalizeName()))
                continue;

            var coversAll = ix.KeyCols.All(col => equalsByColumn.ContainsKey(col.NormalizeName()));
            if (!coversAll)
                continue;

            if (best is null || ix.KeyCols.Count > best.KeyCols.Count)
                best = ix;
        }

        if (best is null)
            return null;

        var key = src.Physical is TableMock
            ? best.BuildIndexKeyFromValues(equalsByColumn)
            : string.Join("|", best.KeyCols.Select(col =>
            {
                var norm = col.NormalizeName();
                var value = equalsByColumn[norm];
                return value?.ToString() ?? "<null>";
            }));

        var positions = LookupIndexWithMetrics(src.Physical, best, key);
        if (positions is null)
            return [];

        return src.RowsByIndexes(positions.Keys);
    }



    private static MySqlIndexHintPlan? BuildMySqlIndexHintPlan(
        IReadOnlyList<SqlMySqlIndexHint>? hints,
        ITableMock table,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (hints is null || hints.Count == 0)
            return null;

        var allIndexes = table.Indexes.Values.ToList();
        var existingIndexNames = allIndexes
            .Select(static ix => ix.Name.NormalizeName())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var primaryEquivalentIndexNames = ResolvePrimaryEquivalentIndexNames(table, allIndexes);

        var forceHintsToValidate = hints
            .Where(hint => hint.Kind == SqlMySqlIndexHintKind.Force
                && (hint.Scope == SqlMySqlIndexHintScope.Any
                    || hint.Scope == SqlMySqlIndexHintScope.Join
                    || (hint.Scope == SqlMySqlIndexHintScope.OrderBy && hasOrderBy)
                    || (hint.Scope == SqlMySqlIndexHintScope.GroupBy && hasGroupBy)))
            .ToList();

        var missingForced = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var hint in forceHintsToValidate)
        {
            var normalizedNames = ExpandHintIndexNames(hint.IndexNames, primaryEquivalentIndexNames)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var item in normalizedNames)
            {
                if (!existingIndexNames.Contains(item))
                    missingForced.Add(item);
            }
        }

        var joinScopeHints = hints.Where(static h => h.Scope is SqlMySqlIndexHintScope.Any or SqlMySqlIndexHintScope.Join).ToList();
        var allowedNames = new HashSet<string>(existingIndexNames, StringComparer.OrdinalIgnoreCase);

        foreach (var hint in joinScopeHints)
        {
            var normalizedNames = ExpandHintIndexNames(hint.IndexNames, primaryEquivalentIndexNames)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (hint.Kind is SqlMySqlIndexHintKind.Use or SqlMySqlIndexHintKind.Force)
            {
                allowedNames.IntersectWith(normalizedNames);
            }
            else if (hint.Kind == SqlMySqlIndexHintKind.Ignore)
            {
                allowedNames.ExceptWith(normalizedNames);
            }
        }

        return new MySqlIndexHintPlan(allowedNames, missingForced);
    }


    private static IEnumerable<string> ExpandHintIndexNames(
        IReadOnlyList<string> hintedNames,
        IReadOnlyHashSet<string> primaryEquivalentIndexNames)
    {
        foreach (var hintedName in hintedNames)
        {
            var normalized = hintedName.NormalizeName();
            if (!normalized.Equals("primary", StringComparison.OrdinalIgnoreCase))
            {
                yield return normalized;
                continue;
            }

            if (primaryEquivalentIndexNames.Count == 0)
            {
                yield return "primary";
                continue;
            }

            foreach (var item in primaryEquivalentIndexNames)
                yield return item;
        }
    }

    private static IReadOnlyHashSet<string> ResolvePrimaryEquivalentIndexNames(
        ITableMock table,
        IReadOnlyList<IndexDef> allIndexes)
    {
        var pkColumnNames = table.PrimaryKeyIndexes
            .Select(pkIdx => table.Columns.FirstOrDefault(col => col.Value.Index == pkIdx).Key)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(static name => name!.NormalizeName())
            .ToList();

        if (pkColumnNames.Count == 0)
            return new ReadOnlyHashSet<string>(); ;

        var primaryEquivalent = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var index in allIndexes)
        {
            if (index.KeyCols.Count != pkColumnNames.Count)
                continue;

            var indexCols = index.KeyCols.Select(static col => col.NormalizeName()).ToList();
            if (indexCols.SequenceEqual(pkColumnNames))
                primaryEquivalent.Add(index.Name.NormalizeName());
        }

        return new ReadOnlyHashSet<string>(primaryEquivalent);
    }

    private IReadOnlyDictionary<int, Dictionary<string, object?>>? LookupIndexWithMetrics(
        ITableMock table,
        IndexDef indexDef,
        string key)
    {
        _cnn.Metrics.IndexLookups++;
        _cnn.Metrics.IncrementIndexHint(indexDef.Name.NormalizeName());
        return indexDef.LookupMutable(key);
    }

    private bool TryCollectColumnEqualities(
        SqlExpr where,
        Source src,
        out Dictionary<string, object?> equalsByColumn)
    {
        equalsByColumn = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        return Walk(where, ref equalsByColumn);

        bool Walk(SqlExpr expr, ref Dictionary<string, object?> eqCol)
        {
            if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
                return Walk(andExpr.Left, ref eqCol) && Walk(andExpr.Right, ref eqCol);

            if (expr is not BinaryExpr eq || eq.Op != SqlBinaryOp.Eq)
                return false;

            if (TryGetColumnAndValue(eq.Left, eq.Right, src, out var column, out var value)
                || TryGetColumnAndValue(eq.Right, eq.Left, src, out column, out value))
            {
                eqCol[column] = value;
                return true;
            }

            return false;
        }
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
    {
        // FULL is not MySQL; accept as INNER for test/mock purposes
        var jt = //join.Type == SqlJoinType.Full ? SqlJoinType.Inner : 
            join.Type;

        if (jt is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
        {
            var isOuterApply = jt == SqlJoinType.OuterApply;
            foreach (var leftRow in leftRows)
            {
                foreach (var applied in ApplyApplyJoin(join, ctes, hasOrderBy, hasGroupBy, leftRow, isOuterApply))
                    yield return applied;
            }

            yield break;
        }

        if (join.Table.IsLateral)
        {
            foreach (var leftRow in leftRows)
            {
                foreach (var lateralRow in ApplyLateralJoin(join, ctes, leftRow, isLeft: jt == SqlJoinType.Left))
                    yield return lateralRow;
            }

            yield break;
        }

        var rightSrc = ResolveSource(join.Table, ctes);

        if (rightSrc.Physical is not null)
        {
            var hintPlan = BuildMySqlIndexHintPlan(join.Table.MySqlIndexHints, rightSrc.Physical, hasOrderBy, hasGroupBy);
            if (hintPlan?.MissingForcedIndexes.Count > 0)
                throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");
        }

        if (jt == SqlJoinType.Cross)
        {
            foreach (var l in leftRows)
                foreach (var rr in rightSrc.Rows())
                {
                    var merged = l.CloneRow();
                    merged.AddSource(rightSrc);
                    merged.AddFields(rr);
                    yield return merged;
                }
            yield break;
        }

        if (jt == SqlJoinType.Right)
        {
            // Implement RIGHT JOIN by swapping and treating as LEFT
            var swapped = new SqlJoin(SqlJoinType.Left, join.Table, join.On);
            foreach (var r in ApplyJoinRight(leftRows, swapped, rightSrc, ctes))
                yield return r;
            yield break;
        }

        bool isLeft = jt == SqlJoinType.Left;

        foreach (var l in leftRows)
            foreach (var li in ApplyLeftJoin(join, ctes, rightSrc, isLeft, l))
                yield return li;
    }

    private IEnumerable<EvalRow> ApplyApplyJoin(
        SqlJoin join,
        IDictionary<string, Source> ctes,
        bool hasOrderBy,
        bool hasGroupBy,
        EvalRow leftRow,
        bool isOuterApply)
    {
        var rightSrc = ResolveSource(join.Table, ctes, leftRow);

        if (rightSrc.Physical is not null)
        {
            var hintPlan = BuildMySqlIndexHintPlan(join.Table.MySqlIndexHints, rightSrc.Physical, hasOrderBy, hasGroupBy);
            if (hintPlan?.MissingForcedIndexes.Count > 0)
                throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");
        }

        var matched = false;
        foreach (var rr in rightSrc.Rows())
        {
            matched = true;
            var merged = leftRow.CloneRow();
            merged.AddSource(rightSrc);
            merged.AddFields(rr);
            yield return merged;
        }

        if (!isOuterApply || matched)
            yield break;

        var mergedOuter = leftRow.CloneRow();
        mergedOuter.AddSource(rightSrc);
        foreach (var c in rightSrc.ColumnNames)
            mergedOuter.Fields[$"{rightSrc.Alias}.{c}"] = null;
        yield return mergedOuter;
    }

    private IEnumerable<EvalRow> ApplyLeftJoin(
        SqlJoin join,
        IDictionary<string, Source> ctes,
        Source rightSrc,
        bool isLeft, EvalRow l)
    {
        bool matched = false;

        foreach (var rr in rightSrc.Rows())
        {
            var merged = l.CloneRow();
            merged.AddSource(rightSrc);
            merged.AddFields(rr);

            if (!Eval(join.On, merged, group: null, ctes).ToBool())
                continue;
            matched = true;
            yield return merged;
        }

        if (isLeft && !matched)
        {
            var merged = l.CloneRow();
            merged.AddSource(rightSrc);
            // add nulls for right columns
            foreach (var c in rightSrc.ColumnNames)
                merged.Fields[$"{rightSrc.Alias}.{c}"] = null;
            yield return merged;
        }
    }

    private IEnumerable<EvalRow> ApplyLateralJoin(
        SqlJoin join,
        IDictionary<string, Source> ctes,
        EvalRow leftRow,
        bool isLeft)
    {
        var rightSrc = ResolveSource(join.Table, ctes, leftRow);
        var matched = false;

        foreach (var rr in rightSrc.Rows())
        {
            var merged = leftRow.CloneRow();
            merged.AddSource(rightSrc);
            merged.AddFields(rr);

            if (!Eval(join.On, merged, group: null, ctes).ToBool())
                continue;

            matched = true;
            yield return merged;
        }

        if (isLeft && !matched)
        {
            var merged = leftRow.CloneRow();
            merged.AddSource(rightSrc);
            foreach (var c in rightSrc.ColumnNames)
                merged.Fields[$"{rightSrc.Alias}.{c}"] = null;
            yield return merged;
        }
    }

    private IEnumerable<EvalRow> ApplyJoinRight(
        IEnumerable<EvalRow> leftRows,
        SqlJoin leftJoin,
        Source rightSrc,
        IDictionary<string, Source> ctes)
    {
        // RIGHT JOIN: produce all right rows, optionally matched with left
        var leftList = leftRows.ToList();

        foreach (var rr in rightSrc.Rows())
        {
            bool matched = false;
            foreach (var l in leftList)
            {
                var merged = l.CloneRow();
                merged.AddSource(rightSrc);
                merged.AddFields(rr);

                if (Eval(leftJoin.On, merged, group: null, ctes).ToBool())
                {
                    matched = true;
                    yield return merged;
                }
            }

            if (matched)
                continue;

            // left side nulls (best effort: keep existing left sources, but blank their fields)
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var sources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            {
                // no left sources/fields
                [rightSrc.Alias] = rightSrc
            };

            var row = new EvalRow(fields, sources);
            row.AddFields(rr);
            yield return row;
        }
    }

    private Source ResolveSource(
        SqlTableSource ts,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow = null)
    {
        var alias = ts.Alias ?? ts.TableFunction?.Name ?? ts.Name ?? ts.DbName ?? "t";

        Source source;

        if (ts.DerivedUnion is not null)
        {
            var res = ExecuteUnion(
                [.. ts.DerivedUnion.Parts
                    .Select(_=>_)
                    .Where(_=>_!= null)
                    .Select(_=>_!)],
                ts.DerivedUnion.AllFlags,
                ts.DerivedUnion.OrderBy,
                ts.DerivedUnion.RowLimit,
                ts.DerivedSql ?? "(derived)"
            );
            source = Source.FromResult(alias, res);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        if (ts.Derived is not null)
        {
            var res = ExecuteSelect(ts.Derived, ctes, outerRow);
            source = Source.FromResult(alias, res);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        if (ts.TableFunction is not null)
        {
            source = ResolveTableFunctionSource(ts.TableFunction, alias, ts.OpenJsonWithClause, ctes, outerRow);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        if (!string.IsNullOrWhiteSpace(ts.Name)
            && ctes.TryGetValue(ts.Name!, out var cteSrc))
        {
            source = cteSrc.WithAlias(alias);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        if (string.IsNullOrWhiteSpace(ts.Name))
            throw new InvalidOperationException("FROM sem nome de tabela/CTE/derived não suportado.");

        var tableName = ts.Name!.NormalizeName();

        if (_cnn.TryGetView(tableName, out var viewSelect, ts.DbName)
            && viewSelect != null)
        {
            var viewRes = ExecuteSelect(viewSelect, ctes, outerRow: null);
            source = Source.FromResult(alias, viewRes);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        if (tableName.Equals("DUAL", StringComparison.OrdinalIgnoreCase))
        {
            var one = new TableResultMock
            {
                ([])
            };
            source = Source.FromResult("DUAL", alias, one);
            return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
        }

        _cnn.Metrics.IncrementTableHint(tableName);
        var tb = _cnn.GetTable(tableName, ts.DbName);
        source = Source.FromPhysical(tableName, alias, tb, ts.MySqlIndexHints);
        return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
    }

    private Source ResolveTableFunctionSource(
        FunctionCallExpr function,
        string alias,
        SqlOpenJsonWithClause? openJsonWithClause,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        TableResultMock result;
        if (function.Name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase))
        {
            result = ExecuteOpenJsonTableFunction(function, alias, openJsonWithClause, ctes, outerRow);
            return Source.FromResult(function.Name, alias, result);
        }

        if (function.Name.Equals("STRING_SPLIT", StringComparison.OrdinalIgnoreCase))
        {
            result = ExecuteStringSplitTableFunction(function, alias, ctes, outerRow);
            return Source.FromResult(function.Name, alias, result);
        }

        throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");
    }

    private TableResultMock ExecuteOpenJsonTableFunction(
        FunctionCallExpr function,
        string alias,
        SqlOpenJsonWithClause? openJsonWithClause,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para OPENJSON.");
        if (!dialect.SupportsOpenJsonFunction)
            throw SqlUnsupported.ForDialect(dialect, "OPENJSON");

        if (function.Args.Count is < 1 or > 2)
            throw new NotSupportedException("OPENJSON table source currently supports one or two arguments in the mock.");

        var evalRow = CreateFunctionEvaluationRow(outerRow);
        var json = Eval(function.Args[0], evalRow, group: null, ctes);
        var result = openJsonWithClause is null
            ? CreateOpenJsonTableResult(alias)
            : CreateOpenJsonWithSchemaTableResult(alias, openJsonWithClause);

        if (IsNullish(json))
            return result;

        var path = function.Args.Count == 2
            ? Eval(function.Args[1], evalRow, group: null, ctes)?.ToString()
            : null;

        System.Text.Json.JsonElement target;
        try
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                using var document = System.Text.Json.JsonDocument.Parse(json!.ToString() ?? string.Empty);
                target = document.RootElement.Clone();
            }
            else
            {
                var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, path!);
                if (!lookup.Success)
                {
                    if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                        throw new InvalidOperationException($"OPENJSON path '{path}' is invalid in the mock.");

                    if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                        throw new InvalidOperationException($"OPENJSON strict path '{path}' was not found in the JSON payload.");

                    return result;
                }

                target = lookup.Value;
            }
        }
        catch (System.Text.Json.JsonException ex)
        {
            throw new InvalidOperationException("OPENJSON recebeu JSON inválido.", ex);
        }

        if (openJsonWithClause is not null)
        {
            foreach (var rowContext in EnumerateOpenJsonExplicitSchemaContexts(target))
                result.Add(ProjectOpenJsonExplicitSchemaRow(openJsonWithClause, rowContext));

            return result;
        }

        if (target.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            var index = 0;
            foreach (var item in target.EnumerateArray())
            {
                AddOpenJsonRow(result, index.ToString(CultureInfo.InvariantCulture), item);
                index++;
            }

            return result;
        }

        if (target.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var property in target.EnumerateObject())
                AddOpenJsonRow(result, property.Name, property.Value);

            return result;
        }

        AddOpenJsonRow(result, "0", target);
        return result;
    }

    private TableResultMock ExecuteStringSplitTableFunction(
        FunctionCallExpr function,
        string alias,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para STRING_SPLIT.");
        if (!dialect.SupportsStringSplitFunction)
            throw SqlUnsupported.ForDialect(dialect, "STRING_SPLIT");

        if (function.Args.Count is < 2 or > 3)
            throw new NotSupportedException("STRING_SPLIT table source currently supports two or three arguments in the mock.");

        var evalRow = CreateFunctionEvaluationRow(outerRow);
        var input = Eval(function.Args[0], evalRow, group: null, ctes);
        var separator = Eval(function.Args[1], evalRow, group: null, ctes)?.ToString() ?? string.Empty;
        var includeOrdinal = false;
        if (function.Args.Count == 3)
        {
            if (!dialect.SupportsStringSplitOrdinalArgument)
                throw SqlUnsupported.ForDialect(dialect, "STRING_SPLIT enable_ordinal");

            includeOrdinal = EvaluateStringSplitOrdinalFlag(
                Eval(function.Args[2], evalRow, group: null, ctes));
        }

        var result = CreateStringSplitTableResult(alias, includeOrdinal);

        if (IsNullish(input))
            return result;

        if (separator.Length != 1)
            throw new InvalidOperationException("STRING_SPLIT separator must be a single character in the mock.");

        var pieces = (input?.ToString() ?? string.Empty)
            .Split([separator], StringSplitOptions.None);

        for (var index = 0; index < pieces.Length; index++)
        {
            var row = new Dictionary<int, object?>
            {
                [0] = pieces[index]
            };

            if (includeOrdinal)
                row[1] = (long)index + 1L;

            result.Add(row);
        }

        return result;
    }

    private static TableResultMock CreateOpenJsonTableResult(string tableAlias)
        => new()
        {
            Columns =
            [
                new TableResultColMock(tableAlias, "key", "key", 0, DbType.String, false),
                new TableResultColMock(tableAlias, "value", "value", 1, DbType.String, true),
                new TableResultColMock(tableAlias, "type", "type", 2, DbType.Int32, false)
            ]
        };

    private static TableResultMock CreateOpenJsonWithSchemaTableResult(
        string tableAlias,
        SqlOpenJsonWithClause withClause)
        => new()
        {
            Columns = withClause.Columns
                .Select((column, index) => new TableResultColMock(
                    tableAlias,
                    column.Name,
                    column.Name,
                    index,
                    column.DbType,
                    true,
                    column.AsJson))
                .ToList()
        };

    private static TableResultMock CreateStringSplitTableResult(string tableAlias, bool includeOrdinal)
    {
        var columns = new List<TableResultColMock>
        {
            new(tableAlias, "value", "value", 0, DbType.String, true)
        };

        if (includeOrdinal)
            columns.Add(new TableResultColMock(tableAlias, "ordinal", "ordinal", 1, DbType.Int64, false));

        return new TableResultMock
        {
            Columns = columns
        };
    }

    private static bool EvaluateStringSplitOrdinalFlag(object? rawValue)
    {
        if (rawValue is null or DBNull)
            throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");

        if (rawValue is bool boolean)
            return boolean;

        if (rawValue is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            var numeric = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);
            return numeric switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        if (rawValue is decimal or double or float)
        {
            var numeric = Convert.ToDecimal(rawValue, CultureInfo.InvariantCulture);
            return numeric switch
            {
                0m => false,
                1m => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        var text = rawValue.ToString()?.Trim();
        if (string.Equals(text, "0", StringComparison.Ordinal))
            return false;

        if (string.Equals(text, "1", StringComparison.Ordinal))
            return true;

        if (decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedNumeric))
        {
            return parsedNumeric switch
            {
                0m => false,
                1m => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");
    }

    private static void AddOpenJsonRow(
        TableResultMock result,
        string key,
        System.Text.Json.JsonElement value)
    {
        result.Add(new Dictionary<int, object?>
        {
            [0] = key,
            [1] = ConvertOpenJsonValue(value),
            [2] = GetOpenJsonType(value)
        });
    }

    private static object? ConvertOpenJsonValue(System.Text.Json.JsonElement value)
        => value.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.String => value.GetString(),
            System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array => value.GetRawText(),
            _ => value.ToString()
        };

    private static int GetOpenJsonType(System.Text.Json.JsonElement value)
        => value.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => 0,
            System.Text.Json.JsonValueKind.String => 1,
            System.Text.Json.JsonValueKind.Number => 2,
            System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => 3,
            System.Text.Json.JsonValueKind.Array => 4,
            System.Text.Json.JsonValueKind.Object => 5,
            _ => 0
        };

    private static IEnumerable<System.Text.Json.JsonElement> EnumerateOpenJsonExplicitSchemaContexts(System.Text.Json.JsonElement target)
    {
        if (target.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var item in target.EnumerateArray())
                yield return item;

            yield break;
        }

        yield return target;
    }

    private static Dictionary<int, object?> ProjectOpenJsonExplicitSchemaRow(
        SqlOpenJsonWithClause withClause,
        System.Text.Json.JsonElement rowContext)
    {
        var row = new Dictionary<int, object?>();
        for (var i = 0; i < withClause.Columns.Count; i++)
            row[i] = ResolveOpenJsonExplicitColumnValue(rowContext, withClause.Columns[i]);

        return row;
    }

    private static object? ResolveOpenJsonExplicitColumnValue(
        System.Text.Json.JsonElement rowContext,
        SqlOpenJsonWithColumn column)
    {
        var resolution = ResolveOpenJsonExplicitColumnElement(rowContext, column);
        if (!resolution.Success)
        {
            if (resolution.InvalidPath)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' uses an invalid JSON path '{resolution.Path}'.");

            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH strict path '{resolution.Path}' for column '{column.Name}' was not found in the JSON payload.");

            return null;
        }

        var valueElement = resolution.Value;

        if (column.AsJson)
        {
            if (valueElement.ValueKind is System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array)
                return valueElement.GetRawText();

            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' requires an object or array at strict path '{resolution.Path}' when AS JSON is used.");

            return null;
        }

        if (valueElement.ValueKind is System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array)
        {
            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' requires a scalar value at strict path '{resolution.Path}'.");

            return null;
        }

        var scalarText = ConvertOpenJsonExplicitScalarToText(valueElement);
        return scalarText is null ? null : column.DbType.Parse(scalarText);
    }

    private static OpenJsonColumnResolution ResolveOpenJsonExplicitColumnElement(
        System.Text.Json.JsonElement rowContext,
        SqlOpenJsonWithColumn column)
    {
        if (!string.IsNullOrWhiteSpace(column.Path))
        {
            var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, column.Path!);
            return new OpenJsonColumnResolution(
                lookup.Success,
                lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict,
                lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath,
                column.Path,
                lookup.Value);
        }

        if (rowContext.ValueKind == System.Text.Json.JsonValueKind.Object
            && rowContext.TryGetProperty(column.Name, out var valueElement))
        {
            return new OpenJsonColumnResolution(true, false, false, null, valueElement);
        }

        return new OpenJsonColumnResolution(false, false, false, null, default);
    }

    private static string? ConvertOpenJsonExplicitScalarToText(System.Text.Json.JsonElement valueElement)
        => valueElement.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.String => valueElement.GetString(),
            System.Text.Json.JsonValueKind.True => "true",
            System.Text.Json.JsonValueKind.False => "false",
            _ => valueElement.ToString()
        };

    private readonly record struct OpenJsonColumnResolution(
        bool Success,
        bool IsStrict,
        bool InvalidPath,
        string? Path,
        System.Text.Json.JsonElement Value);

    private readonly record struct AutoJsonProjection(
        int ColumnIndex,
        string? Qualifier,
        string PropertyName,
        bool IsJsonFragment);

    private sealed class AutoJsonRootRow(Dictionary<string, object?> properties)
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

    private static EvalRow CreateFunctionEvaluationRow(EvalRow? outerRow)
        => outerRow ?? new EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));

    private Source ApplyTableTransformsIfNeeded(
        Source source,
        SqlPivotSpec? pivot,
        SqlUnpivotSpec? unpivot,
        IDictionary<string, Source> ctes)
    {
        source = ApplyPivotIfNeeded(source, pivot, ctes);
        source = ApplyUnpivotIfNeeded(source, unpivot);
        return source;
    }

    private Source ApplyPivotIfNeeded(Source source, SqlPivotSpec? pivot, IDictionary<string, Source> ctes)
    {
        if (pivot is null)
            return source;

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para PIVOT.");
        var inputRows = MaterializeSourceRows(source);

        var forExpr = ParseExpr(pivot.ForColumnRaw);
        var aggArgExpr = ParseExpr(pivot.AggregateArgRaw);

        var forValues = inputRows.ToDictionary(
            r => r,
            r => Eval(forExpr, r, group: null, ctes),
            ReferenceEqualityComparer<EvalRow>.Instance);

        var inItems = pivot.InItems
            .Select(i => new { i.Alias, Value = Eval(ParseExpr(i.ValueRaw), new EvalRow(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)), group: null, ctes) })
            .ToList();

        var forColumnNormalized = pivot.ForColumnRaw[(pivot.ForColumnRaw.LastIndexOf('.') + 1)..];
        var aggregateArgNormalized = pivot.AggregateArgRaw[(pivot.AggregateArgRaw.LastIndexOf('.') + 1)..];
        var groupColumns = source.ColumnNames
            .Where(c => !c.Equals(pivot.ForColumnRaw, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(forColumnNormalized, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(pivot.AggregateArgRaw, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(aggregateArgNormalized, StringComparison.OrdinalIgnoreCase))
            .ToList();

        static string BuildGroupKey(EvalRow row, IEnumerable<string> columns)
            => string.Join("", columns.Select(c => row.GetByName(c)?.ToString() ?? "<null>"));

        var grouped = inputRows.GroupBy(r => BuildGroupKey(r, groupColumns)).ToList();
        var result = new TableResultMock();

        for (int i = 0; i < groupColumns.Count; i++)
        {
            var dbType = TryGetSourceColumnDbType(source, groupColumns[i]) ?? DbType.Object;
            var isNullable = TryGetSourceColumnIsNullable(source, groupColumns[i]) ?? true;
            result.Columns.Add(new TableResultColMock(source.Alias, groupColumns[i], groupColumns[i], i, dbType, isNullable));
        }

        var pivotAggregateDbType = GetPivotAggregateResultDbType(pivot.AggregateFunction, aggArgExpr, source, dialect);
        for (int i = 0; i < inItems.Count; i++)
            result.Columns.Add(new TableResultColMock(source.Alias, inItems[i].Alias, inItems[i].Alias, groupColumns.Count + i, pivotAggregateDbType, true));

        foreach (var group in grouped)
        {
            var first = group.First();
            var outRow = new Dictionary<int, object?>();

            for (int i = 0; i < groupColumns.Count; i++)
                outRow[i] = first.GetByName(groupColumns[i]);

            for (int i = 0; i < inItems.Count; i++)
            {
                var bucket = group.Where(r => forValues[r].EqualsSql(inItems[i].Value, dialect)).ToList();
                var aggregated = AggregatePivotBucket(pivot.AggregateFunction, aggArgExpr, bucket, ctes);
                outRow[groupColumns.Count + i] = CoercePivotAggregateValue(aggregated, pivot.AggregateFunction, pivotAggregateDbType);
            }

            result.Add(outRow);
        }

        return Source.FromResult(source.Name, source.Alias, result);
    }

    private static DbType GetPivotAggregateResultDbType(string aggregateFunction, SqlExpr aggArgExpr, Source source, ISqlDialect dialect)
        => aggregateFunction.ToUpperInvariant() switch
        {
            "COUNT" => DbType.Int32,
            "COUNT_BIG" => DbType.Int64,
            "STDEV" or "STDEVP" or "VAR" or "VARP" => DbType.Double,
            "SUM" => PromotePivotSumResultDbType(TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object),
            "AVG" => PromotePivotAvgResultDbType(TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object, dialect),
            "MIN" or "MAX" => TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object,
            _ => DbType.Object
        };

    private static DbType PromotePivotSumResultDbType(DbType inputType)
        => inputType switch
        {
            DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32 => DbType.Int32,
            DbType.UInt32 or DbType.Int64 or DbType.UInt64 => DbType.Int64,
            DbType.Single or DbType.Double => DbType.Double,
            DbType.Currency or DbType.Decimal or DbType.VarNumeric => DbType.Decimal,
            _ => inputType
        };

    private static DbType PromotePivotAvgResultDbType(DbType inputType, ISqlDialect dialect)
    {
        if (dialect.PivotAvgReturnsDecimalForIntegralInputs && IsIntegralPivotDbType(inputType))
            return DbType.Decimal;

        return inputType switch
        {
            DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32 => DbType.Int32,
            DbType.UInt32 or DbType.Int64 or DbType.UInt64 => DbType.Int64,
            DbType.Currency or DbType.Decimal or DbType.VarNumeric => DbType.Decimal,
            DbType.Single or DbType.Double => DbType.Double,
            _ => inputType
        };
    }

    private static bool IsIntegralPivotDbType(DbType inputType)
        => inputType is DbType.Byte
            or DbType.SByte
            or DbType.Int16
            or DbType.UInt16
            or DbType.Int32
            or DbType.UInt32
            or DbType.Int64
            or DbType.UInt64;

    private static object? CoercePivotAggregateValue(object? value, string aggregateFunction, DbType targetDbType)
    {
        if (value is null)
            return null;

        if (aggregateFunction.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("AVG", StringComparison.OrdinalIgnoreCase))
        {
            return targetDbType switch
            {
                DbType.Int32 => CoercePivotIntegerLikeValue<int>(value),
                DbType.Int64 => CoercePivotIntegerLikeValue<long>(value),
                DbType.Double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                _ => value
            };
        }

        return value;
    }

    private static TInteger CoercePivotIntegerLikeValue<TInteger>(object value)
        where TInteger : struct
    {
        var decimalValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        var truncated = decimal.Truncate(decimalValue);

        if (typeof(TInteger) == typeof(int))
            return (TInteger)(object)decimal.ToInt32(truncated);

        if (typeof(TInteger) == typeof(long))
            return (TInteger)(object)decimal.ToInt64(truncated);

        throw new NotSupportedException($"Integer-like coercion to '{typeof(TInteger).Name}' is not supported.");
    }

    private static DbType? TryGetPivotAggregateArgumentDbType(SqlExpr aggArgExpr, Source source)
    {
        if (aggArgExpr is not IdentifierExpr identifier)
            return null;

        return ResolveSourceColumnDbType(source, identifier.Name);
    }

    private static DbType? TryGetSourceColumnDbType(Source source, string columnName)
        => TryGetSourceColumnMetadata(source, columnName)?.DbType;

    private static bool? TryGetSourceColumnIsNullable(Source source, string columnName)
        => TryGetSourceColumnMetadata(source, columnName)?.IsNullable;

    private static TableResultColMock? TryGetSourceColumnMetadata(Source source, string columnName)
    {
        var normalizedColumnName = columnName[(columnName.LastIndexOf('.') + 1)..];

        return source.TryGetColumnMetadata(columnName, out var qualifiedMetadata)
            ? qualifiedMetadata
            : source.TryGetColumnMetadata(normalizedColumnName, out var metadata)
                ? metadata
                : null;
    }

    private static DbType? ResolveSourceColumnDbType(Source source, string columnName)
    {
        var metadataDbType = TryGetSourceColumnDbType(source, columnName);
        var sampledDbType = TryInferSourceColumnDbTypeFromRows(source, columnName);

        if (sampledDbType is DbType.Int32
            && metadataDbType is DbType.Decimal or DbType.Object or null)
        {
            return DbType.Int32;
        }

        if (sampledDbType is DbType.Double
            && metadataDbType is DbType.Decimal or DbType.Object or null)
        {
            return DbType.Double;
        }

        return metadataDbType ?? sampledDbType;
    }

    private static DbType? TryInferSourceColumnDbTypeFromRows(Source source, string columnName)
    {
        var normalizedColumnName = columnName[(columnName.LastIndexOf('.') + 1)..];
        var qualifiedColumnName = $"{source.Alias}.{normalizedColumnName}";

        foreach (var row in source.Rows())
        {
            if (!row.TryGetValue(qualifiedColumnName, out var value) || value is null or DBNull)
                continue;

            if (value is decimal dec
                && decimal.Truncate(dec) == dec
                && dec >= int.MinValue
                && dec <= int.MaxValue)
            {
                return DbType.Int32;
            }

            var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
            if (type == typeof(float) || type == typeof(double))
                return DbType.Double;

            try
            {
                return type.ConvertTypeToDbType();
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        return null;
    }

    private static DbType? ResolveUnpivotValueDbType(Source source, SqlUnpivotSpec unpivot)
    {
        DbType? resolved = null;
        foreach (var item in unpivot.InItems)
        {
            var next = ResolveSourceColumnDbType(source, item.SourceColumnName) ?? DbType.Object;
            if (resolved is null)
            {
                resolved = next;
                continue;
            }

            if (resolved != next)
                return DbType.Object;
        }

        return resolved;
    }

    private Source ApplyUnpivotIfNeeded(Source source, SqlUnpivotSpec? unpivot)
    {
        if (unpivot is null)
            return source;

        var inputRows = MaterializeSourceRows(source);
        var inColumns = new HashSet<string>(
            unpivot.InItems.Select(static item => item.SourceColumnName),
            StringComparer.OrdinalIgnoreCase);

        foreach (var item in unpivot.InItems)
        {
            if (!source.ColumnNames.Any(column => column.Equals(item.SourceColumnName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"UNPIVOT source column '{item.SourceColumnName}' was not found in the input rowset.");
        }

        var groupColumns = source.ColumnNames
            .Where(column => !inColumns.Contains(column))
            .ToList();

        var result = new TableResultMock();
        for (var index = 0; index < groupColumns.Count; index++)
        {
            var dbType = ResolveSourceColumnDbType(source, groupColumns[index]) ?? DbType.Object;
            var isNullable = TryGetSourceColumnIsNullable(source, groupColumns[index]) ?? true;
            result.Columns.Add(new TableResultColMock(source.Alias, groupColumns[index], groupColumns[index], index, dbType, isNullable));
        }

        result.Columns.Add(new TableResultColMock(source.Alias, unpivot.NameColumnName, unpivot.NameColumnName, groupColumns.Count, DbType.String, false));
        var unpivotValueDbType = ResolveUnpivotValueDbType(source, unpivot) ?? DbType.Object;
        result.Columns.Add(new TableResultColMock(source.Alias, unpivot.ValueColumnName, unpivot.ValueColumnName, groupColumns.Count + 1, unpivotValueDbType, false));

        foreach (var row in inputRows)
        {
            foreach (var item in unpivot.InItems)
            {
                var value = row.GetByName(item.SourceColumnName);
                if (IsNullish(value))
                    continue;

                var outRow = new Dictionary<int, object?>();
                for (var index = 0; index < groupColumns.Count; index++)
                    outRow[index] = row.GetByName(groupColumns[index]);

                outRow[groupColumns.Count] = item.OutputName;
                outRow[groupColumns.Count + 1] = value;
                result.Add(outRow);
            }
        }

        return Source.FromResult(source.Name, source.Alias, result);
    }

    private static List<EvalRow> MaterializeSourceRows(Source source)
        => source.Rows()
            .Select(fields =>
            {
                var rowSources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
                {
                    [source.Alias] = source
                };
                if (!source.Name.Equals(source.Alias, StringComparison.OrdinalIgnoreCase))
                    rowSources[source.Name] = source;
                return new EvalRow(new Dictionary<string, object?>(fields, StringComparer.OrdinalIgnoreCase), rowSources);
            })
            .ToList();

    private object? AggregatePivotBucket(string aggregateFunction, SqlExpr aggArgExpr, List<EvalRow> rows, IDictionary<string, Source> ctes)
    {
        if (aggregateFunction.Equals("COUNT", StringComparison.OrdinalIgnoreCase))
        {
            if (aggArgExpr is StarExpr)
                return rows.Count;

            var count = 0;
            foreach (var row in rows)
            {
                var value = Eval(aggArgExpr, row, group: null, ctes);
                if (!IsNullish(value))
                    count++;
            }

            return count;
        }

        if (aggregateFunction.Equals("COUNT_BIG", StringComparison.OrdinalIgnoreCase))
        {
            if (aggArgExpr is StarExpr)
                return (long)rows.Count;

            long count = 0;
            foreach (var row in rows)
            {
                var value = Eval(aggArgExpr, row, group: null, ctes);
                if (!IsNullish(value))
                    count++;
            }

            return count;
        }

        if (aggregateFunction.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("AVG", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("MIN", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("MAX", StringComparison.OrdinalIgnoreCase))
        {
            var group = new EvalGroup(rows);
            var aggregateExpr = new FunctionCallExpr(aggregateFunction, [aggArgExpr]);
            return EvalAggregate(aggregateExpr, group, ctes);
        }

        if (aggregateFunction.Equals("STDEV", StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: true, squareRoot: true);

        if (aggregateFunction.Equals("STDEVP", StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: false, squareRoot: true);

        if (aggregateFunction.Equals("VAR", StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: true, squareRoot: false);

        if (aggregateFunction.Equals("VARP", StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: false, squareRoot: false);

        throw new NotSupportedException($"PIVOT aggregate '{aggregateFunction}' not supported yet.");
    }

    private double? EvaluatePivotVarianceAggregate(
        IReadOnlyList<EvalRow> rows,
        SqlExpr aggArgExpr,
        IDictionary<string, Source> ctes,
        bool sample,
        bool squareRoot)
    {
        var values = new List<double>(rows.Count);
        foreach (var row in rows)
        {
            var rawValue = Eval(aggArgExpr, row, group: null, ctes);
            if (IsNullish(rawValue))
                continue;

            values.Add(Convert.ToDouble(rawValue, CultureInfo.InvariantCulture));
        }

        if (values.Count == 0)
            return null;

        if (sample && values.Count < 2)
            return null;

        var mean = values.Average();
        var sumOfSquaredDifferences = 0d;
        foreach (var value in values)
        {
            var difference = value - mean;
            sumOfSquaredDifferences += difference * difference;
        }

        var divisor = sample ? values.Count - 1 : values.Count;
        if (divisor <= 0)
            return null;

        var variance = sumOfSquaredDifferences / divisor;
        return squareRoot ? Math.Sqrt(variance) : variance;
    }

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
        foreach (var r in rows)
        {
            var outRow = new Dictionary<int, object?>();
            for (int i = 0; i < selectPlan.Evaluators.Count; i++)
                outRow[i] = selectPlan.Evaluators[i](r, null);

            res.Add(outRow);
            res.JoinFields.Add(new Dictionary<string, object?>(r.Fields, StringComparer.OrdinalIgnoreCase));
        }

        return res;
    }

    private TableResultMock ProjectGrouped(
        SqlSelectQuery q,
        IEnumerable<IGrouping<GroupKey, EvalRow>> groups,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var res = new TableResultMock();
        var groupsList = groups.Select(g => new { g.Key, Rows = g.ToList() }).ToList();
        var hasGroups = groupsList.Count > 0;

        // SQL aggregate semantics: when no GROUP BY is present and the filtered input is empty,
        // aggregate projections (e.g. COUNT(*)) still return a single row.
        if (!hasGroups && q.GroupBy.Count == 0)
            groupsList.Add(new { Key = default(GroupKey), Rows = new List<EvalRow>() });

        var selectPlan = BuildSelectPlan(
            q,
            hasGroups
                ? groupsList.ConvertAll(g => g.Rows[0])
                : [],
            ctes);

        // columns
        for (int i = 0; i < selectPlan.Columns.Count; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        // rows
        foreach (var g in groupsList)
        {
            var eg = new EvalGroup(g.Rows);
            var outRow = new Dictionary<int, object?>();

            var first = g.Rows.Count > 0 ? g.Rows[0] : EvalRow.Empty();
            for (int i = 0; i < selectPlan.Evaluators.Count; i++)
                outRow[i] = selectPlan.Evaluators[i](first, eg);

            res.Add(outRow);
            res.JoinFields.Add(new Dictionary<string, object?>(first.Fields, StringComparer.OrdinalIgnoreCase));
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
        res = ApplyForJsonIfNeeded(res, q, debugTrace);
        return res;
    }

    private void ComputeWindowSlots(
        List<WindowSlot> slots,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        if (slots.Count == 0 || rows.Count == 0)
            return;

        foreach (var slot in slots)
        {
            var w = slot.Expr;

            var isRowNumber = Dialect!.IsRowNumberWindowFunction(w.Name);
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
            if (!isRowNumber && !isRank && !isDenseRank && !isNtile && !isPercentRank && !isCumeDist && !isLag && !isLead && !isFirstValue && !isLastValue && !isNthValue)
                throw SqlUnsupported.ForDialect(
                    Dialect ?? throw new InvalidOperationException("Dialect is required for window function validation."),
                    $"window functions ({w.Name})");

            if (Dialect?.RequiresOrderByInWindowFunction(w.Name) == true && w.Spec.OrderBy.Count == 0)
                throw new InvalidOperationException($"Window function '{w.Name}' requires ORDER BY in OVER clause.");

            var partitions = WindowPartitionHelper.BuildPartitions(
                w,
                rows,
                (expr, row) => Eval(expr, row, null, ctes),
                value => NormalizeDistinctKey(value));

            foreach (var part in partitions.Values)
            {
                WindowPartitionHelper.SortPartition(
                    part,
                    w.Spec.OrderBy,
                    (expr, row) => Eval(expr, row, null, ctes),
                    CompareSql);

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
                    FillNtile(slot.Map, part, w, ctes);
                    continue;
                }

                if (isPercentRank || isCumeDist)
                {
                    FillPercentRankOrCumeDist(slot.Map, part, w.Spec.Frame, w.Spec.OrderBy, ctes, isPercentRank);
                    continue;
                }

                if (isLag || isLead)
                {
                    FillLagOrLead(slot.Map, part, w, ctes, isLead);
                    continue;
                }

                if (isFirstValue || isLastValue)
                {
                    FillFirstOrLastValue(slot.Map, part, w, ctes, isLastValue);
                    continue;
                }

                if (isNthValue)
                {
                    FillNthValue(slot.Map, part, w, ctes);
                    continue;
                }

                FillRankOrDenseRank(slot.Map, part, w, ctes, isRank);
            }
        }
    }








        /// <summary>
    /// EN: Fills FIRST_VALUE/LAST_VALUE results for all rows in the current partition.
    /// PT: Preenche os resultados de FIRST_VALUE/LAST_VALUE para todas as linhas da partição atual.
    /// </summary>
private void FillFirstOrLastValue(
        Dictionary<EvalRow, object?> map,
        List<EvalRow> part,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillLast)
    {
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, i, windowFunctionExpr.Spec.OrderBy, ctes);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
            map[part[i]] = Eval(valueExpr, part[targetIndex], null, ctes);
        }
    }


        /// <summary>
    /// EN: Fills NTH_VALUE results using the resolved 1-based index in the ordered partition.
    /// PT: Preenche os resultados de NTH_VALUE usando o índice 1-based resolvido na partição ordenada.
    /// </summary>
private void FillNthValue(
        Dictionary<EvalRow, object?> map,
        List<EvalRow> part,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes)
    {
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var nth = ResolveNthValueIndex(windowFunctionExpr.Args, part[0], ctes);
        if (nth <= 0)
            return;

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, i, windowFunctionExpr.Spec.OrderBy, ctes);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = frameRange.StartIndex + (nth - 1);
            map[part[i]] = targetIndex <= frameRange.EndIndex
                ? Eval(valueExpr, part[targetIndex], null, ctes)
                : null;
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
        IDictionary<string, Source> ctes)
    {
        if (part.Count == 0)
            return RowsFrameRange.Empty;

        if (frame is null || frame.Unit == WindowFrameUnit.Rows)
            return WindowFrameRangeResolver.ResolveRowsFrameRange(frame, part.Count, rowIndex);

        if (orderBy.Count == 0)
            throw new InvalidOperationException($"Window frame unit '{frame.Unit}' requires ORDER BY in OVER clause.");

        var orderValuesByRow = WindowOrderValueHelper.BuildWindowOrderValuesByRow(
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
        if (expr is LiteralExpr lit)
        {
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
        }

        return false;
    }

    private static bool TryReadLongLiteral(SqlExpr expr, out long value)
    {
        value = default;
        if (expr is LiteralExpr lit)
        {
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
        List<EvalRow> part,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillLead)
    {
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var offset = ResolveLagLeadOffset(windowFunctionExpr.Args, part[0], ctes);
        var defaultExpr = windowFunctionExpr.Args.Count >= 3 ? windowFunctionExpr.Args[2] : null;

        for (int i = 0; i < part.Count; i++)
        {
            var targetIndex = fillLead ? i + offset : i - offset;
            var currentRow = part[i];
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, i, windowFunctionExpr.Spec.OrderBy, ctes);

            if (!frameRange.IsEmpty && targetIndex >= frameRange.StartIndex && targetIndex <= frameRange.EndIndex)
            {
                map[currentRow] = Eval(valueExpr, part[targetIndex], null, ctes);
                continue;
            }

            map[currentRow] = defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
        }
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
        List<EvalRow> part,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillRank)
    {
        if (part.Count == 0)
            return;

        var orderValuesByRow = WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            part,
            windowFunctionExpr.Spec.OrderBy,
            (expr, row) => Eval(expr, row, null, ctes));

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, i, windowFunctionExpr.Spec.OrderBy, ctes);
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
        List<EvalRow> part,
        WindowFrameSpec? frame,
        IReadOnlyList<WindowOrderItem> orderBy,
        IDictionary<string, Source> ctes,
        bool fillPercentRank)
    {
        if (part.Count == 0)
            return;

        var orderValuesByRow = WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            part,
            orderBy,
            (expr, row) => Eval(expr, row, null, ctes));

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var row = part[rowIndex];
            var frameRange = ResolveWindowFrameRange(frame, part, rowIndex, orderBy, ctes);
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
        List<EvalRow> part,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes)
    {
        if (part.Count == 0)
            return;

        var bucketCount = ResolveNtileBucketCount(windowFunctionExpr, part.Count, part[0], ctes);
        if (bucketCount <= 0)
            return;

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, rowIndex, windowFunctionExpr.Spec.OrderBy, ctes);
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
        => SelectPlanBuilderHelper.Build(
            q,
            sampleRows,
            ctes,
            Dialect,
            ParseScalarExpr,
            Eval,
            ResolveColumn);

    private void EnsureDialectSupportsSequenceFunction(string? functionName)
        => SequenceFunctionSupportHelper.EnsureSupported(Dialect, functionName);

    // Remove "AS alias" somente quando:
    // - está no FINAL do select item
    // - e esse "AS" está fora de parênteses (pra não quebrar CAST(x AS CHAR))
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
    {
        // ORDER BY (aliases/ordinals/expressions) + LIMIT/OFFSET

        // LIMIT/OFFSET sem ORDER BY ainda precisa aplicar
        if (q.OrderBy.Count == 0)
        {
            var limitInput = res.Count;
            QueryRowLimitHelper.ApplyLimit(res, q);
            if (debugTrace is not null && q.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    res.Count,
                    TimeSpan.Zero,
                    QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(q.RowLimit));
            }
            return res;
        }

        var sortStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var sortInput = res.Count;
        var sorted = QueryOrderByHelper.TryApplyOrder(
            res,
            q.OrderBy,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            CompareSql);
        if (!sorted)
        {
            var limitInput = res.Count;
            QueryRowLimitHelper.ApplyLimit(res, q);
            if (debugTrace is not null && q.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    res.Count,
                    TimeSpan.Zero,
                    QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(q.RowLimit));
            }
            return res;
        }

        debugTrace?.AddStep(
            "Sort",
            sortInput,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(sortStart)),
            QueryDebugTraceFormattingHelper.FormatOrderByDebugDetails(q.OrderBy));

        var limitInputRows = res.Count;
        QueryRowLimitHelper.ApplyLimit(res, q);
        if (debugTrace is not null && q.RowLimit is not null)
        {
            debugTrace.AddStep(
                "Limit",
                limitInputRows,
                res.Count,
                TimeSpan.Zero,
                QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(q.RowLimit));
        }
        return res;
    }

    private TableResultMock ApplyForJsonIfNeeded(
        TableResultMock result,
        SqlSelectQuery query,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        if (query.ForJson is null)
            return result;

        var serializeStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var serialized = SerializeForJson(result, query);
        debugTrace?.AddStep(
            "ForJson",
            result.Count,
            serialized.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(serializeStart)),
            $"{query.ForJson.Mode.ToString().ToUpperInvariant()}{(query.ForJson.RootName is null ? string.Empty : $";root={query.ForJson.RootName}")}");
        return serialized;
    }

    private static TableResultMock SerializeForJson(TableResultMock result, SqlSelectQuery query)
    {
        var clause = query.ForJson ?? throw new InvalidOperationException("FOR JSON clause expected by serializer.");
        var payload = clause.Mode switch
        {
            SqlForJsonMode.Path => SerializeForJsonPath(result, clause),
            SqlForJsonMode.Auto => SerializeForJsonAuto(result, query, clause),
            _ => throw new NotSupportedException($"FOR JSON mode '{clause.Mode}' not supported in the mock.")
        };

        var table = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock(string.Empty, SqlServerForJsonColumnName, SqlServerForJsonColumnName, 0, DbType.String, false)
            ]
        };
        table.Add(new Dictionary<int, object?> { [0] = payload });
        return table;
    }

    private static string SerializeForJsonPath(TableResultMock result, SqlForJsonClause clause)
    {
        ValidateForJsonPathProjectionOrder(result);

        var rowJson = new List<string>(result.Count);
        foreach (var row in result)
            rowJson.Add(System.Text.Json.JsonSerializer.Serialize(BuildPathJsonObject(result, row, clause.IncludeNullValues)));

        return WrapForJsonPayload(rowJson, clause);
    }

    private static string SerializeForJsonAuto(TableResultMock result, SqlSelectQuery query, SqlForJsonClause clause)
    {
        var projections = BuildAutoJsonProjections(result, query);
        var rootAlias = query.Table?.Alias ?? query.Table?.Name;

        var grouped = new List<AutoJsonRootRow>();
        var groupedIndex = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var row in result)
        {
            var rootProperties = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var nestedByAlias = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase);
            var nestedHasNonNullValueByAlias = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < projections.Count; i++)
            {
                var projection = projections[i];
                var value = row.TryGetValue(projection.ColumnIndex, out var rawValue) ? rawValue : null;

                if (projection.Qualifier is null
                    || rootAlias is null
                    || projection.Qualifier.Equals(rootAlias, StringComparison.OrdinalIgnoreCase))
                {
                    AddJsonProperty(rootProperties, projection.PropertyName, value, clause.IncludeNullValues, projection.IsJsonFragment);
                    continue;
                }

                if (!nestedByAlias.TryGetValue(projection.Qualifier, out var nested))
                {
                    nested = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    nestedByAlias[projection.Qualifier] = nested;
                    nestedHasNonNullValueByAlias[projection.Qualifier] = false;
                }

                AddJsonProperty(nested, projection.PropertyName, value, clause.IncludeNullValues, projection.IsJsonFragment);
                if (NormalizeForJsonValue(value, projection.IsJsonFragment) is not null)
                    nestedHasNonNullValueByAlias[projection.Qualifier] = true;
            }

            var rootKey = System.Text.Json.JsonSerializer.Serialize(rootProperties);
            if (!groupedIndex.TryGetValue(rootKey, out var rootIndex))
            {
                rootIndex = grouped.Count;
                groupedIndex[rootKey] = rootIndex;
                grouped.Add(new AutoJsonRootRow(rootProperties));
            }

            var groupedRoot = grouped[rootIndex];
            foreach (var nested in nestedByAlias)
            {
                if (nested.Value.Count == 0
                    || !nestedHasNonNullValueByAlias.TryGetValue(nested.Key, out var hasNonNullValue)
                    || !hasNonNullValue)
                    continue;

                if (!groupedRoot.Nested.TryGetValue(nested.Key, out var items))
                {
                    items = [];
                    groupedRoot.Nested[nested.Key] = items;
                }

                items.Add(nested.Value);
            }
        }

        var serializedRows = grouped
            .Select(static groupedRow => System.Text.Json.JsonSerializer.Serialize(groupedRow.ToJsonObject()))
            .ToList();

        return WrapForJsonPayload(serializedRows, clause);
    }

    private static string WrapForJsonPayload(IReadOnlyList<string> serializedRows, SqlForJsonClause clause)
    {
        var payload = clause.WithoutArrayWrapper
            ? serializedRows.Count switch
            {
                0 => "[]",
                1 => serializedRows[0],
                _ => string.Join(",", serializedRows)
            }
            : $"[{string.Join(",", serializedRows)}]";

        if (clause.RootName is null)
            return payload;

        return "{" + System.Text.Json.JsonSerializer.Serialize(clause.RootName) + ":" + payload + "}";
    }

    private static Dictionary<string, object?> BuildPathJsonObject(
        TableResultMock result,
        Dictionary<int, object?> row,
        bool includeNullValues)
    {
        var root = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        for (var index = 0; index < result.Columns.Count; index++)
        {
            var column = result.Columns[index];
            var value = row.TryGetValue(index, out var rawValue) ? rawValue : null;
            AddPathJsonProperty(root, column.ColumnAlias, value, includeNullValues, column.IsJsonFragment);
        }

        return root;
    }

    private static void ValidateForJsonPathProjectionOrder(TableResultMock result)
    {
        var terminalPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var objectPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var closedObjectPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string[]? previousSegments = null;

        foreach (var column in result.Columns)
        {
            var segments = column.ColumnAlias
                .Split('.')
                .Select(static segment => segment.Trim())
                .Where(static segment => !string.IsNullOrWhiteSpace(segment))
                .ToArray();
            if (segments.Length == 0)
                continue;

            if (previousSegments is not null)
            {
                var commonPrefixLength = 0;
                var maxShared = Math.Min(previousSegments.Length, segments.Length);
                while (commonPrefixLength < maxShared
                    && previousSegments[commonPrefixLength].Equals(segments[commonPrefixLength], StringComparison.OrdinalIgnoreCase))
                {
                    commonPrefixLength++;
                }

                for (var depth = commonPrefixLength + 1; depth < previousSegments.Length; depth++)
                    closedObjectPaths.Add(BuildJsonPath(previousSegments, depth));

                for (var depth = 1; depth < commonPrefixLength; depth++)
                {
                    var sharedPrefix = BuildJsonPath(segments, depth);
                    if (closedObjectPaths.Contains(sharedPrefix))
                        throw CreateForJsonPathConflictException(column.ColumnAlias);
                }
            }

            for (var depth = 1; depth < segments.Length; depth++)
            {
                var prefix = BuildJsonPath(segments, depth);
                if (closedObjectPaths.Contains(prefix))
                    throw CreateForJsonPathConflictException(column.ColumnAlias);

                if (terminalPaths.Contains(prefix))
                    throw CreateForJsonPathConflictException(column.ColumnAlias);
            }

            var fullPath = BuildJsonPath(segments, segments.Length);
            if (terminalPaths.Contains(fullPath) || objectPaths.Contains(fullPath))
                throw CreateForJsonPathConflictException(column.ColumnAlias);

            for (var depth = 1; depth < segments.Length; depth++)
                objectPaths.Add(BuildJsonPath(segments, depth));

            terminalPaths.Add(fullPath);
            previousSegments = segments;
        }
    }

    private static InvalidOperationException CreateForJsonPathConflictException(string propertyPath)
        => new($"Property '{propertyPath}' cannot be generated in JSON output due to a conflict with another column name or alias in the FOR JSON PATH projection order.");

    private static string BuildJsonPath(string[] segments, int length)
        => string.Join(".", segments.Take(length));

    private static void AddPathJsonProperty(
        Dictionary<string, object?> root,
        string propertyPath,
        object? value,
        bool includeNullValues,
        bool isJsonFragment = false)
    {
        if (value is null && !includeNullValues)
            return;

        var segments = propertyPath
            .Split('.')
            .Select(_=>_.Trim())
            .Where(_=>!string.IsNullOrWhiteSpace(_))
            .ToArray();
        if (segments.Length == 0)
            return;

        Dictionary<string, object?> current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var segment = segments[i];
            if (!current.TryGetValue(segment, out var nestedValue) || nestedValue is not Dictionary<string, object?> nested)
            {
                nested = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                current[segment] = nested;
            }

            current = nested;
        }

        current[segments[^1]] = NormalizeForJsonValue(value, isJsonFragment);
    }

    private static void AddJsonProperty(
        Dictionary<string, object?> target,
        string propertyName,
        object? value,
        bool includeNullValues,
        bool isJsonFragment = false)
    {
        if (value is null && !includeNullValues)
            return;

        target[propertyName] = NormalizeForJsonValue(value, isJsonFragment);
    }

    private static List<AutoJsonProjection> BuildAutoJsonProjections(TableResultMock result, SqlSelectQuery query)
    {
        var projections = new List<AutoJsonProjection>(result.Columns.Count);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            var qualifier = i < query.SelectItems.Count
                ? TryGetSimpleQualifiedColumnQualifier(query.SelectItems[i].Raw, query.SelectItems[i].Alias)
                : null;

            projections.Add(new AutoJsonProjection(i, qualifier, result.Columns[i].ColumnAlias, result.Columns[i].IsJsonFragment));
        }

        return projections;
    }

    private static object? NormalizeForJsonValue(object? value, bool isJsonFragment)
    {
        if (!isJsonFragment || value is null)
            return value;

        if (value is System.Text.Json.JsonElement jsonElement)
            return jsonElement.Clone();

        if (value is not string text)
            return value;

        var trimmed = text.Trim();
        if (trimmed.Length < 2 || (trimmed[0] != '{' && trimmed[0] != '['))
            return value;

        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(trimmed);
            return document.RootElement.ValueKind is System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array
                ? document.RootElement.Clone()
                : value;
        }
        catch (System.Text.Json.JsonException)
        {
            return value;
        }
    }

    private static string? TryGetSimpleQualifiedColumnQualifier(string raw, string? alias)
    {
        var (expression, _) = SelectAliasParserHelper.SplitTrailingAsAlias(raw, alias);
        var match = Regex.Match(
            expression.Trim(),
            @"^(?<qual>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s*\.\s*(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)$",
            RegexOptions.CultureInvariant);

        return match.Success
            ? match.Groups["qual"].Value.NormalizeName()
            : null;
    }

    private static string FormatJoinDebugDetails(SqlJoin join)
    {
        var source = FormatSource(join.Table);
        if (join.Type is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
            return source;

        var predicate = SqlExprPrinter.Print(join.On);
        return string.IsNullOrWhiteSpace(predicate)
            ? source
            : $"{source};on={predicate}";
    }

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
        var extract = new FunctionCallExpr("JSON_EXTRACT", [ja.Target, ja.Path]);
        return ja.Unquote
            ? new FunctionCallExpr("JSON_UNQUOTE", [extract])
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
        var cacheKey = BuildCorrelatedSubqueryCacheKey("SCALAR", sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddScalar(
            cacheKey,
            _ =>
            {
                var r = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "EVAL subquery"), ctes, row);
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
        if (SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, identifier.Name, out var temporalIdentifierValue))
            return temporalIdentifierValue;

        if ((identifier.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("@@DATEFIRST", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("@@MAX_PRECISION", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("@@TEXTSIZE", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase)
                || identifier.Name.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsSqlServerMetadataIdentifier(identifier.Name))
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
        => expression.Items.Select(item => Eval(item, row, group, ctes)).ToArray();

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

        if (left is not DateTime dateTime)
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

    private static bool TryConvertNumericToDouble(object? value, out double result)
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

    private static bool TryConvertNumericToDecimal(object? value, out decimal result)
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
            _ => throw new InvalidOperationException($"Binary op não suportado: {op}")
        };
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
        var leftVal = Eval(i.Left, row, group, ctes);

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
        var leftVal = Eval(i.Left, row, group, ctes);
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

        state = EvaluateMembershipCandidates(
            leftVal,
            GetOrEvaluateInSubqueryFirstColumnValues(subquery, row, ctes),
            ref hasNullCandidate);
        return true;
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
        var cacheKey = BuildCorrelatedSubqueryCacheKey("EXISTS", sq.Sql, row);

        return _subqueryEvaluationCache.GetOrAddExists(
            cacheKey,
            _ =>
            {
                var sub = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "EXISTS"), ctes, row);
                return sub.Count > 0;
            });
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
        => GetOrEvaluateSubqueryFirstColumnValuesForOperation(sq, "IN", row, ctes);

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

        return _subqueryEvaluationCache.GetOrAddFirstColumnValues(
            cacheKey,
            _ => EvaluateSubqueryFirstColumnValues(sq, operation, row, ctes));
    }

    private List<object?> EvaluateSubqueryFirstColumnValues(
        SubqueryExpr sq,
        string operation,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var subqueryResult = ExecuteSelect(GetSingleSubqueryOrThrow(sq, operation), ctes, row);
        var values = new List<object?>(subqueryResult.Count);
        foreach (var resultRow in subqueryResult)
            values.Add(resultRow.TryGetValue(0, out var cell) ? cell : null);

        return values;
    }

    /// <summary>
    /// EN: Builds a deterministic cache key for correlated subquery evaluation using operation kind, raw subquery text and normalized outer-row values.
    /// PT: Monta uma chave de cache determinística para avaliação de subquery correlacionada usando tipo de operação, texto bruto da subquery e valores normalizados da linha externa.
    /// </summary>
    private static string BuildCorrelatedSubqueryCacheKey(string operation, string? subquerySql, EvalRow row)
    {
        var normalizedSubquerySql = BuildNormalizedCorrelatedSubquerySql(operation, subquerySql);
        var sb = new StringBuilder();
        AppendCorrelatedSubqueryCacheKeyPrefix(sb, operation, normalizedSubquerySql);
        AppendCorrelatedSubqueryCacheKeyFields(sb, GetCorrelatedSubqueryCacheFields(subquerySql ?? string.Empty, row));

        return sb.ToString();
    }

    private static string BuildNormalizedCorrelatedSubquerySql(string operation, string? subquerySql)
    {
        var normalizedSubquerySql = NormalizeSubquerySqlForCacheKey(subquerySql ?? string.Empty);
        return NormalizeOperationSpecificSubquerySqlForCacheKey(operation, normalizedSubquerySql);
    }

    private static void AppendCorrelatedSubqueryCacheKeyPrefix(
        StringBuilder sb,
        string operation,
        string normalizedSubquerySql)
    {
        sb.Append(operation);
        sb.Append('\u001F');
        sb.Append(normalizedSubquerySql);
        sb.Append('\u001F');
    }

    private static void AppendCorrelatedSubqueryCacheKeyFields(
        StringBuilder sb,
        IReadOnlyList<KeyValuePair<string, object?>> cacheFields)
    {
        foreach (var kv in cacheFields)
        {
            sb.Append(kv.Key);
            sb.Append('=');
            sb.Append(NormalizeSubqueryCacheValue(kv.Value));
            sb.Append('\u001E');
        }
    }

    /// <summary>
    /// EN: Applies operation-specific canonicalization rules for subquery SQL used in correlated cache keys.
    /// PT: Aplica regras de canonização específicas por operação para SQL de subquery usado em chaves de cache correlacionado.
    /// </summary>
    private static string NormalizeOperationSpecificSubquerySqlForCacheKey(
        string operation,
        string normalizedSubquerySql)
    {
        if (string.IsNullOrWhiteSpace(normalizedSubquerySql))
            return string.Empty;

        if (string.Equals(operation, "EXISTS", StringComparison.OrdinalIgnoreCase))
            return NormalizeExistsProjectionPayloadForCacheKey(normalizedSubquerySql);

        if (string.Equals(operation, "IN", StringComparison.OrdinalIgnoreCase)
            || string.Equals(operation, "SCALAR", StringComparison.OrdinalIgnoreCase)
            || operation.StartsWith("QANY_", StringComparison.OrdinalIgnoreCase)
            || operation.StartsWith("QALL_", StringComparison.OrdinalIgnoreCase))
            return NormalizeSelectProjectionAliasesForCacheKey(normalizedSubquerySql);

        return normalizedSubquerySql;
    }

    /// <summary>
    /// EN: Selects relevant outer-row fields for correlated subquery cache keys, prioritizing identifiers explicitly referenced in subquery SQL.
    /// PT: Seleciona campos relevantes da linha externa para chaves de cache de subquery correlacionada, priorizando identificadores explicitamente referenciados no SQL da subquery.
    /// </summary>
    private static IReadOnlyList<KeyValuePair<string, object?>> GetCorrelatedSubqueryCacheFields(
        string subquerySql,
        EvalRow row)
    {
        var allFields = GetOrderedCorrelatedSubqueryCacheFields(row);

        if (allFields.Count == 0 || string.IsNullOrWhiteSpace(subquerySql))
            return allFields;

        var normalizedSql = NormalizeSqlIdentifierSpacing(subquerySql);
        var qualifiedMatches = GetQualifiedCorrelatedSubqueryCacheFieldMatches(allFields, normalizedSql);

        if (qualifiedMatches.Count > 0)
            return qualifiedMatches;

        var unqualifiedMatches = GetUnqualifiedCorrelatedSubqueryCacheFieldMatches(allFields, normalizedSql);

        if (unqualifiedMatches.Count > 0)
            return unqualifiedMatches;

        // If we cannot match any outer identifier but SQL still appears to reference outer qualifiers,
        // keep conservative behavior and include all fields to avoid stale cross-row reuse.
        return ContainsPotentialOuterQualifierReference(normalizedSql, allFields)
            ? allFields
            : [];
    }

    private static List<KeyValuePair<string, object?>> GetOrderedCorrelatedSubqueryCacheFields(EvalRow row)
        => row.Fields
            .OrderBy(static kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

    private static List<KeyValuePair<string, object?>> GetQualifiedCorrelatedSubqueryCacheFieldMatches(
        IReadOnlyList<KeyValuePair<string, object?>> allFields,
        string normalizedSql)
    {
        var qualifiedIdentifiers = ExtractQualifiedSqlIdentifiers(normalizedSql);
        return allFields
            .Where(static kv => kv.Key.IndexOf('.') >= 0)
            .Where(kv => qualifiedIdentifiers.Contains(kv.Key))
            .ToList();
    }

    private static List<KeyValuePair<string, object?>> GetUnqualifiedCorrelatedSubqueryCacheFieldMatches(
        IReadOnlyList<KeyValuePair<string, object?>> allFields,
        string normalizedSql)
        => allFields
            .Where(static kv => kv.Key.IndexOf('.') < 0)
            .Where(kv => ContainsSqlIdentifierToken(normalizedSql, kv.Key))
            .ToList();

    /// <summary>
    /// EN: Checks whether a candidate identifier token appears in SQL text using lightweight identifier-boundary guards.
    /// PT: Verifica se um token identificador candidato aparece no texto SQL usando guardas leves de fronteira de identificador.
    /// </summary>
    private static bool ContainsSqlIdentifierToken(string sql, string token)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(token))
            return false;

        var index = 0;
        while (true)
        {
            index = sql.IndexOf(token, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return false;

            var leftBoundaryOk = index == 0 || !IsSqlIdentifierChar(sql[index - 1]);
            var right = index + token.Length;
            var rightBoundaryOk = right >= sql.Length || !IsSqlIdentifierChar(sql[right]);

            if (leftBoundaryOk && rightBoundaryOk)
                return true;

            index = right;
        }
    }

    /// <summary>
    /// EN: Determines whether a character can participate in SQL identifiers when evaluating token boundaries.
    /// PT: Determina se um caractere pode participar de identificadores SQL ao avaliar fronteiras de token.
    /// </summary>
    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    /// <summary>
    /// EN: Extracts qualified identifier tokens (alias.column) from SQL text using lightweight lexical boundaries.
    /// PT: Extrai tokens de identificador qualificado (alias.coluna) do texto SQL usando fronteiras léxicas leves.
    /// </summary>
    private static HashSet<string> ExtractQualifiedSqlIdentifiers(string sql)
    {
        var identifiers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(sql))
            return identifiers;

        var matches = _qualifiedSqlIdentifierRegex.Matches(sql);

        foreach (Match match in matches)
        {
            if (match.Success && match.Groups.Count > 1)
                identifiers.Add(match.Groups[1].Value);
        }

        return identifiers;
    }

    /// <summary>
    /// EN: Detects whether SQL text appears to reference any qualifier from outer-row fields, even when full token matching failed.
    /// PT: Detecta se o texto SQL parece referenciar algum qualificador dos campos da linha externa, mesmo quando o matching completo de token falha.
    /// </summary>
    private static bool ContainsPotentialOuterQualifierReference(
        string sql,
        IReadOnlyList<KeyValuePair<string, object?>> fields)
    {
        if (string.IsNullOrWhiteSpace(sql) || fields.Count == 0)
            return false;

        var qualifiers = fields
            .Select(static kv =>
            {
                var dot = kv.Key.IndexOf('.');
                return dot > 0 ? kv.Key[..dot] : null;
            })
            .Where(static q => !string.IsNullOrWhiteSpace(q))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var qualifier in qualifiers)
        {
            if (Regex.IsMatch(
                    sql,
                    $@"(?<![A-Za-z0-9_$]){Regex.Escape(qualifier!)}\.",
                    RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
                return true;
        }

        return false;
    }

    /// <summary>
    /// EN: Normalizes SQL text by collapsing optional whitespace around dot separators in qualified identifiers.
    /// PT: Normaliza texto SQL colapsando espaços opcionais ao redor de separadores com ponto em identificadores qualificados.
    /// </summary>
    private static string NormalizeSqlIdentifierSpacing(string sql)
        => string.IsNullOrWhiteSpace(sql)
            ? string.Empty
            : Regex.Replace(sql, @"\s*\.\s*", ".", RegexOptions.CultureInvariant);

    /// <summary>
    /// EN: Canonicalizes subquery SQL text for cache-key usage by normalizing identifier spacing, keyword casing and redundant whitespace while preserving string literals.
    /// PT: Canoniza o texto SQL da subquery para uso na chave de cache normalizando espaçamento de identificadores, casing de palavras-chave e whitespace redundante preservando literais de texto.
    /// </summary>
    private static string NormalizeSubquerySqlForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var normalized = NormalizeSqlIdentifierSpacing(sql);
        var sb = new StringBuilder(normalized.Length);
        var previousWasSpace = false;

        for (var i = 0; i < normalized.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(normalized, ref i, sb))
            {
                previousWasSpace = false;
                continue;
            }

            var ch = normalized[i];

            if (char.IsWhiteSpace(ch))
            {
                if (!previousWasSpace)
                {
                    sb.Append(' ');
                    previousWasSpace = true;
                }

                continue;
            }

            sb.Append(char.ToUpperInvariant(ch));
            previousWasSpace = false;
        }

        var canonicalSql = sb.ToString().Trim();
        canonicalSql = NormalizeRelationalOperatorSpacingForCacheKey(canonicalSql);
        canonicalSql = NormalizeSubqueryLocalAliasesForCacheKey(canonicalSql);
        return NormalizeCommutativeAndClausesForCacheKey(canonicalSql);
    }

    /// <summary>
    /// EN: Appends a SQL quoted segment handling escaped quote doubles and returns the consumed end index.
    /// PT: Anexa um segmento SQL entre aspas tratando escape por duplicidade de aspas e retorna o índice final consumido.
    /// </summary>
    private static int AppendQuotedSegment(
        string sql,
        int startIndex,
        char quoteChar,
        StringBuilder sb)
    {
        sb.Append(quoteChar);

        var i = startIndex + 1;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(sql[i]);

            if (ch == quoteChar)
            {
                var hasEscapedQuote = i + 1 < sql.Length && sql[i + 1] == quoteChar;
                if (hasEscapedQuote)
                {
                    sb.Append(sql[i + 1]);
                    i += 2;
                    continue;
                }

                return i;
            }

            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Appends a SQL bracket-identifier segment and returns the consumed end index.
    /// PT: Anexa um segmento SQL de identificador entre colchetes e retorna o índice final consumido.
    /// </summary>
    private static int AppendBracketIdentifierSegment(
        string sql,
        int startIndex,
        StringBuilder sb)
    {
        sb.Append('[');

        var i = startIndex + 1;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(sql[i]);
            if (ch == ']')
                return i;
            i++;
        }

        return sql.Length - 1;
    }

    private static bool TryAppendProtectedSqlSegment(
        string sql,
        ref int index,
        StringBuilder sb)
    {
        if (index < 0 || index >= sql.Length)
            return false;

        var ch = sql[index];
        if (ch is '\'' or '"' or '`')
        {
            index = AppendQuotedSegment(sql, index, ch, sb);
            return true;
        }

        if (ch != '[')
            return false;

        index = AppendBracketIdentifierSegment(sql, index, sb);
        return true;
    }

    /// <summary>
    /// EN: Normalizes local aliases declared inside the subquery so semantically equivalent aliases generate the same cache-key SQL fragment.
    /// PT: Normaliza aliases locais declarados dentro da subquery para que aliases semanticamente equivalentes gerem o mesmo fragmento SQL da chave de cache.
    /// </summary>
    private static string NormalizeSubqueryLocalAliasesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var aliasMap = ExtractSubqueryAliasMap(sql);
        if (aliasMap.Count == 0)
            return sql;

        return ApplySubqueryAliasMapForCacheKey(sql, aliasMap);
    }

    private static string ApplySubqueryAliasMapForCacheKey(
        string sql,
        IReadOnlyDictionary<string, string> aliasMap)
    {
        var normalized = sql;
        foreach (var aliasPair in aliasMap)
        {
            normalized = ReplaceAliasDeclarationForCacheKey(normalized, aliasPair.Key, aliasPair.Value);
            normalized = ReplaceAliasQualifierReferencesForCacheKey(normalized, aliasPair.Key, aliasPair.Value);
        }

        return normalized;
    }

    /// <summary>
    /// EN: Extracts a deterministic map of local alias names declared in FROM/JOIN clauses to canonical placeholders.
    /// PT: Extrai um mapa determinístico de nomes de aliases locais declarados em cláusulas FROM/JOIN para placeholders canônicos.
    /// </summary>
    private static IReadOnlyDictionary<string, string> ExtractSubqueryAliasMap(string sql)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(sql))
            return map;

        var matches = _subqueryAliasDeclarationRegex.Matches(sql);

        foreach (Match match in matches)
        {
            if (!match.Success || match.Groups.Count < 2)
                continue;

            var alias = match.Groups[1].Value;
            if (string.IsNullOrWhiteSpace(alias) || _sqlAliasReservedTokens.Contains(alias))
                continue;

            if (!map.ContainsKey(alias))
                map[alias] = $"T{map.Count + 1}";
        }

        return map;
    }

    /// <summary>
    /// EN: Rewrites FROM/JOIN alias declarations to canonical placeholders for cache-key normalization.
    /// PT: Reescreve declarações de alias em FROM/JOIN para placeholders canônicos na normalização da chave de cache.
    /// </summary>
    private static string ReplaceAliasDeclarationForCacheKey(
        string sql,
        string alias,
        string replacementAlias)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(alias))
            return sql;

        var pattern =
            $@"(?<![A-Z0-9_$])(?<kw>FROM|JOIN|APPLY)\s+(?<table>[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?" +
            $@"{Regex.Escape(alias)}(?![A-Z0-9_$])";

        return Regex.Replace(
            sql,
            pattern,
            m => $"{m.Groups["kw"].Value} {m.Groups["table"].Value} {replacementAlias}",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    /// <summary>
    /// EN: Rewrites alias-qualified references (alias.column) outside quoted segments to canonical placeholders for cache-key normalization.
    /// PT: Reescreve referências qualificadas por alias (alias.coluna) fora de segmentos entre aspas para placeholders canônicos na normalização da chave de cache.
    /// </summary>
    private static string ReplaceAliasQualifierReferencesForCacheKey(
        string sql,
        string alias,
        string replacementAlias)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(alias))
            return sql;

        var sb = new StringBuilder(sql.Length);

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
                continue;

            if (IsAliasQualifierReferenceAt(sql, i, alias))
            {
                sb.Append(replacementAlias);
                sb.Append('.');
                i += alias.Length;
                continue;
            }

            sb.Append(sql[i]);
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Checks whether the SQL text at a given index starts with an alias qualifier reference (alias followed by dot) respecting identifier boundaries.
    /// PT: Verifica se o texto SQL em um índice inicia uma referência de qualificador por alias (alias seguido de ponto) respeitando fronteiras de identificador.
    /// </summary>
    private static bool IsAliasQualifierReferenceAt(
        string sql,
        int startIndex,
        string alias)
    {
        if (startIndex < 0 || startIndex >= sql.Length || string.IsNullOrWhiteSpace(alias))
            return false;

        if (startIndex + alias.Length >= sql.Length)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        for (var i = 0; i < alias.Length; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(alias[i]))
                return false;
        }

        return sql[startIndex + alias.Length] == '.';
    }

    /// <summary>
    /// EN: Normalizes spacing around top-level relational operators outside quoted segments so semantically equivalent operator formatting maps to the same cache-key SQL.
    /// PT: Normaliza espaçamento ao redor de operadores relacionais no topo fora de segmentos entre aspas para que formatações equivalentes mapeiem para o mesmo SQL de chave de cache.
    /// </summary>
    private static string NormalizeRelationalOperatorSpacingForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length + 16);

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
                continue;

            var ch = sql[i];

            if (!TryReadRelationalOperator(sql, i, out var op, out var opLength))
            {
                sb.Append(ch);
                continue;
            }

            TrimTrailingSpaces(sb);
            if (sb.Length > 0)
                sb.Append(' ');

            sb.Append(op);
            sb.Append(' ');
            i += opLength - 1;
        }

        return CollapseWhitespaceOutsideQuotedSegments(sb.ToString()).Trim();
    }

    /// <summary>
    /// EN: Tries to read a relational comparison operator at the current index, including two-character variants.
    /// PT: Tenta ler um operador relacional de comparação no índice atual, incluindo variantes de dois caracteres.
    /// </summary>
    private static bool TryReadRelationalOperator(
        string sql,
        int startIndex,
        out string op,
        out int opLength)
    {
        op = string.Empty;
        opLength = 0;

        if (string.IsNullOrWhiteSpace(sql) || startIndex < 0 || startIndex >= sql.Length)
            return false;

        var ch = sql[startIndex];
        var next = startIndex + 1 < sql.Length ? sql[startIndex + 1] : '\0';

        if (ch == '<' && next == '=')
        {
            op = "<=";
            opLength = 2;
            return true;
        }

        if (ch == '>' && next == '=')
        {
            op = ">=";
            opLength = 2;
            return true;
        }

        if (ch == '<' && next == '>')
        {
            op = "<>";
            opLength = 2;
            return true;
        }

        if (ch == '!' && next == '=')
        {
            op = "!=";
            opLength = 2;
            return true;
        }

        if (ch is '=' or '<' or '>')
        {
            op = ch.ToString();
            opLength = 1;
            return true;
        }

        return false;
    }

    /// <summary>
    /// EN: Trims trailing spaces from a StringBuilder buffer.
    /// PT: Remove espaços à direita de um buffer StringBuilder.
    /// </summary>
    private static void TrimTrailingSpaces(StringBuilder sb)
    {
        while (sb.Length > 0 && sb[^1] == ' ')
            sb.Length--;
    }

    /// <summary>
    /// EN: Collapses repeated whitespace outside quoted or bracket-delimited segments while preserving inner literal content.
    /// PT: Colapsa whitespace repetido fora de segmentos entre aspas ou delimitados por colchetes preservando o conteúdo interno de literais.
    /// </summary>
    private static string CollapseWhitespaceOutsideQuotedSegments(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length);
        var previousWasSpace = false;

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
            {
                previousWasSpace = false;
                continue;
            }

            var ch = sql[i];

            if (!char.IsWhiteSpace(ch))
            {
                sb.Append(ch);
                previousWasSpace = false;
                continue;
            }

            if (previousWasSpace)
                continue;

            sb.Append(' ');
            previousWasSpace = true;
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Canonicalizes top-level EXISTS subquery projection payload by replacing SELECT list with a fixed token while preserving relational clauses.
    /// PT: Canoniza o payload de projeção de subquery EXISTS no nível de topo substituindo a lista do SELECT por token fixo preservando cláusulas relacionais.
    /// </summary>
    private static string NormalizeExistsProjectionPayloadForCacheKey(string sql)
    {
        return RewriteTopLevelSelectPayloadForCacheKey(sql, static _ => "<EXISTS_PAYLOAD>");
    }

    /// <summary>
    /// EN: Canonicalizes top-level SELECT projection aliases by removing explicit AS aliases while preserving projection expressions and relational clauses.
    /// PT: Canoniza aliases da projeção SELECT no nível de topo removendo aliases explícitos AS e preservando expressões projetadas e cláusulas relacionais.
    /// </summary>
    private static string NormalizeSelectProjectionAliasesForCacheKey(string sql)
    {
        return RewriteTopLevelSelectPayloadForCacheKey(sql, NormalizeSelectListAliasesForCacheKey);
    }

    private static string RewriteTopLevelSelectPayloadForCacheKey(
        string sql,
        Func<string, string> rewritePayload)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        if (!TryGetTopLevelSelectPayloadRange(sql, out var afterSelect, out var fromIndex))
            return sql;

        return string.Concat(
            sql.Substring(0, afterSelect),
            " ",
            rewritePayload(sql[afterSelect..fromIndex]),
            " ",
            sql.Substring(fromIndex));
    }

    private static bool TryGetTopLevelSelectPayloadRange(
        string sql,
        out int afterSelect,
        out int fromIndex)
    {
        afterSelect = -1;
        fromIndex = -1;
        if (string.IsNullOrWhiteSpace(sql))
            return false;

        if (!TryFindTopLevelKeywordIndex(sql, "SELECT", 0, out var selectIndex))
            return false;

        afterSelect = selectIndex + "SELECT".Length;
        if (!TryFindTopLevelKeywordIndex(sql, "FROM", afterSelect, out fromIndex))
            return false;

        return fromIndex > afterSelect;
    }

    /// <summary>
    /// EN: Normalizes explicit AS aliases from a top-level SELECT list while preserving nested expressions.
    /// PT: Normaliza aliases explícitos AS de uma lista SELECT de topo preservando expressões aninhadas.
    /// </summary>
    private static string NormalizeSelectListAliasesForCacheKey(string selectList)
    {
        if (string.IsNullOrWhiteSpace(selectList))
            return string.Empty;

        var segments = SplitTopLevelCommaSegments(selectList);
        if (segments.Count == 0)
            return selectList.Trim();

        for (var i = 0; i < segments.Count; i++)
            segments[i] = RemoveExplicitAsAliasFromSelectExpression(segments[i]);

        return string.Join(", ", segments);
    }

    /// <summary>
    /// EN: Splits text by top-level comma separators outside quoted segments and nested parentheses.
    /// PT: Divide o texto por vírgulas de topo fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static List<string> SplitTopLevelCommaSegments(string text)
    {
        var segments = new List<string>();
        if (string.IsNullOrWhiteSpace(text))
            return segments;

        var start = 0;
        var depth = 0;
        for (var i = 0; i < text.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(text, ref i))
                continue;

            var ch = text[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && ch == ',')
            {
                var segment = text[start..i].Trim();
                if (segment.Length > 0)
                    segments.Add(segment);
                start = i + 1;
            }
        }

        var last = text[start..].Trim();
        if (last.Length > 0)
            segments.Add(last);

        return segments;
    }

    /// <summary>
    /// EN: Removes a trailing explicit AS alias from a SELECT expression when alias syntax is valid.
    /// PT: Remove alias explícito AS ao final de uma expressão SELECT quando a sintaxe do alias é válida.
    /// </summary>
    private static string RemoveExplicitAsAliasFromSelectExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var trimmed = expression.Trim();
        if (!TrySplitExplicitAsAlias(trimmed, out var beforeAs, out var aliasPart)
            || !IsValidExplicitAliasToken(aliasPart))
            return trimmed;

        return beforeAs;
    }

    private static bool TrySplitExplicitAsAlias(
        string expression,
        out string beforeAs,
        out string aliasPart)
    {
        beforeAs = string.Empty;
        aliasPart = string.Empty;
        if (string.IsNullOrWhiteSpace(expression))
            return false;

        if (!TryFindTopLevelKeywordIndex(expression, "AS", 0, out var asIndex))
            return false;

        beforeAs = expression[..asIndex].TrimEnd();
        aliasPart = expression[(asIndex + 2)..].Trim();
        return true;
    }

    /// <summary>
    /// EN: Validates whether an alias token matches supported explicit alias forms (identifier or quoted identifier).
    /// PT: Valida se um token de alias corresponde às formas suportadas de alias explícito (identificador ou identificador entre delimitadores).
    /// </summary>
    private static bool IsValidExplicitAliasToken(string aliasToken)
    {
        if (string.IsNullOrWhiteSpace(aliasToken))
            return false;

        var trimmed = aliasToken.Trim();
        if (_simpleAliasTokenRegex.IsMatch(trimmed))
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '[' && trimmed[^1] == ']')
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[^1] == '"')
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '`' && trimmed[^1] == '`')
            return true;

        return false;
    }

    /// <summary>
    /// EN: Tries to find a top-level SQL keyword index outside quoted segments and nested parentheses, starting from a given position.
    /// PT: Tenta localizar o índice de uma palavra-chave SQL no topo fora de segmentos entre aspas e parênteses aninhados, iniciando em uma posição informada.
    /// </summary>
    private static bool TryFindTopLevelKeywordIndex(
        string sql,
        string keyword,
        int startIndex,
        out int keywordIndex)
    {
        keywordIndex = -1;
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(keyword))
            return false;

        var safeStart = startIndex;
        if (safeStart < 0)
            safeStart = 0;
        else if (safeStart > sql.Length)
            safeStart = sql.Length;
        var depth = 0;
        for (var i = safeStart; i < sql.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(sql, ref i))
                continue;

            var ch = sql[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && MatchesKeywordTokenAt(sql, i, keyword))
            {
                keywordIndex = i;
                return true;
            }
        }

        return false;
    }

    private static bool TrySkipProtectedSqlSegment(string sql, ref int index)
    {
        if (index < 0 || index >= sql.Length)
            return false;

        var ch = sql[index];
        if (ch is '\'' or '"' or '`')
        {
            index = FindQuotedSegmentEndIndex(sql, index, ch);
            return true;
        }

        if (ch != '[')
            return false;

        index = FindBracketSegmentEndIndex(sql, index);
        return true;
    }

    /// <summary>
    /// EN: Normalizes commutative top-level AND chains in WHERE/HAVING clauses so equivalent predicate orderings reuse the same cache-key SQL fragment.
    /// PT: Normaliza cadeias comutativas de AND no topo em cláusulas WHERE/HAVING para que ordenações equivalentes de predicados reutilizem o mesmo fragmento SQL da chave de cache.
    /// </summary>
    private static string NormalizeCommutativeAndClausesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var normalizedWhere = RewritePredicateClauseForCacheKey(sql, _cacheKeyWherePredicateRegex, "WHERE");
        return RewritePredicateClauseForCacheKey(normalizedWhere, _cacheKeyHavingPredicateRegex, "HAVING");
    }

    private static string RewritePredicateClauseForCacheKey(
        string sql,
        Regex clauseRegex,
        string clauseKeyword)
        => clauseRegex.Replace(
            sql,
            match => $"{clauseKeyword} {NormalizeTopLevelAndPredicateForCacheKey(match.Groups["predicate"].Value)}");

    /// <summary>
    /// EN: Canonicalizes a predicate text by sorting top-level AND segments when safe (no top-level OR and no BETWEEN token).
    /// PT: Canoniza um texto de predicado ordenando segmentos AND de topo quando seguro (sem OR de topo e sem token BETWEEN).
    /// </summary>
    private static string NormalizeTopLevelAndPredicateForCacheKey(string predicate)
    {
        if (string.IsNullOrWhiteSpace(predicate))
            return string.Empty;

        var trimmedPredicate = TrimRedundantOuterParentheses(predicate);
        if (!ShouldNormalizeTopLevelAndPredicateForCacheKey(trimmedPredicate))
            return trimmedPredicate;

        var segments = SplitTopLevelAndSegments(trimmedPredicate);
        return JoinNormalizedTopLevelAndSegments(trimmedPredicate, segments);
    }

    private static bool ShouldNormalizeTopLevelAndPredicateForCacheKey(string predicate)
        => !string.IsNullOrWhiteSpace(predicate)
            && !ContainsTokenOutsideQuotedSegments(predicate, "OR")
            && !ContainsTokenOutsideQuotedSegments(predicate, "BETWEEN");

    private static string JoinNormalizedTopLevelAndSegments(
        string originalPredicate,
        List<string> segments)
    {
        if (segments.Count <= 1)
            return originalPredicate;

        segments.Sort(StringComparer.Ordinal);
        return string.Join(" AND ", segments);
    }

    /// <summary>
    /// EN: Splits predicate text by top-level AND operators outside quoted segments and nested parentheses.
    /// PT: Divide o texto do predicado por operadores AND de topo fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static List<string> SplitTopLevelAndSegments(string predicate)
    {
        var segments = new List<string>();
        if (string.IsNullOrWhiteSpace(predicate))
            return segments;

        var start = 0;
        var depth = 0;

        for (var i = 0; i < predicate.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(predicate, ref i))
                continue;

            var ch = predicate[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && MatchesKeywordTokenAt(predicate, i, "AND"))
            {
                AppendNormalizedPredicateSegment(segments, predicate[start..i]);
                start = i + 3;
                i += 2;
            }
        }

        AppendNormalizedPredicateSegment(segments, predicate[start..]);

        return segments;
    }

    private static void AppendNormalizedPredicateSegment(List<string> segments, string rawSegment)
    {
        var segment = NormalizePredicateSegmentForCacheKey(rawSegment);
        if (segment.Length > 0)
            segments.Add(segment);
    }

    /// <summary>
    /// EN: Normalizes an individual predicate segment by trimming redundant outer parentheses and canonicalizing simple commutative equalities.
    /// PT: Normaliza um segmento individual de predicado removendo parênteses externos redundantes e canonizando igualdades comutativas simples.
    /// </summary>
    private static string NormalizePredicateSegmentForCacheKey(string segment)
    {
        var trimmedSegment = TrimRedundantOuterParentheses(segment);
        if (string.IsNullOrWhiteSpace(trimmedSegment))
            return string.Empty;

        return NormalizeCommutativeEqualitySegmentForCacheKey(trimmedSegment);
    }

    /// <summary>
    /// EN: Canonicalizes a simple top-level equality segment (`lhs = rhs`) by sorting operands lexicographically when safe.
    /// PT: Canoniza um segmento de igualdade simples no topo (`lhs = rhs`) ordenando operandos lexicograficamente quando seguro.
    /// </summary>
    private static string NormalizeCommutativeEqualitySegmentForCacheKey(string segment)
    {
        if (string.IsNullOrWhiteSpace(segment))
            return string.Empty;

        if (!TrySplitStandaloneTopLevelEqualityOperands(segment, out var left, out var right))
            return segment;

        return BuildOrderedEqualitySegmentForCacheKey(left, right);
    }

    private static bool TrySplitStandaloneTopLevelEqualityOperands(
        string segment,
        out string left,
        out string right)
    {
        left = string.Empty;
        right = string.Empty;
        if (!TryFindStandaloneTopLevelEqualityOperator(segment, out var equalityIndex))
            return false;

        left = TrimRedundantOuterParentheses(segment[..equalityIndex]);
        right = TrimRedundantOuterParentheses(segment[(equalityIndex + 1)..]);
        return left.Length > 0 && right.Length > 0;
    }

    private static string BuildOrderedEqualitySegmentForCacheKey(string left, string right)
        => StringComparer.Ordinal.Compare(left, right) <= 0
            ? $"{left} = {right}"
            : $"{right} = {left}";

    /// <summary>
    /// EN: Tries to find a single standalone top-level equality operator, excluding composite comparisons such as less-or-equal, greater-or-equal, different and double-equals.
    /// PT: Tenta localizar um único operador de igualdade isolado no topo, excluindo comparações compostas como menor-ou-igual, maior-ou-igual, diferente e igualdade dupla.
    /// </summary>
    private static bool TryFindStandaloneTopLevelEqualityOperator(string segment, out int equalityIndex)
    {
        equalityIndex = -1;
        if (string.IsNullOrWhiteSpace(segment))
            return false;

        var depth = 0;
        for (var i = 0; i < segment.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(segment, ref i))
                continue;

            var ch = segment[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth != 0 || ch != '=')
                continue;

            var previous = i > 0 ? segment[i - 1] : '\0';
            var next = i + 1 < segment.Length ? segment[i + 1] : '\0';

            var isCompositeComparison = previous is '<' or '>' or '!' or '='
                                      || next is '<' or '>' or '!' or '=';
            if (isCompositeComparison)
                continue;

            if (equalityIndex >= 0)
                return false;

            equalityIndex = i;
        }

        return equalityIndex >= 0;
    }

    /// <summary>
    /// EN: Removes redundant outer parentheses that wrap the full expression while preserving inner structure.
    /// PT: Remove parênteses externos redundantes que envolvem a expressão inteira preservando a estrutura interna.
    /// </summary>
    private static string TrimRedundantOuterParentheses(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var trimmed = expression.Trim();
        while (trimmed.Length >= 2 && trimmed[0] == '(' && trimmed[^1] == ')')
        {
            if (!HasSingleOuterParenthesesWrappingWholeExpression(trimmed))
                break;

            trimmed = trimmed[1..^1].Trim();
        }

        return trimmed;
    }

    /// <summary>
    /// EN: Checks whether the first and last parentheses wrap the whole expression without closing earlier at top level.
    /// PT: Verifica se o primeiro e o último parêntese envolvem toda a expressão sem fechar antes no nível de topo.
    /// </summary>
    private static bool HasSingleOuterParenthesesWrappingWholeExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression) || expression.Length < 2)
            return false;

        if (expression[0] != '(' || expression[^1] != ')')
            return false;

        var depth = 0;
        for (var i = 0; i < expression.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(expression, ref i))
                continue;

            var ch = expression[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch != ')')
                continue;

            depth--;
            if (depth < 0)
                return false;

            if (depth == 0 && i < expression.Length - 1)
                return false;
        }

        return depth == 0;
    }

    /// <summary>
    /// EN: Detects whether a token appears outside quoted segments and nested parentheses.
    /// PT: Detecta se um token aparece fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static bool ContainsTokenOutsideQuotedSegments(string sql, string token)
        => TryFindTopLevelKeywordIndex(sql, token, 0, out _);

    /// <summary>
    /// EN: Matches a SQL keyword token at a position ensuring identifier boundaries.
    /// PT: Compara um token de palavra-chave SQL em uma posição garantindo fronteiras de identificador.
    /// </summary>
    private static bool MatchesKeywordTokenAt(string sql, int startIndex, string token)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(token))
            return false;

        if (startIndex < 0 || startIndex + token.Length > sql.Length)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        var endIndex = startIndex + token.Length;
        if (endIndex < sql.Length && IsSqlIdentifierChar(sql[endIndex]))
            return false;

        for (var i = 0; i < token.Length; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(token[i]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// EN: Finds the end index of a quoted SQL segment handling escaped doubled quote characters.
    /// PT: Localiza o índice final de um segmento SQL entre aspas tratando escapes por duplicidade de aspas.
    /// </summary>
    private static int FindQuotedSegmentEndIndex(string sql, int startIndex, char quoteChar)
    {
        var i = startIndex + 1;
        while (i < sql.Length)
        {
            if (sql[i] == quoteChar)
            {
                var hasEscapedQuote = i + 1 < sql.Length && sql[i + 1] == quoteChar;
                if (hasEscapedQuote)
                {
                    i += 2;
                    continue;
                }

                return i;
            }

            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Finds the end index of a bracket-delimited SQL identifier segment.
    /// PT: Localiza o índice final de um segmento SQL de identificador delimitado por colchetes.
    /// </summary>
    private static int FindBracketSegmentEndIndex(string sql, int startIndex)
    {
        var i = startIndex + 1;
        while (i < sql.Length)
        {
            if (sql[i] == ']')
                return i;
            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Normalizes scalar and tuple-like values into stable cache-key fragments for correlated subquery memoization.
    /// PT: Normaliza valores escalares e em formato tupla em fragmentos estáveis de chave de cache para memoização de subquery correlacionada.
    /// </summary>
    private static string NormalizeSubqueryCacheValue(object? value)
    {
        if (value is null || value is DBNull)
            return "<null>";

        if (value is object?[] tuple)
            return "[" + string.Join(",", tuple.Select(NormalizeSubqueryCacheValue)) + "]";

        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

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
    {
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para avaliação de função.");

        // Aggregate?
        if (group is not null && _aggFns.Contains(fn.Name))
            return EvalAggregate(fn, group, ctes);

        // Scalar functions (best-effort)
        if (fn.Args.Count == 0 && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, fn.Name, out var temporalValue))
            return temporalValue;

        if (TryEvalFindInSetFunction(fn, EvalArg, out var findInSetResult))
            return findInSetResult;

        if (TryEvalMatchAgainstFunction(fn, dialect, EvalArg, out var matchAgainstResult))
            return matchAgainstResult;

        if (IsFoundRowsEquivalentFunction(fn.Name, dialect))
        {
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            return _cnn.GetLastFoundRows();
        }

        if (TryEvalConditionalFunction(fn, dialect, EvalArg, out var conditionalResult))
            return conditionalResult;

        if (TryEvalNullSubstituteFunction(fn, dialect, EvalArg, out var nullSubstituteResult))
            return nullSubstituteResult;

        if (TryEvalNvl2Function(fn, EvalArg, out var nvl2Result))
            return nvl2Result;

        if (TryEvalDecodeFunction(fn, dialect, EvalArg, out var decodeResult))
            return decodeResult;

        if (TryEvalMySqlBase64Functions(fn, dialect, EvalArg, out var base64Result))
            return base64Result;

        if (TryEvalMySqlStringCompareFunction(fn, dialect, EvalArg, out var stringCompareResult))
            return stringCompareResult;

        if (TryEvalMySqlChecksumFunction(fn, dialect, EvalArg, out var checksumResult))
            return checksumResult;

        if (TryEvalMySqlNetworkFunctions(fn, dialect, EvalArg, out var networkResult))
            return networkResult;

        if (TryEvalMySqlUuidFunctions(fn, dialect, EvalArg, out var uuidResult))
            return uuidResult;

        if (TryEvalMySqlDateFormatFunction(fn, dialect, EvalArg, out var dateFormatResult))
            return dateFormatResult;

        if (TryEvalMySqlStrToDateFunction(fn, dialect, EvalArg, out var strToDateResult))
            return strToDateResult;

        if (TryEvalMySqlFromUnixTimeFunction(fn, dialect, EvalArg, out var fromUnixTimeResult))
            return fromUnixTimeResult;

        if (TryEvalMySqlFromDaysFunction(fn, dialect, EvalArg, out var fromDaysResult))
            return fromDaysResult;

        if (TryEvalMySqlDateSubFunction(fn, dialect, row, group, ctes, EvalArg, out var dateSubResult))
            return dateSubResult;

        if (TryEvalMySqlGetFormatFunction(fn, dialect, EvalArg, out var getFormatResult))
            return getFormatResult;

        if (TryEvalMySqlConvertTzFunction(fn, dialect, EvalArg, out var convertTzResult))
            return convertTzResult;

        if (TryEvalMySqlConvFunction(fn, dialect, EvalArg, out var convResult))
            return convResult;

        if (TryEvalMySqlDayFunctions(fn, dialect, EvalArg, out var dayResult))
            return dayResult;

        if (TryEvalMySqlDatabaseFunctions(fn, dialect, EvalArg, out var databaseResult))
            return databaseResult;

        if (TryEvalMySqlStringMetadataFunctions(fn, dialect, EvalArg, out var stringMetadataResult))
            return stringMetadataResult;

        if (TryEvalMySqlSetFunctions(fn, dialect, EvalArg, out var setResult))
            return setResult;

        if (TryEvalMySqlHexFunctions(fn, dialect, EvalArg, out var hexResult))
            return hexResult;

        if (TryEvalMySqlFormatFunction(fn, dialect, EvalArg, out var mysqlFormatResult))
            return mysqlFormatResult;

        if (TryEvalMySqlRandomBytesFunction(fn, dialect, EvalArg, out var randomBytesResult))
            return randomBytesResult;

        if (TryEvalMySqlSleepFunction(fn, dialect, EvalArg, out var sleepResult))
            return sleepResult;

        if (TryEvalMySqlCompressFunctions(fn, dialect, EvalArg, out var compressResult))
            return compressResult;

        if (TryEvalMySqlFormatBytesFunction(fn, dialect, EvalArg, out var formatBytesResult))
            return formatBytesResult;

        if (TryEvalMySqlFormatPicoTimeFunction(fn, dialect, EvalArg, out var formatPicoTimeResult))
            return formatPicoTimeResult;

        if (TryEvalMySqlXmlFunctions(fn, dialect, EvalArg, out var mysqlXmlResult))
            return mysqlXmlResult;

        if (TryEvalMySqlCryptoFunctions(fn, dialect, EvalArg, out var mysqlCryptoResult))
            return mysqlCryptoResult;

        if (TryEvalMySqlDefaultFunction(fn, dialect, row, EvalArg, out var mysqlDefaultResult))
            return mysqlDefaultResult;

        if (TryEvalMySqlMemberOfFunction(fn, dialect, EvalArg, out var mysqlMemberOfResult))
            return mysqlMemberOfResult;

        if (TryEvalCoalesceFunction(fn, EvalArg, out var coalesceResult))
            return coalesceResult;

        if (TryEvalNullIfFunction(fn, dialect, EvalArg, out var nullIfResult))
            return nullIfResult;

        if (TryEvalAddMonthsFunction(fn, dialect, EvalArg, out var addMonthsResult))
            return addMonthsResult;

        if (TryEvalAsciiStrFunction(fn, dialect, EvalArg, out var asciiStrResult))
            return asciiStrResult;

        if (TryEvalBinToNumFunction(fn, dialect, EvalArg, out var binToNumResult))
            return binToNumResult;

        if (TryEvalBitAndFunction(fn, dialect, EvalArg, out var bitAndResult))
            return bitAndResult;

        if (TryEvalCardinalityFunction(fn, dialect, EvalArg, out var cardinalityResult))
            return cardinalityResult;

        if (TryEvalChrFunction(fn, dialect, EvalArg, out var chrResult))
            return chrResult;

        if (TryEvalComposeFunction(fn, dialect, EvalArg, out var composeResult))
            return composeResult;

        if (TryEvalConvertFunction(fn, dialect, EvalArg, out var convertResult))
            return convertResult;

        if (TryEvalDbTimeZoneFunction(fn, dialect, out var dbTimeZoneResult))
            return dbTimeZoneResult;

        if (TryEvalDecomposeFunction(fn, dialect, EvalArg, out var decomposeResult))
            return decomposeResult;

        if (TryEvalEmptyLobFunction(fn, dialect, out var emptyLobResult))
            return emptyLobResult;

        if (TryEvalInitCapFunction(fn, dialect, EvalArg, out var initCapResult))
            return initCapResult;

        if (TryEvalChartoRowidFunction(fn, dialect, EvalArg, out var chartoRowidResult))
            return chartoRowidResult;

        if (TryEvalClusterFunctions(fn, dialect, EvalArg, out var clusterResult))
            return clusterResult;

        if (TryEvalCollationFunction(fn, dialect, EvalArg, out var collationResult))
            return collationResult;

        if (TryEvalConIdFunctions(fn, dialect, EvalArg, out var conIdResult))
            return conIdResult;

        if (TryEvalCubeTableFunction(fn, dialect, EvalArg, out var cubeTableResult))
            return cubeTableResult;

        if (TryEvalCvFunction(fn, dialect, EvalArg, out var cvResult))
            return cvResult;

        if (TryEvalDataObjToPartitionFunctions(fn, dialect, EvalArg, out var dataObjResult))
            return dataObjResult;

        if (TryEvalDepthFunction(fn, dialect, EvalArg, out var depthResult))
            return depthResult;

        if (TryEvalDerefFunction(fn, dialect, EvalArg, out var derefResult))
            return derefResult;

        if (TryEvalDumpFunction(fn, dialect, EvalArg, out var dumpResult))
            return dumpResult;

        if (TryEvalExistsNodeFunction(fn, dialect, EvalArg, out var existsNodeResult))
            return existsNodeResult;

        if (TryEvalFromTzFunction(fn, dialect, EvalArg, out var fromTzResult))
            return fromTzResult;

        if (TryEvalGroupIdFunction(fn, dialect, out var groupIdResult))
            return groupIdResult;

        if (TryEvalHexToRawFunction(fn, dialect, EvalArg, out var hexToRawResult))
            return hexToRawResult;

        if (TryEvalIterationNumberFunction(fn, dialect, out var iterationResult))
            return iterationResult;

        if (TryEvalJsonDataGuideFunction(fn, dialect, EvalArg, out var jsonDataGuideResult))
            return jsonDataGuideResult;

        if (TryEvalJsonTransformFunction(fn, dialect, EvalArg, out var jsonTransformResult))
            return jsonTransformResult;

        if (TryEvalLnnvlFunction(fn, dialect, EvalArg, out var lnnvlResult))
            return lnnvlResult;

        if (TryEvalLocalTimeFunction(fn, dialect, out var localTimeResult))
            return localTimeResult;

        if (TryEvalLocalTimestampFunction(fn, dialect, out var localTimestampResult))
            return localTimestampResult;

        if (TryEvalLowerFunction(fn, dialect, EvalArg, out var lowerResult))
            return lowerResult;

        if (TryEvalLtrimFunction(fn, dialect, EvalArg, out var ltrimResult))
            return ltrimResult;

        if (TryEvalModFunction(fn, dialect, EvalArg, out var modResult))
            return modResult;

        if (TryEvalMonthsBetweenFunction(fn, dialect, EvalArg, out var monthsBetweenResult))
            return monthsBetweenResult;

        if (TryEvalNanvlFunction(fn, dialect, EvalArg, out var nanvlResult))
            return nanvlResult;

        if (TryEvalNewTimeFunction(fn, dialect, EvalArg, out var newTimeResult))
            return newTimeResult;

        if (TryEvalNextDayFunction(fn, dialect, EvalArg, out var nextDayResult))
            return nextDayResult;

        if (TryEvalNlsFunctions(fn, dialect, EvalArg, out var nlsResult))
            return nlsResult;

        if (TryEvalNumIntervalFunctions(fn, dialect, EvalArg, out var numIntervalResult))
            return numIntervalResult;

        if (TryEvalMakeRefFunction(fn, dialect, EvalArg, out var makeRefResult))
            return makeRefResult;

        if (TryEvalOracleApproxFunctions(fn, dialect, EvalArg, out var approxResult))
            return approxResult;

        if (TryEvalOracleBfilenameFunction(fn, dialect, EvalArg, out var bfileResult))
            return bfileResult;

        if (TryEvalOracleHashFunction(fn, dialect, EvalArg, out var hashResult))
            return hashResult;

        if (TryEvalOracleRawFunctions(fn, dialect, EvalArg, out var rawResult))
            return rawResult;

        if (TryEvalMySqlRegexFunctions(fn, dialect, EvalArg, out var mysqlRegexResult))
            return mysqlRegexResult;

        if (TryEvalOracleRegexFunctions(fn, dialect, EvalArg, out var regexResult))
            return regexResult;

        if (TryEvalOracleRemainderFunction(fn, dialect, EvalArg, out var remainderResult))
            return remainderResult;

        if (TryEvalOracleRowIdFunctions(fn, dialect, EvalArg, out var rowidResult))
            return rowidResult;

        if (TryEvalOracleSessionTimeZoneFunction(fn, dialect, out var sessionTzResult))
            return sessionTzResult;

        if (TryEvalOracleSysFunctions(fn, dialect, EvalArg, out var sysResult))
            return sysResult;

        if (TryEvalOracleToCharFunctions(fn, dialect, EvalArg, out var toCharResult))
            return toCharResult;

        if (TryEvalTranslateFunctions(fn, dialect, EvalArg, out var translateResult))
            return translateResult;

        if (TryEvalOracleUserEnvFunctions(fn, dialect, EvalArg, out var userEnvResult))
            return userEnvResult;

        if (TryEvalOracleValidateConversionFunction(fn, dialect, EvalArg, out var validateResult))
            return validateResult;

        if (TryEvalOracleVsizeFunction(fn, dialect, EvalArg, out var vsizeResult))
            return vsizeResult;

        if (TryEvalOracleWidthBucketFunction(fn, dialect, EvalArg, out var widthBucketResult))
            return widthBucketResult;

        if (TryEvalOracleAnalyticsFunctions(fn, dialect, EvalArg, out var analyticsResult))
            return analyticsResult;

        if (TryEvalOracleScnFunctions(fn, dialect, EvalArg, out var scnResult))
            return scnResult;

        if (TryEvalOracleTimeZoneOffsetFunction(fn, dialect, EvalArg, out var tzOffsetResult))
            return tzOffsetResult;

        if (TryEvalOracleXmlFunctions(fn, dialect, EvalArg, out var xmlResult))
            return xmlResult;

        if (TryEvalOracleUserFunction(fn, dialect, out var oracleUserResult))
            return oracleUserResult;

        if (TryEvalPostgresSystemFunctions(fn, dialect, EvalArg, out var postgresSystemResult))
            return postgresSystemResult;

        if (TryEvalPostgresDateFunctions(fn, dialect, EvalArg, out var postgresDateResult))
            return postgresDateResult;

        if (TryEvalPostgresScalarUtilityFunctions(fn, dialect, EvalArg, out var postgresScalarUtilityResult))
            return postgresScalarUtilityResult;

        if (TryEvalPostgresTextFunctions(fn, dialect, EvalArg, out var postgresTextResult))
            return postgresTextResult;

        if (TryEvalPostgresNetworkFunctions(fn, dialect, EvalArg, out var postgresNetworkResult))
            return postgresNetworkResult;

        if (TryEvalPostgresUnicodeFunctions(fn, dialect, EvalArg, out var postgresUnicodeResult))
            return postgresUnicodeResult;

        if (TryEvalPostgresRegexFunctions(fn, dialect, EvalArg, out var postgresRegexResult))
            return postgresRegexResult;

        if (TryEvalPostgresArrayFunctions(fn, dialect, EvalArg, out var postgresArrayResult))
            return postgresArrayResult;

        if (TryEvalPostgresJsonFunctions(fn, dialect, EvalArg, out var postgresJsonResult))
            return postgresJsonResult;

        if (TryEvalPostgresUuidFunctions(fn, dialect, out var postgresUuidResult))
            return postgresUuidResult;

        var numericResult = TryEvalNumericFunction(fn, EvalArg, out var handledNumeric);
        if (handledNumeric)
        {
            if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                && (fn.Name.Equals("COT", StringComparison.OrdinalIgnoreCase)
                    || fn.Name.Equals("DEGREES", StringComparison.OrdinalIgnoreCase)
                    || fn.Name.Equals("FLOOR", StringComparison.OrdinalIgnoreCase))
                && dialect.Version >= 84)
            {
                throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
            }

            return numericResult;
        }

        if (TryEvalAppNameFunction(fn, out var appNameResult))
            return appNameResult;

        if (TryEvalCharIndexFunction(fn, EvalArg, out var charIndexResult))
            return charIndexResult;

        if (TryEvalCurrentUserFunction(fn, dialect, out var currentUserResult))
            return currentUserResult;

        if (dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && (fn.Name.Equals("APP_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("APPLOCK_MODE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("APPLOCK_TEST", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ASSEMBLYPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CERTENCODED", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CERTPRIVATEKEY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CURSOR_STATUS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DB_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CURRENT_REQUEST_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CURRENT_TRANSACTION_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CONTEXT_INFO", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATABASE_PRINCIPAL_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATABASEPROPERTYEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CONNECTIONPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COLUMNPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DB_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COL_LENGTH", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COL_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECT_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILE_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILE_IDEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILE_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILEGROUP_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILEGROUP_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILEGROUPPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FILEPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FULLTEXTCATALOGPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FULLTEXTSERVICEPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("GET_FILESTREAM_TRANSACTION_CONTEXT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("HAS_PERMS_BY_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("INDEX_COL", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("INDEXKEY_PROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("INDEXPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("MIN_ACTIVE_ROWVERSION", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECT_DEFINITION", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECTPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECTPROPERTYEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECT_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("OBJECT_SCHEMA_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("IS_MEMBER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("IS_ROLEMEMBER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("IS_SRVROLEMEMBER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ORIGINAL_DB_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ORIGINAL_LOGIN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("PWDCOMPARE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("PWDENCRYPT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SCHEMA_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SCHEMA_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SESSION_CONTEXT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SERVERPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SESSION_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SUSER_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SUSER_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SUSER_SID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SUSER_SNAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("STATS_DATE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TYPE_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TYPE_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TYPEPROPERTY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("USER_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("USER_NAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("XACT_STATE", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsSqlServerMetadataFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && (fn.Name.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATENAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATEPART", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DAY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("MONTH", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("YEAR", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsSqlServerDateFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && (fn.Name.Equals("ABS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ACOS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ASCII", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ASIN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ATAN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ATN2", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("BINARY_CHECKSUM", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CEILING", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CHARINDEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CHECKSUM", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COMPRESS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DECOMPRESS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("COT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DEGREES", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DIFFERENCE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("EXP", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FLOOR", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FORMAT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("FORMATMESSAGE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATALENGTH", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("DATEDIFF_BIG", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("GROUPING", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("GROUPING_ID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ISDATE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ISJSON", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ISNUMERIC", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CHAR", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CONCAT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LEN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LEFT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LOG", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LOG10", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("PI", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("POWER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("RADIANS", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("RAND", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("NCHAR", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("NEWID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("NEWSEQUENTIALID", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("REPLACE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("RIGHT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("ROUND", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SIGN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SIN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SQUARE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("STR", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TAN", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("STRING_ESCAPE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TRANSLATE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TRIM", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("UPPER", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("LTRIM", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("PARSENAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("PATINDEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("QUOTENAME", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("REPLICATE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("REVERSE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("RTRIM", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SOUNDEX", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SPACE", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("SQRT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("STUFF", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("UNICODE", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsSqlServerScalarFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (TryEvalSqlServerDatabaseFunctions(fn, dialect, EvalArg, out var sqlServerDatabaseResult))
            return sqlServerDatabaseResult;

        if (TryEvalSqlServerServerPropertyFunction(fn, dialect, EvalArg, out var sqlServerServerPropertyResult))
            return sqlServerServerPropertyResult;

        if (TryEvalSqlServerConnectionPropertyFunction(fn, dialect, EvalArg, out var sqlServerConnectionPropertyResult))
            return sqlServerConnectionPropertyResult;

        if (TryEvalSqlServerContextInfoFunction(fn, dialect, out var sqlServerContextInfoResult))
            return sqlServerContextInfoResult;

        if (TryEvalSqlServerSessionFunctions(fn, dialect, EvalArg, out var sqlServerSessionResult))
            return sqlServerSessionResult;

        if (TryEvalSqlServerIdentityFunctions(fn, dialect, EvalArg, out var sqlServerIdentityResult))
            return sqlServerIdentityResult;

        if (TryEvalDataLengthFunction(fn, EvalArg, out var dataLengthResult))
            return dataLengthResult;

        if (TryEvalDateNameFunction(fn, row, group, ctes, EvalArg, out var dateNameResult))
            return dateNameResult;

        if (TryEvalDatePartFunction(fn, row, group, ctes, EvalArg, out var datePartResult))
            return datePartResult;

        if (TryEvalDb2DateAliasFunction(fn, dialect, EvalArg, out var db2DateAliasResult))
            return db2DateAliasResult;

        if (TryEvalDegreesFunction(fn, EvalArg, out var degreesResult))
            return degreesResult;

        if (TryEvalDateDiffBigFunction(fn, row, group, ctes, EvalArg, out var dateDiffBigResult))
            return dateDiffBigResult;

        if (TryEvalDateFromPartsFunction(fn, EvalArg, out var dateFromPartsResult))
            return dateFromPartsResult;

        if (TryEvalDateTimeFromPartsFunction(fn, EvalArg, out var dateTimeFromPartsResult))
            return dateTimeFromPartsResult;

        if (TryEvalDateTime2FromPartsFunction(fn, EvalArg, out var dateTime2FromPartsResult))
            return dateTime2FromPartsResult;

        if (TryEvalDateTimeOffsetFromPartsFunction(fn, EvalArg, out var dateTimeOffsetFromPartsResult))
            return dateTimeOffsetFromPartsResult;

        if (TryEvalTimeFromPartsFunction(fn, EvalArg, out var timeFromPartsResult))
            return timeFromPartsResult;

        if (TryEvalSmallDateTimeFromPartsFunction(fn, EvalArg, out var smallDateTimeFromPartsResult))
            return smallDateTimeFromPartsResult;

        if (fn.Name.Equals("EOMONTH", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsEomonthFunction)
            throw SqlUnsupported.ForDialect(dialect, "EOMONTH");

        if (TryEvalEomonthFunction(fn, EvalArg, out var eomonthResult))
            return eomonthResult;

        if (TryEvalDifferenceFunction(fn, EvalArg, out var differenceResult))
            return differenceResult;

        if (TryEvalErrorFunctions(fn, out var errorResult))
            return errorResult;

        if (TryEvalExpFunction(fn, EvalArg, out var expResult))
            return expResult;

        if (TryEvalFloorFunction(fn, EvalArg, out var floorResult))
            return floorResult;

        if (TryEvalSqlServerFormatFunction(fn, dialect, EvalArg, out var sqlServerFormatResult))
            return sqlServerFormatResult;

        if (TryEvalSqlServerFormatMessageFunction(fn, EvalArg, out var sqlServerFormatMessageResult))
            return sqlServerFormatMessageResult;

        if (TryEvalSqlServerCompressFunction(fn, EvalArg, out var sqlServerCompressResult))
            return sqlServerCompressResult;

        if (TryEvalSqlServerDecompressFunction(fn, EvalArg, out var sqlServerDecompressResult))
            return sqlServerDecompressResult;

        if (TryEvalSqlServerChecksumFunction(fn, EvalArg, out var sqlServerChecksumResult))
            return sqlServerChecksumResult;

        if (fn.Name.Equals("GETUTCDATE", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsGetUtcDateFunction)
            throw SqlUnsupported.ForDialect(dialect, "GETUTCDATE");

        if (TryEvalGetUtcDateFunction(fn, out var getUtcDateResult))
            return getUtcDateResult;

        if (TryEvalGetAnsiNullFunction(fn, out var getAnsiNullResult))
            return getAnsiNullResult;

        if (TryEvalGroupingFunctions(fn, dialect, EvalArg, out var groupingResult))
            return groupingResult;

        if (TryEvalHostFunctions(fn, out var hostResult))
            return hostResult;

        if (TryEvalSessionContextFunction(fn, EvalArg, out var sessionContextResult))
            return sessionContextResult;

        if (TryEvalSqlServerGuidFunctions(fn, out var sqlServerGuidResult))
            return sqlServerGuidResult;

        if (TryEvalSqlServerStringEscapeFunction(fn, EvalArg, out var sqlServerStringEscapeResult))
            return sqlServerStringEscapeResult;

        if (TryEvalSqlServerStrFunction(fn, EvalArg, out var sqlServerStrResult))
            return sqlServerStrResult;

        if (TryEvalSqlServerDateTimeOffsetFunctions(fn, EvalArg, out var sqlServerDateTimeOffsetResult))
            return sqlServerDateTimeOffsetResult;

        if (TryEvalIsDateFunction(fn, EvalArg, out var isDateResult))
            return isDateResult;

        if (TryEvalIsJsonFunction(fn, EvalArg, out var isJsonResult))
            return isJsonResult;

        if (TryEvalIsNumericFunction(fn, EvalArg, out var isNumericResult))
            return isNumericResult;

        if (TryEvalAddDateFunction(fn, EvalArg, out var addDateResult))
            return addDateResult;

        if (TryEvalAddTimeFunction(fn, EvalArg, out var addTimeResult))
            return addTimeResult;

        if (TryEvalIpFunctions(fn, EvalArg, out var ipResult))
            return ipResult;

        if (TryEvalIsUuidFunction(fn, EvalArg, out var uuidResult))
            return uuidResult;

        if (TryEvalJsonArrayFunction(fn, EvalArg, out var jsonArrayResult))
            return jsonArrayResult;

        if (TryEvalJsonDepthFunction(fn, EvalArg, out var jsonDepthResult))
            return jsonDepthResult;

        if (TryEvalJsonUtilityFunctions(fn, dialect, EvalArg, out var jsonUtilityResult))
            return jsonUtilityResult;

        if (TryEvalMinMaxFunctions(fn, dialect, EvalArg, out var minMaxResult))
            return minMaxResult;

        if (TryEvalLastDayFunction(fn, EvalArg, out var lastDayResult))
            return lastDayResult;

        if (TryEvalLastInsertIdFunction(fn, EvalArg, out var lastInsertIdResult))
            return lastInsertIdResult;

        if (TryEvalLocateFunction(fn, dialect, EvalArg, out var locateResult))
            return locateResult;

        if (TryEvalLogFunctions(fn, dialect, EvalArg, out var logResult))
            return logResult;

        if (TryEvalInstrFunction(fn, EvalArg, out var instrResult))
            return instrResult;

        if (TryEvalGlobFunction(fn, EvalArg, out var globResult))
            return globResult;

        if (TryEvalLikeFunction(fn, dialect, EvalArg, out var likeResult))
            return likeResult;

        if (TryEvalPatIndexFunction(fn, dialect, EvalArg, out var patIndexResult))
            return patIndexResult;

        if (TryEvalStrftimeFunction(fn, EvalArg, out var strftimeResult))
            return strftimeResult;

        if (TryEvalPrintfFunction(fn, dialect, EvalArg, out var printfResult))
            return printfResult;

        if (TryEvalRandomFunctions(fn, dialect, EvalArg, out var randomResult))
            return randomResult;

        if (TryEvalTypeofFunction(fn, EvalArg, out var typeofResult))
            return typeofResult;

        if (TryEvalUnicodeFunctions(fn, EvalArg, out var unicodeResult))
            return unicodeResult;

        if (TryEvalSqliteSystemFunctions(fn, dialect, EvalArg, out var sqliteSystemResult))
            return sqliteSystemResult;

        if (TryEvalSqliteJsonFunctions(fn, dialect, EvalArg, out var sqliteJsonResult))
            return sqliteJsonResult;

        if (TryEvalLikelihoodFunctions(fn, EvalArg, out var likelihoodResult))
            return likelihoodResult;

        if (TryEvalPadFunctions(fn, EvalArg, out var padResult))
            return padResult;

        if (TryEvalMakeDateFunction(fn, EvalArg, out var makeDateResult))
            return makeDateResult;

        if (TryEvalMakeTimeFunction(fn, EvalArg, out var makeTimeResult))
            return makeTimeResult;

        if (TryEvalMicrosecondFunction(fn, EvalArg, out var microsecondResult))
            return microsecondResult;

        if (TryEvalMd5Function(fn, EvalArg, out var md5Result))
            return md5Result;

        if (TryEvalModFunction(fn, EvalArg, out var modResult2))
            return modResult2;

        if (TryEvalMonthNameFunction(fn, EvalArg, out var monthNameResult))
            return monthNameResult;

        if (TryEvalOctFunction(fn, EvalArg, out var octResult))
            return octResult;

        if (TryEvalHexFunction(fn, EvalArg, out var hexResult))
            return hexResult;

        if (TryEvalUnhexFunction(fn, EvalArg, out var unhexResult))
            return unhexResult;

        if (TryEvalOctetLengthFunction(fn, EvalArg, out var octetLengthResult))
            return octetLengthResult;

        if (TryEvalNameConstFunction(fn, EvalArg, out var nameConstResult))
            return nameConstResult;

        if (TryEvalOrdFunction(fn, EvalArg, out var ordResult))
            return ordResult;

        if (TryEvalPositionFunction(fn, EvalArg, out var positionResult))
            return positionResult;

        if (TryEvalPiFunction(fn, out var piResult))
            return piResult;

        if (TryEvalPowerFunctions(fn, EvalArg, out var powerResult))
            return powerResult;

        if (TryEvalPeriodFunctions(fn, EvalArg, out var periodResult))
            return periodResult;

        if (TryEvalQuarterFunction(fn, EvalArg, out var quarterResult))
            return quarterResult;

        if (TryEvalQuoteFunction(fn, EvalArg, out var quoteResult))
            return quoteResult;

        if (TryEvalSqlServerScalarFunctions(fn, dialect, EvalArg, out var sqlServerScalarResult))
            return sqlServerScalarResult;

        if (TryEvalRadiansFunction(fn, EvalArg, out var radiansResult))
            return radiansResult;

        if (TryEvalRandFunction(fn, EvalArg, out var randResult))
            return randResult;

        if (TryEvalRepeatFunction(fn, EvalArg, out var repeatResult))
            return repeatResult;

        if (TryEvalReverseFunction(fn, EvalArg, out var reverseResult))
            return reverseResult;

        if (TryEvalLeftFunction(fn, EvalArg, out var leftResult))
            return leftResult;

        if (TryEvalRightFunction(fn, EvalArg, out var rightResult))
            return rightResult;

        if (TryEvalRoundFunction(fn, EvalArg, out var roundResult))
            return roundResult;

        if (TryEvalPadRightFunction(fn, EvalArg, out var padRightResult))
            return padRightResult;

        if (TryEvalSecToTimeFunction(fn, EvalArg, out var secToTimeResult))
            return secToTimeResult;

        if (TryEvalShaFunctions(fn, EvalArg, out var shaResult))
            return shaResult;

        if (TryEvalSinFunction(fn, EvalArg, out var sinResult))
            return sinResult;

        if (TryEvalSoundexFunction(fn, EvalArg, out var soundexResult))
            return soundexResult;

        if (TryEvalSpaceFunction(fn, EvalArg, out var spaceResult))
            return spaceResult;

        if (TryEvalSqrtFunction(fn, EvalArg, out var sqrtResult))
            return sqrtResult;

        if (TryEvalSubDateFunction(fn, row, group, ctes, EvalArg, out var subDateResult))
            return subDateResult;

        if (TryEvalSubTimeFunction(fn, EvalArg, out var subTimeResult))
            return subTimeResult;

        if (TryEvalSubstringIndexFunction(fn, EvalArg, out var substringIndexResult))
            return substringIndexResult;

        if (TryEvalTanFunction(fn, EvalArg, out var tanResult))
            return tanResult;

        if (TryEvalTimeFormatFunction(fn, EvalArg, out var timeFormatResult))
            return timeFormatResult;

        if (TryEvalTimeToSecFunction(fn, EvalArg, out var timeToSecResult))
            return timeToSecResult;

        if (TryEvalTimeDiffFunction(fn, EvalArg, out var timeDiffResult))
            return timeDiffResult;

        if (TryEvalSessionUserFunction(fn, dialect, out var sessionUserResult))
            return sessionUserResult;

        if (TryEvalSystemUserFunction(fn, dialect, out var systemUserResult))
            return systemUserResult;

        if (TryEvalToDaysFunction(fn, EvalArg, out var toDaysResult))
            return toDaysResult;

        if (TryEvalToSecondsFunction(fn, EvalArg, out var toSecondsResult))
            return toSecondsResult;

        if (TryEvalTruncateFunction(fn, EvalArg, out var truncateResult))
            return truncateResult;

        if (TryEvalUnixTimestampFunction(fn, EvalArg, out var unixTimestampResult))
            return unixTimestampResult;

        if (TryEvalUserFunction(fn, out var userResult))
            return userResult;

        if (TryEvalUtcDateFunction(fn, out var utcDateResult))
            return utcDateResult;

        if (TryEvalUtcTimeFunction(fn, out var utcTimeResult))
            return utcTimeResult;

        if (TryEvalUtcTimestampFunction(fn, out var utcTimestampResult))
            return utcTimestampResult;

        if (TryEvalUuidShortFunction(fn, out var uuidShortResult))
            return uuidShortResult;

        if (TryEvalWeekFunctions(fn, dialect, EvalArg, out var weekResult))
            return weekResult;

        EnsureDialectSupportsSequenceFunction(fn.Name);
        if (SqlSequenceEvaluator.TryEvaluateCall(_cnn, fn.Name, fn.Args, expr => Eval(expr, row, group, ctes), out var sequenceValue))
            return sequenceValue;

        var jsonNumberResult = TryEvalJsonAndNumberFunctions(fn, dialect, EvalArg, out var handledJsonNumber);
        if (handledJsonNumber)
            return jsonNumberResult;

        // TRY_CAST(x AS TYPE) - similar ao CAST, mas retorna null em falha
        if (fn.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryCastFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_CAST");

            return EvalTryCast(fn, EvalArg);
        }

        // TRY_CONVERT(TYPE, x[, style]) - sintaxe do SQL Server, normalizada pelo parser
        if (fn.Name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryConvertFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_CONVERT");

            return EvalTryCast(fn, EvalArg);
        }

        if (fn.Name.Equals("PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsParseFunction)
                throw SqlUnsupported.ForDialect(dialect, "PARSE");

            return EvalParseFunction(fn, EvalArg, false);
        }

        if (fn.Name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryParseFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_PARSE");

            return EvalParseFunction(fn, EvalArg, true);
        }

        // CAST(x AS TYPE) - aqui chega como CallExpr("CAST", [expr, RawSqlExpr("SIGNED")]) via parser
        if (fn.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
            return EvalCast(fn, EvalArg);
        var concatResult = TryEvalConcatFunctions(fn, EvalArg, out var handledConcat);
        if (handledConcat)
            return concatResult;

        if (TryEvalCharFunction(fn, dialect, EvalArg, out var charResult))
            return charResult;

        if (TryEvalDialectSpecificCastFunction(fn, dialect, EvalArg, out var dialectSpecificCastResult))
            return dialectSpecificCastResult;

        if (TryEvalBasicStringFunction(fn, EvalArg, out var basicStringResult))
            return basicStringResult;

        if (TryEvalSubstringFunction(fn, EvalArg, out var substringResult))
            return substringResult;

        if (TryEvalReplaceFunction(fn, EvalArg, out var replaceResult))
            return replaceResult;

        if (TryEvalDaysFunction(fn, EvalArg, out var daysResult))
            return daysResult;

        if (TryEvalDateDiffFunction(fn, row, group, ctes, EvalArg, out var dateDiffResult))
            return dateDiffResult;

        if (TryEvalTimestampDiffFunction(fn, row, group, ctes, EvalArg, out var timestampDiffResult))
            return timestampDiffResult;

        var dateAddResult = TryEvalDateAddFunction(fn, row, group, ctes, EvalArg, out var handledDateAdd);
        if (handledDateAdd)
            return dateAddResult;

        if (TryEvalDateConstructionFunction(fn, EvalArg, out var dateConstructionResult))
            return dateConstructionResult;

        if (TryEvalJulianDayFunction(fn, EvalArg, out var julianDayResult))
            return julianDayResult;

        if (TryEvalTruncFunction(fn, EvalArg, out var truncResult))
            return truncResult;

        if (TryEvalExtractFunction(fn, row, group, ctes, EvalArg, out var extractResult))
            return extractResult;

        if (TryEvalFieldFunction(fn, dialect, EvalArg, out var fieldResult))
            return fieldResult;

        if (fn.Args.Count == 0
            && SqlTemporalFunctionEvaluator.IsKnownTemporalFunctionName(fn.Name))
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for dialect '{dialect.Name}'.");

// Unknown scalar => null (don't explode tests)
        return null;

        object? EvalArg(int i) => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null;
    }

    private static bool TryEvalCharFunction(
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
            || dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
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

    private bool TryEvalFindInSetFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FIND_IN_SET", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var parts = haystack.Split(',').Select(static part => part.Trim()).ToArray();
        var index = Array.FindIndex(parts, part => string.Equals(part, needle, StringComparison.OrdinalIgnoreCase));
        result = index >= 0 ? index + 1 : 0;
        return true;
    }

    private bool TryEvalMatchAgainstFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MATCH_AGAINST", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.SupportsMatchAgainstPredicate)
            throw SqlUnsupported.ForDialect(dialect, "MATCH ... AGAINST full-text predicate");

        if (fn.Args.Count < 2)
        {
            result = 0;
            return true;
        }

        var haystack = FlattenMatchAgainstTarget(evalArg(0));
        var query = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(haystack) || string.IsNullOrWhiteSpace(query))
        {
            result = 0;
            return true;
        }

        var terms = ExtractMatchAgainstTerms(query);
        if (terms.Count == 0)
        {
            result = 0;
            return true;
        }

        var modeSql = fn.Args.Count > 2
            ? (fn.Args[2] is RawSqlExpr rx ? rx.Sql : evalArg(2)?.ToString() ?? string.Empty)
            : string.Empty;

        result = EvaluateMatchAgainstTerms(haystack, terms, modeSql, dialect.TextComparison);
        return true;
    }

    private static int EvaluateMatchAgainstTerms(
        string haystack,
        IReadOnlyList<MatchAgainstTerm> terms,
        string modeSql,
        StringComparison comparison)
    {
        var isBooleanMode = modeSql.IndexOf("BOOLEAN MODE", StringComparison.OrdinalIgnoreCase) >= 0;
        var haystackWords = ExtractMatchAgainstWords(haystack);
        var score = 0;

        foreach (var term in terms)
        {
            var found = ContainsMatchAgainstTerm(haystack, haystackWords, term, comparison);
            if (isBooleanMode)
            {
                if (term.Prohibited && found)
                    return 0;

                if (term.Required && !found)
                    return 0;
            }

            if (found && !term.Prohibited)
                score++;
        }

        return score;
    }

    private static bool TryEvalConditionalFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isIf = fn.Name.Equals("IF", StringComparison.OrdinalIgnoreCase);
        var isIif = fn.Name.Equals("IIF", StringComparison.OrdinalIgnoreCase);
        if (!((isIf && dialect.SupportsIfFunction) || (isIif && dialect.SupportsIifFunction)))
        {
            result = null;
            return false;
        }

        var condition = evalArg(0).ToBool();
        result = condition ? evalArg(1) : evalArg(2);
        return true;
    }

    private static bool TryEvalNullSubstituteFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase)
            || !dialect.NullSubstituteFunctionNames.Any(name => name.Equals(fn.Name, StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(1) : value;
        return true;
    }

    private static bool TryEvalNvl2Function(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NVL2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("NVL2() espera 3 argumentos.");

        var value = evalArg(0);
        result = IsNullish(value) ? evalArg(2) : evalArg(1);
        return true;
    }

    private static bool TryEvalDecodeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DECODE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count != 2)
                throw new InvalidOperationException("DECODE() no PostgreSQL espera payload e formato.");

            var payload = evalArg(0)?.ToString();
            var format = evalArg(1)?.ToString();
            if (string.IsNullOrWhiteSpace(payload) || string.IsNullOrWhiteSpace(format))
            {
                result = null;
                return true;
            }

            try
            {
                result = format!.Trim().ToLowerInvariant() switch
                {
                    "hex" when TryNormalizeHexPayload(payload!.Trim(), out var hex) && hex.Length % 2 == 0
                        => ParseHexBinaryPayload(hex),
                    "base64" => Convert.FromBase64String(payload),
                    _ => null
                };
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DECODE() espera ao menos 3 argumentos.");

        var expr = evalArg(0);
        var pairCount = (fn.Args.Count - 1) / 2;
        var hasDefault = (fn.Args.Count - 1) % 2 == 1;

        for (int i = 0; i < pairCount; i++)
        {
            var search = evalArg(1 + i * 2);
            var resultValue = evalArg(2 + i * 2);

            if (DecodeEquals(expr, search, dialect))
            {
                result = resultValue;
                return true;
            }
        }

        result = hasDefault ? evalArg(fn.Args.Count - 1) : null;
        return true;
    }

    private static bool DecodeEquals(object? left, object? right, ISqlDialect dialect)
    {
        if (IsNullish(left) && IsNullish(right))
            return true;

        if (IsNullish(left) || IsNullish(right))
            return false;

        return left!.EqualsSql(right!, dialect);
    }

    private static byte[] ParseHexBinaryPayload(string hex)
    {
        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            buffer[i / 2] = byte.Parse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return buffer;
    }

    private static bool TryEvalCoalesceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        for (int i = 0; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (!IsNullish(value))
            {
                result = value;
                return true;
            }
        }

        result = null;
        return true;
    }

    private static bool TryEvalNullIfFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = left;
            return true;
        }

        result = left!.Compare(right!, dialect) == 0 ? null : left;
        return true;
    }

    private static bool TryEvalAddMonthsFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ADD_MONTHS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADD_MONTHS() espera data e quantidade de meses.");

        var baseValue = evalArg(0);
        var monthsValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(monthsValue))
        {
            result = null;
            return true;
        }

        if (!TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        try
        {
            var months = Convert.ToInt32(monthsValue.ToDec());
            result = dateTime.AddMonths(months);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAsciiStrFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ASCIISTR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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
        var builder = new StringBuilder(text.Length);
        foreach (var ch in text)
        {
            if (ch <= 0x7F)
            {
                builder.Append(ch);
                continue;
            }

            builder.Append('\\');
            builder.Append(((int)ch).ToString("X4", CultureInfo.InvariantCulture));
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalBinToNumFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("BIN_TO_NUM", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        try
        {
            long acc = 0;
            for (var i = 0; i < fn.Args.Count; i++)
            {
                var bitValue = evalArg(i);
                if (IsNullish(bitValue))
                {
                    result = null;
                    return true;
                }

                var bit = Convert.ToInt32(bitValue.ToDec(), CultureInfo.InvariantCulture);
                acc = (acc << 1) | (bit != 0 ? 1L : 0L);
            }

            result = acc;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitAndFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("BITAND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BITAND() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            var r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = l & r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCardinalityFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CARDINALITY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (value is System.Text.Json.JsonElement element
            && element.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            result = element.GetArrayLength();
            return true;
        }

        if (value is string)
        {
            result = null;
            return true;
        }

        if (value is Array arr)
        {
            result = arr.Length;
            return true;
        }

        if (value is ICollection collection)
        {
            result = collection.Count;
            return true;
        }

        if (value is IEnumerable enumerable)
        {
            var count = 0;
            foreach (var _ in enumerable)
                count++;
            result = count;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalChrFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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
            var code = Convert.ToInt32(value.ToDec(), CultureInfo.InvariantCulture);
            if (code < 0 || code > 0x10FFFF)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(code);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalComposeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("COMPOSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormC);
        return true;
    }

    private static bool TryEvalConvertFunction(
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

        if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("CONVERT() espera ao menos um argumento.");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            result = value is string text ? text : value!.ToString();
            return true;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalDbTimeZoneFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("DBTIMEZONE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "+00:00";
        return true;
    }

    private static bool TryEvalDecomposeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DECOMPOSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormD);
        return true;
    }

    private static bool TryEvalEmptyLobFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!(fn.Name.Equals("EMPTY_BLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("EMPTY_CLOB", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = fn.Name.Equals("EMPTY_BLOB", StringComparison.OrdinalIgnoreCase)
            ? Array.Empty<byte>()
            : string.Empty;
        return true;
    }

    private static bool TryEvalInitCapFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("INITCAP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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
        if (text.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var builder = new StringBuilder(text.Length);
        var makeUpper = true;
        foreach (var ch in text)
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

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalChartoRowidFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CHARTOROWID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        result = value?.ToString();
        return true;
    }

    private static bool TryEvalClusterFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CLUSTER_DETAILS" or "CLUSTER_DISTANCE" or "CLUSTER_ID" or "CLUSTER_PROBABILITY" or "CLUSTER_SET"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleClusterFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        // Data mining functions are not simulated: return null consistently.
        result = null;
        return true;
    }

    private static bool TryEvalMySqlBase64Functions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("FROM_BASE64", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TO_BASE64", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var isToBase64 = fn.Name.Equals("TO_BASE64", StringComparison.OrdinalIgnoreCase);
        var minSupportedVersion = 56;
        if (dialect.Version < minSupportedVersion
            || (isToBase64 && dialect.Version >= 84))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (isToBase64)
        {
            var bytes = value as byte[]
                ?? Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty);
            result = Convert.ToBase64String(bytes);
            return true;
        }

        var payload = value.ToString();
        if (string.IsNullOrWhiteSpace(payload))
        {
            result = null;
            return true;
        }

        try
        {
            result = Convert.FromBase64String(payload);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMySqlStringCompareFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRCMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STRCMP() espera dois argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        var comparison = string.Compare(
            Convert.ToString(left, CultureInfo.InvariantCulture),
            Convert.ToString(right, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);

        result = comparison < 0 ? -1 : comparison > 0 ? 1 : 0;
        return true;
    }

    private static bool TryEvalMySqlChecksumFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CRC32", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("CRC32() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var bytes = value as byte[]
            ?? Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
        var crc = ComputeCrc32(bytes);
        result = (long)crc;
        return true;
    }

    private static bool TryEvalMySqlNetworkFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("INET_ATON" or "INET_NTOA" or "INET6_ATON" or "INET6_NTOA"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        if (name is "INET_ATON" or "INET6_ATON")
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException($"{name}() espera um argumento.");
        }

        if (name is "INET_ATON")
        {
            if (dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, "INET_ATON");

            var textValue = evalArg(0);
            if (IsNullish(textValue))
            {
                result = null;
                return true;
            }

            var text = Convert.ToString(textValue, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!IPAddress.TryParse(text, out var address) || address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            {
                result = null;
                return true;
            }

            var bytes = address.GetAddressBytes();
            var numeric = ((uint)bytes[0] << 24) | ((uint)bytes[1] << 16) | ((uint)bytes[2] << 8) | bytes[3];
            result = (long)numeric;
            return true;
        }

        if (name is "INET_NTOA")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryConvertNumericToUInt64(value, out var numeric) || numeric > uint.MaxValue)
            {
                result = null;
                return true;
            }

            var bytes = new[]
            {
                (byte)((numeric >> 24) & 0xFF),
                (byte)((numeric >> 16) & 0xFF),
                (byte)((numeric >> 8) & 0xFF),
                (byte)(numeric & 0xFF)
            };
            result = new IPAddress(bytes).ToString();
            return true;
        }

        if (name is "INET6_ATON")
        {
            if (dialect.Version < 56 || dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, "INET6_ATON");

            var textValue = evalArg(0);
            if (IsNullish(textValue))
            {
                result = null;
                return true;
            }

            var text = Convert.ToString(textValue, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!IPAddress.TryParse(text, out var address))
            {
                result = null;
                return true;
            }

            result = address.GetAddressBytes();
            return true;
        }

        if (name is "INET6_NTOA")
        {
            if (dialect.Version < 56 || dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, "INET6_NTOA");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is not byte[] bytes || (bytes.Length != 4 && bytes.Length != 16))
            {
                result = null;
                return true;
            }

            result = new IPAddress(bytes).ToString();
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalMySqlUuidFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("UUID_TO_BIN" or "BIN_TO_UUID"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{name}() espera ao menos um argumento.");

        var swapFlag = false;
        if (fn.Args.Count > 1)
        {
            var flagValue = evalArg(1);
            if (!IsNullish(flagValue) && TryConvertNumericToInt64(flagValue, out var numericFlag))
                swapFlag = numericFlag != 0;
        }

        if (name == "UUID_TO_BIN")
        {
            if (dialect.Version < 80 || dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, "UUID_TO_BIN");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is byte[] byteValue)
            {
                if (byteValue.Length != 16)
                {
                    result = null;
                    return true;
                }

                result = swapFlag ? ApplyMySqlUuidSwap(byteValue) : byteValue.ToArray();
                return true;
            }

            var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!TryParseUuidHex(text, out var bytes))
            {
                result = null;
                return true;
            }

            result = swapFlag ? ApplyMySqlUuidSwap(bytes) : bytes;
            return true;
        }

        if (dialect.Version < 80)
            throw SqlUnsupported.ForDialect(dialect, "BIN_TO_UUID");

        var binValue = evalArg(0);
        if (IsNullish(binValue))
        {
            result = null;
            return true;
        }

        if (binValue is not byte[] binBytes || binBytes.Length != 16)
        {
            result = null;
            return true;
        }

        var normalized = swapFlag ? ApplyMySqlUuidUnswap(binBytes) : binBytes.ToArray();
        result = FormatUuid(normalized);
        return true;
    }

    private static bool TryConvertNumericToUInt64(object value, out ulong numeric)
    {
        numeric = 0;
        switch (value)
        {
            case byte b:
                numeric = b;
                return true;
            case sbyte sb:
                if (sb < 0) return false;
                numeric = (ulong)sb;
                return true;
            case short s:
                if (s < 0) return false;
                numeric = (ulong)s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i:
                if (i < 0) return false;
                numeric = (ulong)i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l:
                if (l < 0) return false;
                numeric = (ulong)l;
                return true;
            case ulong ul:
                numeric = ul;
                return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture);
        return ulong.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    private static bool TryConvertNumericToInt64(object value, out long numeric)
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

    private static uint ComputeCrc32(byte[] bytes)
    {
        var table = Crc32Table.Value;
        var crc = uint.MaxValue;
        foreach (var b in bytes)
        {
            var index = (crc ^ b) & 0xFF;
            crc = (crc >> 8) ^ table[index];
        }

        return crc ^ uint.MaxValue;
    }

    private static readonly Lazy<uint[]> Crc32Table = new(static () =>
    {
        var table = new uint[256];
        for (var i = 0; i < table.Length; i++)
        {
            var crc = (uint)i;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0
                    ? (crc >> 1) ^ 0xEDB88320u
                    : crc >> 1;
            }

            table[i] = crc;
        }

        return table;
    });

    private static bool TryParseUuidHex(string text, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var normalized = text.Trim().Trim('{', '}').Replace("-", string.Empty, StringComparison.Ordinal);
        if (normalized.Length != 32)
            return false;

        bytes = new byte[16];
        for (var i = 0; i < 16; i++)
        {
            if (!byte.TryParse(normalized.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out bytes[i]))
                return false;
        }

        return true;
    }

    private static byte[] ApplyMySqlUuidSwap(byte[] bytes)
    {
        var swapped = new byte[16];
        var map = new[] { 6, 7, 4, 5, 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15 };
        for (var i = 0; i < swapped.Length; i++)
            swapped[i] = bytes[map[i]];
        return swapped;
    }

    private static byte[] ApplyMySqlUuidUnswap(byte[] bytes)
    {
        var swapped = new byte[16];
        var map = new[] { 4, 5, 6, 7, 2, 3, 0, 1, 8, 9, 10, 11, 12, 13, 14, 15 };
        for (var i = 0; i < swapped.Length; i++)
            swapped[i] = bytes[map[i]];
        return swapped;
    }

    private static string FormatUuid(byte[] bytes)
        => $"{bytes[0]:x2}{bytes[1]:x2}{bytes[2]:x2}{bytes[3]:x2}-{bytes[4]:x2}{bytes[5]:x2}-{bytes[6]:x2}{bytes[7]:x2}-{bytes[8]:x2}{bytes[9]:x2}-{bytes[10]:x2}{bytes[11]:x2}{bytes[12]:x2}{bytes[13]:x2}{bytes[14]:x2}{bytes[15]:x2}";

    private static bool TryEvalMySqlDateFormatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATE_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATE_FORMAT() espera data e formato.");

        var value = evalArg(0);
        var formatValue = evalArg(1)?.ToString();
        if (IsNullish(value) || string.IsNullOrWhiteSpace(formatValue) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var dotNetFormat = ConvertMySqlDateFormatToDotNet(formatValue!);
        result = dateTime.ToString(dotNetFormat, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalMySqlStrToDateFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STR_TO_DATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "STR_TO_DATE");

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STR_TO_DATE() espera texto e formato.");

        var textValue = evalArg(0)?.ToString();
        var formatValue = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(textValue) || string.IsNullOrWhiteSpace(formatValue))
        {
            result = null;
            return true;
        }

        var dotNetFormat = ConvertMySqlDateFormatToDotNet(formatValue!);
        if (DateTime.TryParseExact(textValue, dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsed))
        {
            result = parsed;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalMySqlFromUnixTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FROM_UNIXTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FROM_UNIXTIME() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value) || !TryConvertNumericToDouble(value, out var seconds))
        {
            result = null;
            return true;
        }

        var dateTime = DateTime.UnixEpoch.AddSeconds(seconds);
        if (fn.Args.Count > 1)
        {
            var formatValue = evalArg(1)?.ToString();
            if (string.IsNullOrWhiteSpace(formatValue))
            {
                result = null;
                return true;
            }

            var dotNetFormat = ConvertMySqlDateFormatToDotNet(formatValue!);
            result = dateTime.ToString(dotNetFormat, CultureInfo.InvariantCulture);
            return true;
        }

        result = dateTime;
        return true;
    }

    private static bool TryEvalMySqlFromDaysFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FROM_DAYS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "FROM_DAYS");

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FROM_DAYS() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value) || !TryConvertNumericToInt64(value, out var days) || days < 1)
        {
            result = null;
            return true;
        }

        result = new DateTime(1, 1, 1).AddDays(days - 1);
        return true;
    }

    private static bool TryEvalMySqlDateSubFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATE_SUB", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "DATE_SUB");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalExpression = fn.Args.Count > 1 ? fn.Args[1] : null;
        if (intervalExpression is not CallExpr intervalCall
            || !intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
            || intervalCall.Args.Count < 2)
        {
            result = dateTime;
            return true;
        }

        var amountObject = Eval(intervalCall.Args[0], row, group, ctes);
        var unit = intervalCall.Args[1] is RawSqlExpr raw
            ? raw.Sql
            : Eval(intervalCall.Args[1], row, group, ctes)?.ToString() ?? "DAY";
        result = ApplyDateDelta(dateTime, unit, -Convert.ToInt32((amountObject ?? 0m).ToDec()));
        return true;
    }

    private static bool TryEvalMySqlGetFormatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("GET_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("GET_FORMAT() espera tipo e formato.");

        var typeValue = evalArg(0)?.ToString();
        var formatValue = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(typeValue) || string.IsNullOrWhiteSpace(formatValue))
        {
            result = null;
            return true;
        }

        result = ResolveMySqlGetFormatPattern(typeValue!, formatValue!);
        return true;
    }

    private static bool TryEvalMySqlConvertTzFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONVERT_TZ", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "CONVERT_TZ");

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("CONVERT_TZ() espera data e dois fusos.");

        var value = evalArg(0);
        var fromValue = evalArg(1)?.ToString();
        var toValue = evalArg(2)?.ToString();
        if (IsNullish(value) || string.IsNullOrWhiteSpace(fromValue) || string.IsNullOrWhiteSpace(toValue) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!TryParseMySqlTimeZoneOffset(fromValue!, out var fromOffset)
            || !TryParseMySqlTimeZoneOffset(toValue!, out var toOffset))
        {
            result = null;
            return true;
        }

        result = dateTime - fromOffset + toOffset;
        return true;
    }

    private static string ConvertMySqlDateFormatToDotNet(string format)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%')
            {
                builder.Append(ch);
                continue;
            }

            if (i + 1 >= format.Length)
                break;

            i++;
            builder.Append(format[i] switch
            {
                'Y' => "yyyy",
                'y' => "yy",
                'm' => "MM",
                'c' => "M",
                'd' => "dd",
                'e' => "d",
                'H' => "HH",
                'k' => "H",
                'h' or 'I' => "hh",
                'l' => "h",
                'i' => "mm",
                's' or 'S' => "ss",
                'f' => "ffffff",
                'p' => "tt",
                'T' => "HH:mm:ss",
                'r' => "hh:mm:ss tt",
                'b' => "MMM",
                'M' => "MMMM",
                'a' => "ddd",
                'W' => "dddd",
                '%' => "%",
                _ => format[i].ToString()
            });
        }

        return builder.ToString();
    }

    private static string? ResolveMySqlGetFormatPattern(string type, string format)
    {
        var typeKey = type.Trim().ToUpperInvariant();
        var formatKey = format.Trim().ToUpperInvariant();
        return typeKey switch
        {
            "DATE" => formatKey switch
            {
                "USA" => "%m.%d.%Y",
                "JIS" or "ISO" => "%Y-%m-%d",
                "EUR" => "%d.%m.%Y",
                "INTERNAL" => "%Y%m%d",
                _ => null
            },
            "TIME" => formatKey switch
            {
                "USA" => "%h:%i:%s %p",
                "JIS" or "ISO" => "%H:%i:%s",
                "EUR" => "%H.%i.%s",
                "INTERNAL" => "%H%i%s",
                _ => null
            },
            "DATETIME" or "TIMESTAMP" => formatKey switch
            {
                "USA" => "%m.%d.%Y %h:%i:%s %p",
                "JIS" or "ISO" => "%Y-%m-%d %H:%i:%s",
                "EUR" => "%d.%m.%Y %H.%i.%s",
                "INTERNAL" => "%Y%m%d%H%i%s",
                _ => null
            },
            _ => null
        };
    }

    private static bool TryParseMySqlTimeZoneOffset(string text, out TimeSpan offset)
    {
        offset = TimeSpan.Zero;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var normalized = text.Trim().ToUpperInvariant();
        if (normalized is "UTC" or "GMT" or "SYSTEM")
        {
            offset = TimeSpan.Zero;
            return true;
        }

        if (normalized.Length == 6 && (normalized[0] == '+' || normalized[0] == '-') && normalized[3] == ':')
        {
            if (int.TryParse(normalized.AsSpan(1, 2), NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours)
                && int.TryParse(normalized.AsSpan(4, 2), NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (normalized[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        return false;
    }

    private static bool TryEvalMySqlSetFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("ELT" or "MAKE_SET" or "EXPORT_SET"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (name == "EXPORT_SET" && dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "EXPORT_SET");

        if (name == "ELT")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ELT() espera indice e valores.");

            var indexValue = evalArg(0);
            if (IsNullish(indexValue))
            {
                result = null;
                return true;
            }

            if (!TryConvertNumericToInt64(indexValue!, out var index) || index <= 0)
            {
                result = null;
                return true;
            }

            var position = (int)index;
            if (position >= fn.Args.Count)
            {
                result = null;
                return true;
            }

            var value = evalArg(position);
            result = IsNullish(value) ? null : value;
            return true;
        }

        if (name == "MAKE_SET")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("MAKE_SET() espera bits e valores.");

            var bitsValue = evalArg(0);
            if (IsNullish(bitsValue) || !TryConvertNumericToInt64(bitsValue!, out var bits))
            {
                result = null;
                return true;
            }

            var selected = new List<string>();
            for (var i = 1; i < fn.Args.Count; i++)
            {
                if ((bits & (1L << (i - 1))) == 0)
                    continue;

                var value = evalArg(i);
                if (IsNullish(value))
                    continue;

                selected.Add(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
            }

            result = selected.Count == 0 ? null : string.Join(",", selected);
            return true;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("EXPORT_SET() espera bits, on, off.");

        var bitsExportValue = evalArg(0);
        if (IsNullish(bitsExportValue) || !TryConvertNumericToInt64(bitsExportValue!, out var bitsExport))
        {
            result = null;
            return true;
        }

        var onValue = evalArg(1);
        var offValue = evalArg(2);
        var separatorValue = fn.Args.Count > 3 ? evalArg(3) : ",";
        var limitValue = fn.Args.Count > 4 ? evalArg(4) : null;

        var onText = Convert.ToString(onValue, CultureInfo.InvariantCulture) ?? string.Empty;
        var offText = Convert.ToString(offValue, CultureInfo.InvariantCulture) ?? string.Empty;
        var separator = Convert.ToString(separatorValue, CultureInfo.InvariantCulture) ?? ",";
        var limit = 64;
        if (!IsNullish(limitValue) && TryConvertNumericToInt64(limitValue!, out var limitParsed) && limitParsed > 0)
            limit = (int)limitParsed;

        var pieces = new string[limit];
        for (var i = 0; i < limit; i++)
        {
            pieces[i] = (bitsExport & (1L << i)) != 0 ? onText : offText;
        }

        result = string.Join(separator, pieces);
        return true;
    }

    private static bool TryEvalMySqlHexFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("HEX" or "UNHEX"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{name}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (name == "HEX")
        {
            if (value is byte[] bytes)
            {
                result = Convert.ToHexString(bytes);
                return true;
            }

            if (value is string text)
            {
                result = Convert.ToHexString(Encoding.UTF8.GetBytes(text));
                return true;
            }

            try
            {
                var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                result = number.ToString("X", CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        var payload = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (payload.Length == 0)
        {
            result = Array.Empty<byte>();
            return true;
        }

        if (payload.Length % 2 == 1)
            payload = "0" + payload;

        var output = new byte[payload.Length / 2];
        for (var i = 0; i < output.Length; i++)
        {
            if (!byte.TryParse(payload.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var parsed))
            {
                result = null;
                return true;
            }

            output[i] = parsed;
        }

        result = output;
        return true;
    }

    private static bool TryEvalMySqlFormatFunction(
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

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FORMAT() espera valor e casas decimais.");

        var value = evalArg(0);
        var decimalsValue = evalArg(1);
        if (IsNullish(value) || IsNullish(decimalsValue))
        {
            result = null;
            return true;
        }

        var locale = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var cultureName = string.IsNullOrWhiteSpace(locale) ? string.Empty : locale!.Replace('_', '-');
        var culture = string.IsNullOrWhiteSpace(cultureName)
            ? CultureInfo.InvariantCulture
            : CultureInfo.GetCultureInfo(cultureName);

        if (!TryConvertNumericToInt64(decimalsValue!, out var decimalsParsed))
        {
            result = null;
            return true;
        }

        var decimals = (int)Math.Max(0, decimalsParsed);
        try
        {
            var numeric = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            result = numeric.ToString("N" + decimals, culture);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMySqlRandomBytesFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("RANDOM_BYTES", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version < 56 || dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "RANDOM_BYTES");

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("RANDOM_BYTES() espera o tamanho em bytes.");

        var lengthValue = evalArg(0);
        if (IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        if (!TryConvertNumericToInt64(lengthValue!, out var length) || length < 0 || length > int.MaxValue)
        {
            result = null;
            return true;
        }

        if (length == 0)
        {
            result = Array.Empty<byte>();
            return true;
        }

        var buffer = new byte[length];
        RandomNumberGenerator.Fill(buffer);
        result = buffer;
        return true;
    }

    private static bool TryEvalMySqlSleepFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SLEEP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "SLEEP");

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("SLEEP() espera o tempo em segundos.");

        var secondsValue = evalArg(0);
        if (IsNullish(secondsValue))
        {
            result = null;
            return true;
        }

        if (!TryConvertNumericToDouble(secondsValue!, out var seconds) || seconds < 0d)
        {
            result = null;
            return true;
        }

        // Avoid real delays; return the same success code as a completed sleep.
        result = 0;
        return true;
    }

    private static bool TryEvalMySqlCompressFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("COMPRESS" or "UNCOMPRESS" or "UNCOMPRESSED_LENGTH"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{name}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (name == "COMPRESS")
        {
            var input = value is byte[] bytes
                ? bytes
                : Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty);

            using var output = new MemoryStream();
            using (var deflate = new DeflateStream(output, CompressionLevel.Optimal, leaveOpen: true))
                deflate.Write(input, 0, input.Length);

            result = output.ToArray();
            return true;
        }

        if (value is not byte[] compressed)
        {
            result = null;
            return true;
        }

        try
        {
            using var input = new MemoryStream(compressed);
            using var deflate = new DeflateStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            deflate.CopyTo(output);
            var decompressed = output.ToArray();

            result = name == "UNCOMPRESSED_LENGTH"
                ? decompressed.LongLength
                : decompressed;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMySqlFormatBytesFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FORMAT_BYTES", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version < 80 || dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "FORMAT_BYTES");

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT_BYTES() espera um valor numerico.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!TryConvertNumericToDouble(value!, out var bytes) || bytes < 0d)
        {
            result = null;
            return true;
        }

        if (bytes < 1024d)
        {
            result = $"{Math.Truncate(bytes)} bytes";
            return true;
        }

        var units = new[] { "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
        var unitIndex = 0;
        var scaled = bytes;
        while (scaled >= 1024d && unitIndex < units.Length - 1)
        {
            scaled /= 1024d;
            unitIndex++;
        }

        result = $"{scaled:0.00} {units[unitIndex]}";
        return true;
    }

    private static bool TryEvalMySqlFormatPicoTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FORMAT_PICO_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version < 80)
            throw SqlUnsupported.ForDialect(dialect, "FORMAT_PICO_TIME");

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT_PICO_TIME() espera um valor numerico.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!TryConvertNumericToDouble(value!, out var pico) || pico < 0d)
        {
            result = null;
            return true;
        }

        var units = new[]
        {
            ("ps", 1000d),
            ("ns", 1000d),
            ("us", 1000d),
            ("ms", 1000d),
            ("s", 60d),
            ("min", 60d),
            ("h", 24d),
            ("d", double.PositiveInfinity)
        };

        var scaled = pico;
        var unit = "ps";
        foreach (var (candidate, factor) in units)
        {
            unit = candidate;
            if (scaled < factor)
                break;
            if (double.IsInfinity(factor))
                break;
            scaled /= factor;
        }

        result = $"{scaled:0.00} {unit}";
        return true;
    }

    private static bool TryEvalMySqlXmlFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("EXTRACTVALUE" or "UPDATEXML"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (name == "EXTRACTVALUE" && fn.Args.Count < 2)
            throw new InvalidOperationException("EXTRACTVALUE() espera xml e xpath.");

        if (name == "UPDATEXML" && fn.Args.Count < 3)
            throw new InvalidOperationException("UPDATEXML() espera xml, xpath e novo xml.");

        // XML helpers are not simulated: return null consistently.
        result = null;
        return true;
    }

    private static bool TryEvalMySqlCryptoFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("AES_ENCRYPT" or "AES_DECRYPT" or "DES_ENCRYPT" or "DES_DECRYPT" or "ENCODE" or "DECODE" or "ENCRYPT"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (name is "AES_ENCRYPT" or "AES_DECRYPT")
        {
            if (dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, name);
        }
        else if (dialect.Version >= 80)
        {
            throw SqlUnsupported.ForDialect(dialect, name);
        }

        if (name == "ENCRYPT")
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("ENCRYPT() espera texto.");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var saltValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() ?? string.Empty : string.Empty;
            var text = value?.ToString() ?? string.Empty;
            var payload = Encoding.UTF8.GetBytes(saltValue + text);
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(payload);
            var sb = new StringBuilder(hash.Length * 2);
            foreach (var b in hash)
                sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
            result = sb.ToString();
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{name}() espera payload e chave.");

        var payloadValue = evalArg(0);
        var keyValue = evalArg(1);
        if (IsNullish(payloadValue) || IsNullish(keyValue))
        {
            result = null;
            return true;
        }

        var keyText = keyValue?.ToString() ?? string.Empty;

        if (name is "AES_ENCRYPT" or "AES_DECRYPT")
        {
            var payload = payloadValue is byte[] bytes
                ? bytes
                : Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);
            var key = BuildXorKeyBytes(Encoding.UTF8.GetBytes(keyText), 16);
            var aes = Aes.Create();
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = key;
            aes.IV = new byte[aes.BlockSize / 8];

            if (name == "AES_ENCRYPT")
            {
                using var encryptor = aes.CreateEncryptor();
                result = encryptor.TransformFinalBlock(payload, 0, payload.Length);
                return true;
            }

            try
            {
                using var decryptor = aes.CreateDecryptor();
                var decrypted = decryptor.TransformFinalBlock(payload, 0, payload.Length);
                result = Encoding.UTF8.GetString(decrypted);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name is "DES_ENCRYPT" or "DES_DECRYPT")
        {
            var payload = payloadValue is byte[] bytes
                ? bytes
                : Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);
            var key = BuildXorKeyBytes(Encoding.UTF8.GetBytes(keyText), 8);
            using var des = DES.Create();
            des.Mode = CipherMode.ECB;
            des.Padding = PaddingMode.PKCS7;
            des.Key = key;
            des.IV = new byte[des.BlockSize / 8];

            if (name == "DES_ENCRYPT")
            {
                using var encryptor = des.CreateEncryptor();
                result = encryptor.TransformFinalBlock(payload, 0, payload.Length);
                return true;
            }

            try
            {
                using var decryptor = des.CreateDecryptor();
                var decrypted = decryptor.TransformFinalBlock(payload, 0, payload.Length);
                result = Encoding.UTF8.GetString(decrypted);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        var keyBytes = Encoding.UTF8.GetBytes(keyText);
        if (keyBytes.Length == 0)
        {
            result = null;
            return true;
        }

        if (payloadValue is not byte[] inputBytes)
            inputBytes = Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);

        var output = new byte[inputBytes.Length];
        for (var i = 0; i < inputBytes.Length; i++)
            output[i] = (byte)(inputBytes[i] ^ keyBytes[i % keyBytes.Length]);

        if (name == "DECODE")
        {
            result = Encoding.UTF8.GetString(output);
            return true;
        }

        result = output;
        return true;
    }

    private static byte[] BuildXorKeyBytes(byte[] key, int length)
    {
        var output = new byte[length];
        if (key.Length == 0)
            return output;

        for (var i = 0; i < key.Length; i++)
            output[i % length] ^= key[i];

        return output;
    }

    private static bool TryEvalMySqlDefaultFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        EvalRow row,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DEFAULT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 1)
            throw new InvalidOperationException("DEFAULT() espera um argumento.");

        var arg = fn.Args[0];
        string? qualifier = null;
        string? columnName = null;

        if (arg is ColumnExpr columnExpr)
        {
            qualifier = columnExpr.Qualifier;
            columnName = columnExpr.Name;
        }
        else if (arg is IdentifierExpr identifierExpr)
        {
            var name = identifierExpr.Name;
            var dot = name.IndexOf('.');
            if (dot > 0)
            {
                qualifier = name[..dot];
                columnName = name[(dot + 1)..];
            }
            else
            {
                columnName = name;
            }
        }
        else
        {
            var value = evalArg(0);
            result = IsNullish(value) ? null : value;
            return true;
        }

        if (string.IsNullOrWhiteSpace(columnName))
        {
            result = null;
            return true;
        }

        if (TryResolveDefaultValue(row, qualifier, columnName!, out var defaultValue))
        {
            result = defaultValue;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryResolveDefaultValue(
        EvalRow row,
        string? qualifier,
        string columnName,
        out object? defaultValue)
    {
        defaultValue = null;
        if (row.Sources.Count == 0)
            return false;

        if (!string.IsNullOrWhiteSpace(qualifier))
        {
            if (!row.Sources.TryGetValue(qualifier, out var source))
                return false;

            if (source.Physical is null)
                return false;

            if (!source.Physical.Columns.TryGetValue(columnName, out var column))
                return false;

            defaultValue = column.DefaultValue;
            return true;
        }

        foreach (var source in row.Sources.Values)
        {
            if (source.Physical is null)
                continue;

            if (source.Physical.Columns.TryGetValue(columnName, out var column))
            {
                defaultValue = column.DefaultValue;
                return true;
            }
        }

        return false;
    }

    private static bool TryEvalMySqlMemberOfFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MEMBER_OF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (dialect.Version < 80)
            throw SqlUnsupported.ForDialect(dialect, "MEMBER OF");

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MEMBER OF espera dois argumentos.");

        var candidateValue = evalArg(0);
        var jsonValue = evalArg(1);
        if (IsNullish(candidateValue) || IsNullish(jsonValue))
        {
            result = null;
            return true;
        }

        if (!TryParseJsonCandidate(candidateValue!, out var candidateElement)
            || !TryParseJsonElement(jsonValue!, out var jsonElement))
        {
            result = null;
            return true;
        }

        if (jsonElement.ValueKind != System.Text.Json.JsonValueKind.Array)
        {
            result = null;
            return true;
        }

        foreach (var item in jsonElement.EnumerateArray())
        {
            if (JsonElementEquals(item, candidateElement))
            {
                result = 1;
                return true;
            }
        }

        result = 0;
        return true;
    }

    private static bool TryEvalMySqlConvFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "CONV");

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("CONV() espera valor, base origem e base destino.");

        var value = evalArg(0);
        var fromBaseValue = evalArg(1);
        var toBaseValue = evalArg(2);
        if (IsNullish(value) || IsNullish(fromBaseValue) || IsNullish(toBaseValue))
        {
            result = null;
            return true;
        }

        if (!TryConvertNumericToInt64(fromBaseValue!, out var fromBase)
            || !TryConvertNumericToInt64(toBaseValue!, out var toBase))
        {
            result = null;
            return true;
        }

        var sourceBase = Math.Clamp((int)Math.Abs(fromBase), 2, 36);
        var targetBase = Math.Clamp((int)Math.Abs(toBase), 2, 36);
        var textValue = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrWhiteSpace(textValue))
        {
            result = null;
            return true;
        }

        if (!TryParseBaseN(textValue!.Trim(), sourceBase, out var parsed))
        {
            result = null;
            return true;
        }

        result = ConvertToBaseN(parsed, targetBase);
        return true;
    }

    private static bool TryEvalMySqlDayFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DAYNAME" or "DAYOFMONTH" or "DAYOFWEEK" or "DAYOFYEAR"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if ((name is "DAYOFMONTH" or "DAYOFYEAR") && dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value!, out var date))
        {
            result = null;
            return true;
        }

        result = name switch
        {
            "DAYNAME" => CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(date.DayOfWeek),
            "DAYOFMONTH" => date.Day,
            "DAYOFWEEK" => ((int)date.DayOfWeek + 1),
            "DAYOFYEAR" => date.DayOfYear,
            _ => null
        };
        return true;
    }

    private static bool TryEvalMySqlDatabaseFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DATABASE" or "SCHEMA" or "SESSION_USER" or "CURRENT_USER" or "LOCALTIME" or "LOCALTIMESTAMP" or "CONNECTION_ID"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84
            && name is "DATABASE" or "SCHEMA" or "CURRENT_USER" or "LOCALTIME")
        {
            throw SqlUnsupported.ForDialect(dialect, name);
        }

        if (fn.Args.Count != 0)
            throw new InvalidOperationException($"{name}() nao aceita argumentos.");

        result = name switch
        {
            "DATABASE" or "SCHEMA" => "DefaultSchema",
            "SESSION_USER" => "root@localhost",
            "CURRENT_USER" => "root@localhost",
            "LOCALTIME" => DateTime.Now,
            "LOCALTIMESTAMP" => DateTime.Now,
            "CONNECTION_ID" => 1L,
            _ => null
        };
        return true;
    }

    private static bool TryEvalMySqlStringMetadataFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CHARSET" or "COLLATION" or "COERCIBILITY"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (dialect.Version >= 84 && name is "CHARSET" or "COERCIBILITY")
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

        result = name switch
        {
            "CHARSET" => "utf8mb4",
            "COLLATION" => "utf8mb4_general_ci",
            "COERCIBILITY" => 0,
            _ => null
        };
        return true;
    }

    private static bool TryParseBaseN(string text, int radix, out long value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var negative = text[0] == '-';
        var start = negative ? 1 : 0;
        long resultValue = 0;

        for (var i = start; i < text.Length; i++)
        {
            var ch = char.ToUpperInvariant(text[i]);
            int digit;
            if (ch >= '0' && ch <= '9')
                digit = ch - '0';
            else if (ch >= 'A' && ch <= 'Z')
                digit = ch - 'A' + 10;
            else
                return false;

            if (digit >= radix)
                return false;

            resultValue = checked(resultValue * radix + digit);
        }

        value = negative ? -resultValue : resultValue;
        return true;
    }

    private static string ConvertToBaseN(long value, int radix)
    {
        if (value == 0)
            return "0";

        var negative = value < 0;
        var working = Math.Abs(value);
        var chars = new List<char>();
        const string digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        while (working > 0)
        {
            var rem = (int)(working % radix);
            chars.Add(digits[rem]);
            working /= radix;
        }

        if (negative)
            chars.Add('-');

        chars.Reverse();
        return new string(chars.ToArray());
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleCollationFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleContainerFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

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
        if (normalizedName is not ("BIGINT" or "DECIMAL" or "DOUBLE" or "FLOAT" or "INT" or "INTEGER" or "REAL" or "SMALLINT" or "VARCHAR"))
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
                "DECIMAL" => CoerceToDecimal(value!),
                "DOUBLE" or "FLOAT" or "REAL" => CoerceToDouble(value!),
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if ((fn.Name.Equals("TO_APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals("TO_APPROX_PERCENTILE", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsApproximateScalarFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleJsonTransformFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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
            return true;
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
            return true;
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleNlsFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
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
            "YEAR" or "YEARS" => TimeSpan.FromDays(365d * number),
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

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleApproxFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_COUNT_DISTINCT_AGG", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_COUNT_DISTINCT_DETAIL", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_MEDIAN", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_PERCENTILE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_PERCENTILE_AGG", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("APPROX_PERCENTILE_DETAIL", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TO_APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TO_APPROX_PERCENTILE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleBfilenameFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("BFILENAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BFILENAME() espera diretorio e nome do arquivo.");

        var dir = evalArg(0);
        var name = evalArg(1);
        if (IsNullish(dir) || IsNullish(name))
        {
            result = null;
            return true;
        }

        result = $"{dir}/{name}";
        return true;
    }

    private static bool TryEvalOracleHashFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("STANDARD_HASH" or "ORA_HASH"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.SupportsOracleHashFunction(name))
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

        var algorithm = name.Equals("ORA_HASH", StringComparison.OrdinalIgnoreCase)
            ? "MD5"
            : (fn.Args.Count > 1 ? evalArg(1)?.ToString() : "SHA1");

        var text = value?.ToString() ?? string.Empty;
        var bytes = Encoding.UTF8.GetBytes(text);

        var normalized = algorithm?.ToUpperInvariant() ?? "SHA1";
        byte[] hashBytes;
        using (var hasher = CreateHashAlgorithm(normalized))
        {
            if (hasher is null)
            {
                result = null;
                return true;
            }

            hashBytes = hasher.ComputeHash(bytes);
        }

        var hex = ToHexString(hashBytes);
        if (name.Equals("ORA_HASH", StringComparison.OrdinalIgnoreCase))
        {
            // Reduce to a stable int range for ORA_HASH usage.
            var hash = 0;
            foreach (var b in hashBytes)
                hash = unchecked((hash * 31) + b);
            result = Math.Abs(hash);
            return true;
        }

        result = hex;
        return true;
    }

    private static bool TryEvalOracleRawFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("RAWTOHEX" or "RAWTONHEX" or "REF" or "REFTOHEX"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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

        if (name == "REF")
        {
            result = value;
            return true;
        }

        var bytes = value switch
        {
            byte[] buffer => buffer,
            string text => Encoding.UTF8.GetBytes(text),
            _ => Encoding.UTF8.GetBytes(value!.ToString() ?? string.Empty)
        };

        var hex = ToHexString(bytes);
        result = hex;
        return true;
    }

    private static bool TryEvalMySqlRegexFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("REGEXP_INSTR" or "REGEXP_REPLACE" or "REGEXP_SUBSTR" or "REGEXP_LIKE"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var minVersion = 80;
        if (dialect.Version < minVersion)
            throw SqlUnsupported.ForDialect(dialect, name);

        if ((name is "REGEXP_LIKE" or "REGEXP_SUBSTR") && dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var pattern = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(pattern))
        {
            result = null;
            return true;
        }

        var position = 1;
        if (fn.Args.Count >= 3 && !IsNullish(evalArg(2)))
            position = Math.Max(1, Convert.ToInt32(evalArg(2)!.ToDec()));

        var occurrence = 1;
        if (fn.Args.Count >= 4 && !IsNullish(evalArg(3)))
            occurrence = Math.Max(1, Convert.ToInt32(evalArg(3)!.ToDec()));

        var returnOption = 0;
        if (fn.Args.Count >= 5 && !IsNullish(evalArg(4)))
            returnOption = Convert.ToInt32(evalArg(4)!.ToDec());

        var matchType = fn.Args.Count >= 6 ? evalArg(5)?.ToString() ?? string.Empty : string.Empty;
        var options = RegexOptions.CultureInvariant;
        if (dialect.RegexIsCaseInsensitive)
            options |= RegexOptions.IgnoreCase;

        if (!string.IsNullOrWhiteSpace(matchType))
        {
            if (matchType.Contains('c', StringComparison.OrdinalIgnoreCase))
                options &= ~RegexOptions.IgnoreCase;
            if (matchType.Contains('i', StringComparison.OrdinalIgnoreCase))
                options |= RegexOptions.IgnoreCase;
            if (matchType.Contains('m', StringComparison.OrdinalIgnoreCase))
                options |= RegexOptions.Multiline;
            if (matchType.Contains('n', StringComparison.OrdinalIgnoreCase))
                options |= RegexOptions.Singleline;
        }

        var startIndex = Math.Min(source.Length, Math.Max(0, position - 1));
        var scoped = source[startIndex..];

        try
        {
            if (name == "REGEXP_LIKE")
            {
                result = Regex.IsMatch(scoped, pattern, options) ? 1 : 0;
                return true;
            }

            if (name == "REGEXP_REPLACE")
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                if (fn.Args.Count >= 4 && !IsNullish(evalArg(3)) && occurrence > 0)
                {
                    var matches = Regex.Matches(scoped, pattern, options);
                    if (matches.Count == 0)
                    {
                        result = scoped;
                        return true;
                    }

                    var idx = Math.Min(occurrence - 1, matches.Count - 1);
                    var match = matches[idx];
                    result = string.Concat(
                        scoped.AsSpan(0, match.Index),
                        replacement,
                        scoped.AsSpan(match.Index + match.Length));
                    return true;
                }

                result = Regex.Replace(scoped, pattern, replacement, options);
                return true;
            }

            var matchesForInstr = Regex.Matches(scoped, pattern, options);
            if (matchesForInstr.Count == 0)
            {
                result = name == "REGEXP_SUBSTR" ? null : 0;
                return true;
            }

            var index = Math.Min(occurrence - 1, matchesForInstr.Count - 1);
            var match = matchesForInstr[index];

            if (name == "REGEXP_INSTR")
            {
                var positionValue = returnOption == 1
                    ? startIndex + match.Index + match.Length
                    : startIndex + match.Index + 1;
                result = positionValue;
                return true;
            }

            // REGEXP_SUBSTR
            result = match.Value;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOracleRegexFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("REGEXP_COUNT" or "REGEXP_INSTR" or "REGEXP_REPLACE" or "REGEXP_SUBSTR"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var pattern = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(pattern))
        {
            result = null;
            return true;
        }

        var start = 1;
        if (fn.Args.Count >= 3 && !IsNullish(evalArg(2)))
            start = Math.Max(1, Convert.ToInt32(evalArg(2)!.ToDec()));

        var startIndex = Math.Min(source.Length, Math.Max(0, start - 1));
        var options = RegexOptions.CultureInvariant;

        try
        {
            if (name == "REGEXP_COUNT")
            {
                var matches = Regex.Matches(source[startIndex..], pattern, options);
                result = matches.Count;
                return true;
            }

            if (name == "REGEXP_REPLACE")
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                result = Regex.Replace(source, pattern, replacement, options);
                return true;
            }

            var matchesForInstr = Regex.Matches(source[startIndex..], pattern, options);
            if (matchesForInstr.Count == 0)
            {
                result = 0;
                return true;
            }

            var occurrence = 1;
            if (fn.Args.Count >= 4 && !IsNullish(evalArg(3)))
                occurrence = Math.Max(1, Convert.ToInt32(evalArg(3)!.ToDec()));

            var idx = Math.Min(occurrence - 1, matchesForInstr.Count - 1);
            var match = matchesForInstr[idx];

            if (name == "REGEXP_INSTR")
            {
                result = startIndex + match.Index + 1;
                return true;
            }

            // REGEXP_SUBSTR
            result = match.Value;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOracleRemainderFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("REMAINDER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("REMAINDER() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        var leftValue = Convert.ToDouble(left, CultureInfo.InvariantCulture);
        var rightValue = Convert.ToDouble(right, CultureInfo.InvariantCulture);
        if (rightValue == 0)
        {
            result = null;
            return true;
        }

        result = Math.IEEERemainder(leftValue, rightValue);
        return true;
    }

    private static bool TryEvalOracleRowIdFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("ROWIDTOCHAR" or "ROWTONCHAR"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleRowIdFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        result = IsNullish(value) ? null : value?.ToString();
        return true;
    }

    private static bool TryEvalOracleSessionTimeZoneFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("SESSIONTIMEZONE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        var offset = DateTimeOffset.Now.Offset;
        result = $"{(offset < TimeSpan.Zero ? "-" : "+")}{offset:hh\\:mm}";
        return true;
    }

    private static bool TryEvalOracleSysFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SYS_GUID" or "SYS_EXTRACT_UTC" or "SYS_CONTEXT" or "SYS_CONNECT_BY_PATH" or "SYS_DBURIGEN"
            or "SYS_OP_ZONE_ID" or "SYS_TYPEID" or "SYS_XMLAGG" or "SYS_XMLGEN"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleSysFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        switch (name)
        {
            case "SYS_GUID":
                result = Guid.NewGuid().ToString("D");
                return true;
            case "SYS_EXTRACT_UTC":
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
                if (value is DateTimeOffset dto)
                {
                    result = dto.UtcDateTime;
                    return true;
                }
                if (TryCoerceDateTime(value!, out var dt))
                {
                    result = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
                    return true;
                }
                result = null;
                return true;
            case "SYS_CONTEXT":
                if (fn.Args.Count < 2)
                {
                    result = null;
                    return true;
                }
                var namespaceValue = evalArg(0)?.ToString();
                var parameterValue = evalArg(1)?.ToString();
                if (string.Equals(namespaceValue, "USERENV", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(parameterValue, "CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase))
                {
                    result = "SYS";
                    return true;
                }
                result = null;
                return true;
            default:
                result = null;
                return true;
        }
    }

    private static bool TryEvalOracleToCharFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("TO_BINARY_DOUBLE" or "TO_BINARY_FLOAT" or "TO_BLOB" or "TO_CHAR" or "TO_CLOB" or "TO_DATE"
            or "TO_DSINTERVAL" or "TO_LOB" or "TO_MULTI_BYTE" or "TO_NCHAR" or "TO_NCLOB" or "TO_NUMBER"
            or "TO_SINGLE_BYTE" or "TO_TIMESTAMP" or "TO_TIMESTAMP_TZ" or "TO_YMINTERVAL"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if ((name.Equals("TO_BINARY_DOUBLE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_BINARY_FLOAT", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_BLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_CLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_DSINTERVAL", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_LOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_MULTI_BYTE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_NCHAR", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_NCLOB", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_SINGLE_BYTE", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_TIMESTAMP_TZ", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TO_YMINTERVAL", StringComparison.OrdinalIgnoreCase))
            && !dialect.SupportsOracleSpecificConversionFunction(name))
        {
            throw SqlUnsupported.ForDialect(dialect, name);
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

        switch (name)
        {
            case "TO_BINARY_DOUBLE":
                result = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_BINARY_FLOAT":
                result = Convert.ToSingle(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_NUMBER":
                if (value is string numberText)
                {
                    var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                    if (TryParseOracleNumber(numberText, mask, out var parsedNumber))
                    {
                        result = parsedNumber;
                        return true;
                    }
                }
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case "TO_CHAR":
                if (value is DateTime dateValue)
                {
                    if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                    {
                        var netFormat = NormalizeOracleFormatMask(fmt, out _);
                        result = dateValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        result = dateValue.ToString(CultureInfo.InvariantCulture);
                    }
                    return true;
                }
                if (value is DateTimeOffset dtoValue)
                {
                    if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                    {
                        var netFormat = NormalizeOracleFormatMask(fmt, out _);
                        result = dtoValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        result = dtoValue.ToString(CultureInfo.InvariantCulture);
                    }
                    return true;
                }
                if (IsNumericValue(value))
                {
                    var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                    if (!string.IsNullOrWhiteSpace(mask))
                    {
                        result = FormatOracleNumber(value!, mask!);
                        return true;
                    }
                }
                result = value!.ToString();
                return true;
            case "TO_DATE":
            case "TO_TIMESTAMP":
            case "TO_TIMESTAMP_TZ":
                if (value is DateTime dt)
                {
                    result = dt;
                    return true;
                }
                var textValue = value!.ToString() ?? string.Empty;
                var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                if (name == "TO_TIMESTAMP_TZ")
                {
                    if (TryParseOracleDateTimeOffset(textValue, maskValue, out var parsedOffset))
                    {
                        result = parsedOffset;
                        return true;
                    }
                    result = null;
                    return true;
                }

                if (TryParseOracleDateTime(textValue, maskValue, out var parsed))
                {
                    result = parsed;
                    return true;
                }
                result = null;
                return true;
            case "TO_DSINTERVAL":
            case "TO_YMINTERVAL":
                if (value is TimeSpan span)
                {
                    result = span;
                    return true;
                }
                if (TimeSpan.TryParse(value!.ToString(), CultureInfo.InvariantCulture, out var parsedSpan))
                {
                    result = parsedSpan;
                    return true;
                }
                result = null;
                return true;
            case "TO_BLOB":
            case "TO_CLOB":
            case "TO_NCLOB":
            case "TO_LOB":
            case "TO_MULTI_BYTE":
            case "TO_SINGLE_BYTE":
            case "TO_NCHAR":
                result = value?.ToString();
                return true;
            default:
                result = value;
                return true;
        }
    }

    private static bool TryParseOracleDateTime(string text, string? mask, out DateTime result)
    {
        if (string.IsNullOrWhiteSpace(mask))
        {
            return DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        var netFormat = NormalizeOracleFormatMask(mask, out var hasTz);
        if (string.IsNullOrWhiteSpace(netFormat))
        {
            return DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        if (hasTz && DateTimeOffset.TryParseExact(text, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dto))
        {
            result = dto.DateTime;
            return true;
        }

        return DateTime.TryParseExact(text, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
    }

    private static bool TryParseOracleDateTimeOffset(string text, string? mask, out DateTimeOffset result)
    {
        if (string.IsNullOrWhiteSpace(mask))
        {
            return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        var netFormat = NormalizeOracleFormatMask(mask, out var _);
        if (string.IsNullOrWhiteSpace(netFormat))
        {
            return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        return DateTimeOffset.TryParseExact(text, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);
    }

    private static string? NormalizeOracleFormatMask(string? mask, out bool hasTimeZone)
    {
        hasTimeZone = false;
        if (string.IsNullOrWhiteSpace(mask))
            return null;

        var upper = ReplaceInsensitive(mask!.Trim().ToUpperInvariant(), "FM", string.Empty);
        hasTimeZone = upper.Contains("TZH", StringComparison.OrdinalIgnoreCase)
            || upper.Contains("TZM", StringComparison.OrdinalIgnoreCase)
            || upper.Contains("TZR", StringComparison.OrdinalIgnoreCase)
            || upper.Contains("TZD", StringComparison.OrdinalIgnoreCase);

        var net = upper;

        net = ReplaceInsensitive(net, "TZH:TZM", "zzz");
        net = ReplaceInsensitive(net, "TZH", "zz");
        net = ReplaceInsensitive(net, "TZM", "mm");
        net = ReplaceInsensitive(net, "RRRR", "yyyy");
        net = ReplaceInsensitive(net, "YYYY", "yyyy");
        net = ReplaceInsensitive(net, "YYY", "yyy");
        net = ReplaceInsensitive(net, "YY", "yy");
        net = ReplaceInsensitive(net, "Y", "y");
        net = ReplaceInsensitive(net, "MONTH", "MMMM");
        net = ReplaceInsensitive(net, "MON", "MMM");
        net = ReplaceInsensitive(net, "MM", "MM");
        net = ReplaceInsensitive(net, "DD", "dd");
        net = ReplaceInsensitive(net, "HH24", "HH");
        net = ReplaceInsensitive(net, "HH12", "hh");
        net = ReplaceInsensitive(net, "HH", "hh");
        net = ReplaceInsensitive(net, "MI", "mm");
        net = ReplaceInsensitive(net, "SS", "ss");
        net = ReplaceInsensitive(net, "FF", "fffffff");

        net = net.Replace("\"", "'");

        return net;
    }

    private static bool IsNumericValue(object? value)
        => value is sbyte or byte or short or ushort or int or uint or long or ulong
            or float or double or decimal;

    private static bool TryParseOracleNumber(string text, string? mask, out decimal result)
    {
        result = 0m;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        if (string.IsNullOrWhiteSpace(mask))
            return decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out result);

        var normalizedMask = mask!.ToUpperInvariant();
        var trimmed = text.Trim();
        var isNegative = false;
        if (trimmed.StartsWith("(", StringComparison.Ordinal) && trimmed.EndsWith(")", StringComparison.Ordinal))
        {
            isNegative = true;
            trimmed = trimmed[1..^1];
        }

        if (trimmed.StartsWith("-", StringComparison.Ordinal))
        {
            isNegative = true;
            trimmed = trimmed[1..];
        }

        var decimalSeparator = normalizedMask.Contains('D') ? '.' : '.';
        var groupSeparator = normalizedMask.Contains('G') ? ',' : ',';

        var builder = new StringBuilder(trimmed.Length);
        foreach (var ch in trimmed)
        {
            if (char.IsDigit(ch) || ch == decimalSeparator)
            {
                builder.Append(ch == decimalSeparator ? '.' : ch);
                continue;
            }

            if (ch == groupSeparator)
                continue;
        }

        var cleaned = builder.ToString();
        if (!decimal.TryParse(cleaned, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out result))
            return false;

        if (isNegative)
            result = -result;
        return true;
    }

    private static string FormatOracleNumber(object value, string mask)
    {
        if (!TryConvertNumericToDecimal(value, out var number))
            return value.ToString() ?? string.Empty;

        var normalizedMask = ReplaceInsensitive(mask.ToUpperInvariant(), "FM", string.Empty);
        var formatBuilder = new StringBuilder(normalizedMask.Length);
        foreach (var ch in normalizedMask)
        {
            formatBuilder.Append(ch switch
            {
                '9' => '#',
                '0' => '0',
                'D' => '.',
                'G' => ',',
                _ => ch
            });
        }

        var format = formatBuilder.ToString();
        try
        {
            return number.ToString(format, CultureInfo.InvariantCulture);
        }
        catch
        {
            return number.ToString(CultureInfo.InvariantCulture);
        }
    }

    private static string FormatPostgreSqlNumber(object value, string mask)
    {
        if (!TryConvertNumericToDecimal(value, out var number))
            return value.ToString() ?? string.Empty;

        var normalizedMask = ReplaceInsensitive(mask.ToUpperInvariant(), "FM", string.Empty);
        var decimalIndex = normalizedMask.IndexOf('D');
        if (decimalIndex < 0)
            decimalIndex = normalizedMask.IndexOf('.');

        var integerMask = decimalIndex >= 0 ? normalizedMask[..decimalIndex] : normalizedMask;
        var fractionalMask = decimalIndex >= 0 ? normalizedMask[(decimalIndex + 1)..] : string.Empty;

        var fractionalDigits = fractionalMask.Count(ch => ch is '9' or '0');
        var rounded = Math.Round(number, fractionalDigits, MidpointRounding.AwayFromZero);
        var absText = Math.Abs(rounded).ToString($"F{fractionalDigits}", CultureInfo.InvariantCulture);
        var absParts = absText.Split('.');

        var integerDigits = absParts[0];
        var fractionalDigitsText = absParts.Length > 1 ? absParts[1] : string.Empty;

        var integerPlaceholders = integerMask.Count(ch => ch is '9' or '0');
        if (integerDigits.Length < integerPlaceholders)
        {
            var padded = integerDigits.PadLeft(integerPlaceholders, ' ');
            var chars = padded.ToCharArray();
            var digitIndex = chars.Length - integerDigits.Length;
            for (var i = 0; i < integerMask.Length && i < chars.Length; i++)
            {
                if (integerMask[i] == '0' && i < digitIndex)
                    chars[i] = '0';
            }

            integerDigits = new string(chars);
        }

        if (fractionalDigits > 0 && fractionalDigitsText.Length < fractionalDigits)
            fractionalDigitsText = fractionalDigitsText.PadRight(fractionalDigits, '0');

        var sign = rounded < 0m ? "-" : " ";
        return fractionalDigits > 0
            ? $"{sign}{integerDigits}.{fractionalDigitsText}"
            : $"{sign}{integerDigits}";
    }

    private static HashAlgorithm? CreateHashAlgorithm(string algorithm)
    {
        try
        {
            return algorithm switch
            {
                "SHA256" => SHA256.Create(),
                "SHA384" => SHA384.Create(),
                "SHA512" => SHA512.Create(),
                "MD5" => MD5.Create(),
                _ => SHA1.Create()
            };
        }
        catch
        {
            return null;
        }
    }

    private static string ToHexString(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("X2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    private static string ReplaceInsensitive(string value, string oldValue, string newValue)
    {
        if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(oldValue))
            return value;

        var sb = new StringBuilder(value.Length);
        var index = 0;
        while (true)
        {
            var found = value.IndexOf(oldValue, index, StringComparison.OrdinalIgnoreCase);
            if (found < 0)
            {
                sb.Append(value, index, value.Length - index);
                break;
            }

            sb.Append(value, index, found - index);
            sb.Append(newValue);
            index = found + oldValue.Length;
        }

        return sb.ToString();
    }

    private bool TryEvalPostgresSystemFunctions(
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
        if (name is "CURRENT_DATABASE" or "CURRENT_CATALOG")
        {
            result = "postgres";
            return true;
        }

        if (name is "CURRENT_SCHEMA")
        {
            result = "public";
            return true;
        }

        if (name is "CURRENT_USER" or "CURRENT_ROLE")
        {
            result = "postgres";
            return true;
        }

        if (name is "VERSION")
        {
            result = $"PostgreSQL {dialect.Version}";
            return true;
        }

        if (name is "CURRENT_SCHEMAS")
        {
            result = new[] { "public" };
            return true;
        }

        if (name is "CURRENT_SETTING")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var settingName = evalArg(0)?.ToString();
            if (string.IsNullOrWhiteSpace(settingName))
            {
                result = null;
                return true;
            }

            result = settingName!.Trim().ToLowerInvariant() switch
            {
                "application_name" => "DbSqlLikeMem",
                "search_path" => "\"$user\", public",
                "server_version" => dialect.Version.ToString(CultureInfo.InvariantCulture),
                "server_version_num" => (dialect.Version * 10000).ToString(CultureInfo.InvariantCulture),
                _ => null
            };
            return true;
        }

        if (name is "CURRENT_QUERY")
        {
            result = _cnn.GetCurrentQueryText();
            return true;
        }

        if (name is "CLOCK_TIMESTAMP" or "STATEMENT_TIMESTAMP" or "TRANSACTION_TIMESTAMP")
        {
            result = DateTime.Now;
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalPostgresDateFunctions(
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
        if (name is "DATE_TRUNC")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("DATE_TRUNC() espera unidade e data.");

            var unit = evalArg(0)?.ToString() ?? string.Empty;
            var value = evalArg(1);
            if (IsNullish(value) || string.IsNullOrWhiteSpace(unit) || !TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = unit.Trim().ToLowerInvariant() switch
            {
                "year" => new DateTime(dateTime.Year, 1, 1),
                "month" => new DateTime(dateTime.Year, dateTime.Month, 1),
                "day" => dateTime.Date,
                "hour" => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0),
                "minute" => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0),
                "second" => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second),
                _ => dateTime
            };
            return true;
        }

        if (name is "DATE_PART")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("DATE_PART() espera unidade e data.");

            var unit = evalArg(0)?.ToString() ?? string.Empty;
            var value = evalArg(1);
            if (IsNullish(value) || string.IsNullOrWhiteSpace(unit) || !TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = unit.Trim().ToLowerInvariant() switch
            {
                "year" => (double)dateTime.Year,
                "month" => (double)dateTime.Month,
                "day" => (double)dateTime.Day,
                "hour" => (double)dateTime.Hour,
                "minute" => (double)dateTime.Minute,
                "second" => (double)dateTime.Second,
                _ => null
            };
            return true;
        }

        if (name is "AGE")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var left = evalArg(0);
            if (IsNullish(left) || !TryCoerceDateTime(left, out var leftDate))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count == 1)
            {
                result = DateTime.Now - leftDate;
                return true;
            }

            var right = evalArg(1);
            if (IsNullish(right) || !TryCoerceDateTime(right, out var rightDate))
            {
                result = null;
                return true;
            }

            result = leftDate - rightDate;
            return true;
        }

        if (name is "MAKE_INTERVAL")
        {
            var years = fn.Args.Count > 0 ? Convert.ToInt32(evalArg(0).ToDec()) : 0;
            var months = fn.Args.Count > 1 ? Convert.ToInt32(evalArg(1).ToDec()) : 0;
            var weeks = fn.Args.Count > 2 ? Convert.ToInt32(evalArg(2).ToDec()) : 0;
            var days = fn.Args.Count > 3 ? Convert.ToInt32(evalArg(3).ToDec()) : 0;
            var hours = fn.Args.Count > 4 ? Convert.ToInt32(evalArg(4).ToDec()) : 0;
            var mins = fn.Args.Count > 5 ? Convert.ToInt32(evalArg(5).ToDec()) : 0;
            var secs = fn.Args.Count > 6 ? Convert.ToDouble(evalArg(6), CultureInfo.InvariantCulture) : 0d;

            result = TimeSpan.FromDays((years * 365) + (months * 30) + (weeks * 7) + days)
                .Add(TimeSpan.FromHours(hours))
                .Add(TimeSpan.FromMinutes(mins))
                .Add(TimeSpan.FromSeconds(secs));
            return true;
        }

        if (name is "MAKE_DATE")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("MAKE_DATE() espera ano, mes e dia.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            result = new DateTime(year, month, day);
            return true;
        }

        if (name is "MAKE_TIME")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("MAKE_TIME() espera hora, minuto e segundo.");

            var hour = Convert.ToInt32(evalArg(0).ToDec());
            var minute = Convert.ToInt32(evalArg(1).ToDec());
            var second = Convert.ToInt32(evalArg(2).ToDec());
            result = new TimeSpan(hour, minute, second);
            return true;
        }

        if (name is "MAKE_TIMESTAMP")
        {
            if (fn.Args.Count < 6)
                throw new InvalidOperationException("MAKE_TIMESTAMP() espera data e hora.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            var hour = Convert.ToInt32(evalArg(3).ToDec());
            var minute = Convert.ToInt32(evalArg(4).ToDec());
            var second = Convert.ToInt32(evalArg(5).ToDec());
            result = new DateTime(year, month, day, hour, minute, second);
            return true;
        }

        if (name is "MAKE_TIMESTAMPTZ")
        {
            if (fn.Args.Count < 6)
                throw new InvalidOperationException("MAKE_TIMESTAMPTZ() espera data e hora.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            var hour = Convert.ToInt32(evalArg(3).ToDec());
            var minute = Convert.ToInt32(evalArg(4).ToDec());
            var second = Convert.ToInt32(evalArg(5).ToDec());
            result = new DateTimeOffset(year, month, day, hour, minute, second, DateTimeOffset.Now.Offset);
            return true;
        }

        if (name is "TO_DATE")
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

            if (value is DateTime dateValue)
            {
                result = dateValue.Date;
                return true;
            }

            var textValue = value?.ToString() ?? string.Empty;
            var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (TryParseOracleDateTime(textValue, maskValue, out var parsed))
            {
                result = parsed.Date;
                return true;
            }

            if (DateTime.TryParse(textValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var fallbackParsed))
            {
                result = fallbackParsed.Date;
                return true;
            }

            result = null;
            return true;
        }

        if (name is "TO_CHAR")
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

            if (value is DateTime dateValue)
            {
                if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                {
                    var netFormat = NormalizeOracleFormatMask(fmt, out _);
                    result = dateValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                }
                else
                {
                    result = dateValue.ToString(CultureInfo.InvariantCulture);
                }

                return true;
            }

            if (value is DateTimeOffset dtoValue)
            {
                if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                {
                    var netFormat = NormalizeOracleFormatMask(fmt, out _);
                    result = dtoValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                }
                else
                {
                    result = dtoValue.ToString(CultureInfo.InvariantCulture);
                }

                return true;
            }

            if (IsNumericValue(value))
            {
                var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                if (!string.IsNullOrWhiteSpace(mask))
                {
                    result = FormatPostgreSqlNumber(value!, mask!);
                    return true;
                }
            }

            result = value!.ToString();
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalPostgresScalarUtilityFunctions(
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
        if (name is "NUM_NULLS")
        {
            result = Enumerable.Range(0, fn.Args.Count).Count(i => IsNullish(evalArg(i)));
            return true;
        }

        if (name is "NUM_NONNULLS")
        {
            result = Enumerable.Range(0, fn.Args.Count).Count(i => !IsNullish(evalArg(i)));
            return true;
        }

        if (name is "LCM")
        {
            if (fn.Args.Count < 2)
            {
                result = null;
                return true;
            }

            var leftValue = evalArg(0);
            var rightValue = evalArg(1);
            if (IsNullish(leftValue) || IsNullish(rightValue))
            {
                result = null;
                return true;
            }

            var left = Math.Abs(Convert.ToInt64(leftValue.ToDec(), CultureInfo.InvariantCulture));
            var right = Math.Abs(Convert.ToInt64(rightValue.ToDec(), CultureInfo.InvariantCulture));
            if (left == 0 || right == 0)
            {
                result = 0L;
                return true;
            }

            result = checked((left / ComputeGreatestCommonDivisor(left, right)) * right);
            return true;
        }

        if (name is "MIN_SCALE")
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

            result = GetMinimumNumericScale(value!);
            return true;
        }

        if (name is "PARSE_IDENT")
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

            var text = value?.ToString() ?? string.Empty;
            if (!TryParsePostgresIdentifierParts(text, out var parts))
            {
                result = null;
                return true;
            }

            result = parts.ToArray();
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalPostgresTextFunctions(
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
        if (name is "BTRIM")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = text.Trim();
            return true;
        }

        if (name is "INITCAP")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(text.ToLowerInvariant());
            return true;
        }

        if (name is "CHR")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var code = Convert.ToInt32(value.ToDec(), CultureInfo.InvariantCulture);
                if (code < 0 || code > 0x10FFFF)
                {
                    result = null;
                    return true;
                }

                result = char.ConvertFromUtf32(code);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name is "SPLIT_PART")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("SPLIT_PART() espera texto, separador e indice.");

            var text = evalArg(0)?.ToString() ?? string.Empty;
            var delimiter = evalArg(1)?.ToString() ?? string.Empty;
            var index = Convert.ToInt32(evalArg(2).ToDec());
            if (index <= 0)
            {
                result = string.Empty;
                return true;
            }

            var parts = text.Split([delimiter], StringSplitOptions.None);
            result = index <= parts.Length ? parts[index - 1] : string.Empty;
            return true;
        }

        if (name is "STRING_TO_ARRAY")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("STRING_TO_ARRAY() espera texto e separador.");

            var text = evalArg(0)?.ToString() ?? string.Empty;
            var delimiter = evalArg(1)?.ToString() ?? string.Empty;
            result = text.Split([delimiter], StringSplitOptions.None);
            return true;
        }

        if (name is "QUOTE_LITERAL")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = $"'{text.Replace("'", "''")}'";
            return true;
        }

        if (name is "QUOTE_IDENT")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = $"\"{text.Replace("\"", "\"\"")}\"";
            return true;
        }

        if (name is "TO_HEX")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = number.ToString("x", CultureInfo.InvariantCulture);
            return true;
        }

        if (name is "TRANSLATE")
        {
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

        if (name is "STARTS_WITH")
        {
            if (dialect.Version < 11)
                throw SqlUnsupported.ForDialect(dialect, "STARTS_WITH");

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("STARTS_WITH() espera texto e prefixo.");

            var source = evalArg(0)?.ToString();
            var prefix = evalArg(1)?.ToString();
            if (source is null || prefix is null)
            {
                result = null;
                return true;
            }

            result = source.StartsWith(prefix, StringComparison.Ordinal);
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalPostgresNetworkFunctions(
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
        if (name is not ("HOST" or "HOSTMASK" or "INET_SAME_FAMILY" or "MASKLEN" or "NETMASK" or "NETWORK"))
        {
            result = null;
            return false;
        }

        if (name is "INET_SAME_FAMILY")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("INET_SAME_FAMILY() espera dois enderecos.");

            var left = evalArg(0);
            var right = evalArg(1);
            if (IsNullish(left) || IsNullish(right))
            {
                result = null;
                return true;
            }

            if (!TryParsePostgresInetValue(left, out var leftAddress, out _)
                || !TryParsePostgresInetValue(right, out var rightAddress, out _))
            {
                result = null;
                return true;
            }

            result = leftAddress.AddressFamily == rightAddress.AddressFamily;
            return true;
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

        if (!TryParsePostgresInetValue(value, out var address, out var prefixLength))
        {
            result = null;
            return true;
        }

        var byteLength = address.GetAddressBytes().Length;
        var maskBytes = BuildPrefixMaskBytes(byteLength, prefixLength);

        result = name switch
        {
            "HOST" => address.ToString(),
            "MASKLEN" => prefixLength,
            "NETMASK" => new System.Net.IPAddress(maskBytes).ToString(),
            "HOSTMASK" => new System.Net.IPAddress(maskBytes.Select(static b => (byte)~b).ToArray()).ToString(),
            "NETWORK" => $"{new System.Net.IPAddress(ApplyNetworkMask(address.GetAddressBytes(), maskBytes))}/{prefixLength}",
            _ => null
        };
        return true;
    }

    private static bool TryEvalPostgresUnicodeFunctions(
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
        if (name is not ("NORMALIZE" or "TO_ASCII"))
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

        var text = value?.ToString() ?? string.Empty;
        if (name is "NORMALIZE")
        {
            var formName = fn.Args.Count > 1
                ? (evalArg(1)?.ToString() ?? string.Empty).Trim().ToUpperInvariant()
                : "NFC";
            var form = formName switch
            {
                "" or "NFC" => NormalizationForm.FormC,
                "NFD" => NormalizationForm.FormD,
                "NFKC" => NormalizationForm.FormKC,
                "NFKD" => NormalizationForm.FormKD,
                _ => NormalizationForm.FormC
            };

            result = text.Normalize(form);
            return true;
        }

        result = ConvertToAscii(text);
        return true;
    }

    private static bool TryEvalPostgresRegexFunctions(
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
        if (name is not ("REGEXP_COUNT" or "REGEXP_INSTR" or "REGEXP_LIKE" or "REGEXP_MATCH" or "REGEXP_REPLACE" or "REGEXP_SPLIT_TO_ARRAY" or "REGEXP_SUBSTR"))
        {
            result = null;
            return false;
        }

        var minVersion = name switch
        {
            "REGEXP_MATCH" => 10,
            "REGEXP_REPLACE" or "REGEXP_SPLIT_TO_ARRAY" => 9,
            _ => 15
        };
        if (dialect.Version < minVersion)
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString();
        var pattern = evalArg(1)?.ToString();
        if (source is null || pattern is null)
        {
            result = null;
            return true;
        }

        try
        {
            var regexOptions = RegexOptions.CultureInvariant;
            var flags = TryGetPostgresRegexFlags(fn, evalArg, out var start, out var occurrence);
            if (HasRegexFlag(flags, 'i'))
                regexOptions |= RegexOptions.IgnoreCase;
            if (HasRegexFlag(flags, 'm'))
                regexOptions |= RegexOptions.Multiline;
            if (HasRegexFlag(flags, 'n')
                || HasRegexFlag(flags, 's'))
            {
                regexOptions |= RegexOptions.Singleline;
            }

            var startIndex = Math.Min(source.Length, Math.Max(0, start - 1));
            var segment = source[startIndex..];
            var regex = new Regex(pattern, regexOptions);

            if (name == "REGEXP_REPLACE")
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                var replaceAll = HasRegexFlag(flags, 'g');
                result = regex.Replace(source, replacement, replaceAll ? int.MaxValue : 1, 0);
                return true;
            }

            if (name == "REGEXP_SPLIT_TO_ARRAY")
            {
                result = regex.Split(source);
                return true;
            }

            var matches = regex.Matches(segment);
            if (name == "REGEXP_COUNT")
            {
                result = matches.Count;
                return true;
            }

            if (name == "REGEXP_LIKE")
            {
                result = matches.Count > 0;
                return true;
            }

            if (matches.Count == 0)
            {
                result = name == "REGEXP_INSTR" ? 0 : null;
                return true;
            }

            var index = Math.Min(Math.Max(1, occurrence) - 1, matches.Count - 1);
            var match = matches[index];

            if (name == "REGEXP_INSTR")
            {
                result = startIndex + match.Index + 1;
                return true;
            }

            if (name == "REGEXP_SUBSTR")
            {
                result = match.Value;
                return true;
            }

            var captureValues = match.Groups.Count > 1
                ? match.Groups.Cast<Group>().Skip(1).Select(static g => g.Value).ToArray()
                : [match.Value];
            result = captureValues;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static string TryGetPostgresRegexFlags(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out int start,
        out int occurrence)
    {
        start = 1;
        occurrence = 1;

        if (fn.Args.Count < 3 || IsNullish(evalArg(2)))
            return string.Empty;

        var third = evalArg(2);
        if (third is string flagText && !int.TryParse(flagText, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            return flagText;

        start = Math.Max(1, Convert.ToInt32(third!.ToDec(), CultureInfo.InvariantCulture));

        if (fn.Args.Count >= 4 && !IsNullish(evalArg(3)))
        {
            var fourth = evalArg(3);
            if (fourth is string fourthFlags && !int.TryParse(fourthFlags, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
                return fourthFlags;

            occurrence = Math.Max(1, Convert.ToInt32(fourth!.ToDec(), CultureInfo.InvariantCulture));
        }

        if (fn.Args.Count >= 5 && !IsNullish(evalArg(4)))
            return evalArg(4)?.ToString() ?? string.Empty;

        return string.Empty;
    }

    private static bool HasRegexFlag(string flags, char flag)
    {
        if (string.IsNullOrEmpty(flags))
            return false;

        var upperFlag = char.ToUpperInvariant(flag);
        foreach (var current in flags)
        {
            if (char.ToUpperInvariant(current) == upperFlag)
                return true;
        }

        return false;
    }

    private static bool TryEvalPostgresArrayFunctions(
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
        if (name is "ARRAY_TO_STRING")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_TO_STRING() espera array e separador.");

            var value = evalArg(0);
            var separator = evalArg(1)?.ToString() ?? string.Empty;
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is IEnumerable enumerable)
            {
                var items = new List<string>();
                foreach (var item in enumerable)
                    items.Add(item?.ToString() ?? string.Empty);
                result = string.Join(separator, items);
                return true;
            }
        }

        if (name is "ARRAY_LENGTH" or "ARRAY_UPPER" or "ARRAY_LOWER")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            if (list.Count == 0)
            {
                result = null;
                return true;
            }

            result = name switch
            {
                "ARRAY_LENGTH" => list.Count,
                "ARRAY_UPPER" => list.Count,
                _ => 1
            };
            return true;
        }

        if (name is "ARRAY_DIMS" or "ARRAY_NDIMS")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            if (list.Count == 0)
            {
                result = null;
                return true;
            }

            result = name == "ARRAY_DIMS"
                ? $"[1:{list.Count}]"
                : 1;
            return true;
        }

        if (name is "ARRAY_POSITION")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_POSITION() espera array e valor.");

            var value = evalArg(0);
            var target = evalArg(1);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];
            var index = list.FindIndex(item => Equals(item, target));
            result = index >= 0 ? index + 1 : (object?)null;
            return true;
        }

        if (name is "ARRAY_POSITIONS")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_POSITIONS() espera array e valor.");

            var value = evalArg(0);
            var target = evalArg(1);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            var matches = new List<object?>();
            for (var i = 0; i < list.Count; i++)
            {
                if (Equals(list[i], target))
                    matches.Add(i + 1);
            }

            result = matches.ToArray();
            return true;
        }

        if (name is "ARRAY_TO_JSON")
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("ARRAY_TO_JSON() espera array.");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            var writeIndented = fn.Args.Count > 1 && Convert.ToBoolean(evalArg(1), CultureInfo.InvariantCulture);
            var options = writeIndented
                ? new System.Text.Json.JsonSerializerOptions { WriteIndented = true }
                : null;
            result = System.Text.Json.JsonSerializer.Serialize(list, options);
            return true;
        }

        if (name is "ARRAY_APPEND" or "ARRAY_PREPEND" or "ARRAY_CAT" or "ARRAY_REMOVE" or "ARRAY_REPLACE")
        {
            var list = new List<object?>();
            var left = name is "ARRAY_PREPEND" ? evalArg(1) : evalArg(0);
            if (!IsNullish(left) && left is IEnumerable enumerable)
                list.AddRange(enumerable.Cast<object?>());

            if (name is "ARRAY_CAT")
            {
                var right = evalArg(1);
                if (!IsNullish(right) && right is IEnumerable rightEnum)
                    list.AddRange(rightEnum.Cast<object?>());
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_APPEND")
            {
                list.Add(evalArg(1));
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_PREPEND")
            {
                list.Insert(0, evalArg(0));
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_REMOVE")
            {
                var target = evalArg(1);
                list = list.Where(item => !Equals(item, target)).ToList();
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_REPLACE")
            {
                var target = evalArg(1);
                var replacement = evalArg(2);
                for (var i = 0; i < list.Count; i++)
                {
                    if (Equals(list[i], target))
                        list[i] = replacement;
                }
                result = list.ToArray();
                return true;
            }
        }

        result = null;
        return false;
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
            result = IsNullish(value) ? null : System.Text.Json.JsonSerializer.Serialize(value);
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
                System.Text.Json.JsonValueKind.Object => "object",
                System.Text.Json.JsonValueKind.Array => "array",
                System.Text.Json.JsonValueKind.String => "string",
                System.Text.Json.JsonValueKind.Number => "number",
                System.Text.Json.JsonValueKind.True => "boolean",
                System.Text.Json.JsonValueKind.False => "boolean",
                System.Text.Json.JsonValueKind.Null => "null",
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
                || element.ValueKind != System.Text.Json.JsonValueKind.Array)
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
                    System.Text.Json.JsonValueKind.String => element.GetString(),
                    System.Text.Json.JsonValueKind.Null => null,
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
            result = System.Text.Json.JsonSerializer.Serialize(element, options)
                .Replace("\r\n", "\n");
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalPostgresUuidFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!fn.Name.Equals("GEN_RANDOM_UUID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = Guid.NewGuid().ToString("D");
        return true;
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
            && !dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalOracleUserEnvFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("USERENV" or "ORA_INVOKING_USER" or "ORA_INVOKING_USERID" or "ORA_DST_AFFECTED" or "ORA_DST_CONVERT" or "ORA_DST_ERROR" or "ORA_DM_PARTITION_NAME"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleUserEnvFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        switch (name)
        {
            case "ORA_INVOKING_USER":
                result = "SYS";
                return true;
            case "ORA_INVOKING_USERID":
                result = 0;
                return true;
            case "USERENV":
                if (fn.Args.Count == 0)
                {
                    result = null;
                    return true;
                }
                var param = evalArg(0)?.ToString();
                if (string.Equals(param, "CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(param, "SESSION_USER", StringComparison.OrdinalIgnoreCase))
                {
                    result = "SYS";
                    return true;
                }
                result = null;
                return true;
            default:
                result = null;
                return true;
        }
    }

    private static bool TryEvalOracleValidateConversionFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not "VALIDATE_CONVERSION")
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleValidationFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        var type = evalArg(1)?.ToString() ?? string.Empty;
        if (IsNullish(value) || string.IsNullOrWhiteSpace(type))
        {
            result = 0;
            return true;
        }

        try
        {
            var normalized = type.Trim().ToUpperInvariant();
            _ = normalized switch
            {
                "NUMBER" => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                "DATE" => Convert.ToDateTime(value, CultureInfo.InvariantCulture),
                "TIMESTAMP" => Convert.ToDateTime(value, CultureInfo.InvariantCulture),
                _ => value
            };
            result = 1;
            return true;
        }
        catch
        {
            result = 0;
            return true;
        }
    }

    private static bool TryEvalOracleVsizeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("VSIZE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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

        var text = value?.ToString() ?? string.Empty;
        result = Encoding.UTF8.GetByteCount(text);
        return true;
    }

    private static bool TryEvalOracleWidthBucketFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("WIDTH_BUCKET", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count < 4)
        {
            result = null;
            return true;
        }

        var expr = evalArg(0);
        var min = evalArg(1);
        var max = evalArg(2);
        var count = evalArg(3);
        if (IsNullish(expr) || IsNullish(min) || IsNullish(max) || IsNullish(count))
        {
            result = null;
            return true;
        }

        var exprValue = Convert.ToDouble(expr, CultureInfo.InvariantCulture);
        var minValue = Convert.ToDouble(min, CultureInfo.InvariantCulture);
        var maxValue = Convert.ToDouble(max, CultureInfo.InvariantCulture);
        var bucketCount = Convert.ToInt32(count.ToDec());
        if (bucketCount <= 0 || maxValue <= minValue)
        {
            result = null;
            return true;
        }

        if (exprValue < minValue)
        {
            result = 0;
            return true;
        }

        if (exprValue >= maxValue)
        {
            result = bucketCount + 1;
            return true;
        }

        var width = (maxValue - minValue) / bucketCount;
        var bucket = (int)Math.Floor((exprValue - minValue) / width) + 1;
        result = bucket;
        return true;
    }

    private static bool TryEvalOracleAnalyticsFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("FEATURE_COMPARE" or "FEATURE_DETAILS" or "FEATURE_ID" or "FEATURE_SET" or "FEATURE_VALUE"
            or "NCGR" or "POWERMULTISET" or "POWERMULTISET_BY_CARDINALITY" or "PREDICTION" or "PREDICTION_BOUNDS"
            or "PREDICTION_COST" or "PREDICTION_DETAILS" or "PREDICTION_PROBABILITY" or "PREDICTION_SET"
            or "PRESENTNNV" or "PRESENTV" or "RATIO_TO_REPORT"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleAnalyticsFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        result = null;
        return true;
    }

    private static bool TryEvalOracleScnFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SCN_TO_TIMESTAMP" or "TIMESTAMP_TO_SCN"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (!dialect.SupportsOracleScnFunction(name))
            throw SqlUnsupported.ForDialect(dialect, name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleTimeZoneOffsetFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TZ_OFFSET", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        if (fn.Args.Count == 0)
        {
            var offset = DateTimeOffset.Now.Offset;
            result = $"{(offset < TimeSpan.Zero ? "-" : "+")}{offset:hh\\:mm}";
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (TryParseOffset(value!.ToString() ?? string.Empty, out var parsed))
        {
            result = $"{(parsed < TimeSpan.Zero ? "-" : "+")}{parsed:hh\\:mm}";
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleXmlFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("EXTRACTVALUE" or "XMLCAST" or "XMLCDATA" or "XMLCOLATTVAL" or "XMLCOMMENT" or "XMLCONCAT"
            or "XMLDIFF" or "XMLELEMENT" or "XMLEXISTS" or "XMLFOREST" or "XMLISVALID" or "XMLPARSE" or "XMLPATCH"
            or "XMLPI" or "XMLQUERY" or "XMLROOT" or "XMLSEQUENCE" or "XMLSERIALIZE" or "XMLTABLE" or "XMLTRANSFORM"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalOracleUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "SYS";
        return true;
    }

    private static bool TryParseOffset(string value, out TimeSpan offset)
    {
        offset = default;
        var trimmed = value.Trim();
        if (string.Equals(trimmed, "UTC", StringComparison.OrdinalIgnoreCase))
        {
            offset = TimeSpan.Zero;
            return true;
        }

        if (TimeSpan.TryParse(trimmed, CultureInfo.InvariantCulture, out offset))
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
        var map = new Dictionary<string, DayOfWeek>(StringComparer.OrdinalIgnoreCase)
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

        if (map.TryGetValue(normalized, out day))
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

    private static bool TryEvalNumericFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        var value = evalArg(0);

        if (name.Equals("ABS", StringComparison.OrdinalIgnoreCase))
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

    private static double Log2(double value)
        => Math.Log(value, 2d);

    private static long NextRandomInt64()
    {
        var buffer = new byte[8];
        lock (_randomLock)
            _sharedRandom.NextBytes(buffer);
        return BitConverter.ToInt64(buffer, 0);
    }

    private static double NextRandomDouble()
    {
        lock (_randomLock)
            return _sharedRandom.NextDouble();
    }

    private static bool TryEvalAppNameFunction(
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

    private static bool TryEvalCharIndexFunction(
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

    private static bool TryEvalCurrentUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            ? "root@localhost"
            : "dbo";
        return true;
    }

    private bool TryEvalSqlServerIdentityFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SCHEMA_ID" or "SCHEMA_NAME" or "SCOPE_IDENTITY" or "SUSER_ID" or "SUSER_SID" or "SUSER_NAME" or "SUSER_SNAME" or "TYPE_ID" or "TYPE_NAME" or "USER_ID" or "USER_NAME"))
            return false;

        result = name switch
        {
            "SCHEMA_ID" => 1,
            "SCHEMA_NAME" => "dbo",
            "SCOPE_IDENTITY" => _cnn.GetLastInsertId(),
            "SUSER_ID" => 1,
            "SUSER_SID" => new byte[] { 0x01 },
            "SUSER_NAME" or "SUSER_SNAME" => "sa",
            "TYPE_ID" => TryResolveSqlServerSystemTypeId(evalArg(0)?.ToString()),
            "TYPE_NAME" => TryResolveSqlServerSystemTypeName(evalArg(0)),
            "USER_ID" => 1,
            "USER_NAME" => "dbo",
            _ => null
        };

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

    private bool TryEvalSqlServerDatabaseFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        result = name switch
        {
            "DATABASEPROPERTYEX" => TryResolveSqlServerDatabaseProperty(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            "DATABASE_PRINCIPAL_ID" => TryResolveSqlServerDatabasePrincipalId(evalArg(0)?.ToString()),
            "COLUMNPROPERTY" => TryResolveSqlServerColumnProperty(evalArg(0), evalArg(1)?.ToString(), evalArg(2)?.ToString()),
            "COL_LENGTH" => TryResolveSqlServerColumnLength(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            "COL_NAME" => TryResolveSqlServerColumnName(evalArg(0), evalArg(1)),
            "DB_ID" => 1,
            "DB_NAME" => _cnn.Database,
            "OBJECT_ID" => TryResolveSqlServerObjectId(evalArg(0)?.ToString()),
            "OBJECTPROPERTY" => TryResolveSqlServerObjectProperty(evalArg(0), evalArg(1)?.ToString()),
            "OBJECTPROPERTYEX" => TryResolveSqlServerObjectProperty(evalArg(0), evalArg(1)?.ToString()),
            "OBJECT_NAME" => TryResolveSqlServerObjectName(evalArg(0)),
            "OBJECT_SCHEMA_NAME" => TryResolveSqlServerObjectSchemaName(evalArg(0)),
            "ORIGINAL_DB_NAME" => _cnn.Database,
            "TYPEPROPERTY" => TryResolveSqlServerTypeProperty(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            _ => null
        };

        return result is not null;
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
            "ISTABLE" => entry.ObjectKind == "TABLE" ? 1 : 0,
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
                objects.Add((nextId++, schema.SchemaName, table.TableName, $"{schema.SchemaName}.{table.TableName}", "TABLE"));
            }

            foreach (var procedure in schema.Procedures.Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase))
            {
                objects.Add((nextId++, schema.SchemaName, procedure, $"{schema.SchemaName}.{procedure}", "PROCEDURE"));
            }
        }

        return objects;
    }

    private bool TryEvalSqlServerServerPropertyFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            || !fn.Name.Equals("SERVERPROPERTY", StringComparison.OrdinalIgnoreCase))
            return false;

        var propertyName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(propertyName))
            return true;

        result = propertyName!.Trim().ToUpperInvariant() switch
        {
            "PRODUCTVERSION" => _cnn.Db.Version.ToString(CultureInfo.InvariantCulture),
            "SERVERNAME" => "DbSqlLikeMem",
            _ => null
        };

        return true;
    }

    private bool TryEvalSqlServerConnectionPropertyFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            || !fn.Name.Equals("CONNECTIONPROPERTY", StringComparison.OrdinalIgnoreCase))
            return false;

        var propertyName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(propertyName))
            return true;

        result = propertyName!.Trim().ToUpperInvariant() switch
        {
            "NET_TRANSPORT" => "TCP",
            "PROTOCOL_TYPE" => "TSQL",
            "LOCAL_NET_ADDRESS" => "127.0.0.1",
            _ => null
        };

        return true;
    }

    private bool TryEvalSqlServerContextInfoFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            || !fn.Name.Equals("CONTEXT_INFO", StringComparison.OrdinalIgnoreCase))
            return false;

        result = _cnn.GetContextInfo();
        return true;
    }

    private bool TryEvalSqlServerSessionFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        result = fn.Name.ToUpperInvariant() switch
        {
            "CURRENT_REQUEST_ID" => 1,
            "CURRENT_TRANSACTION_ID" => _cnn.HasActiveTransaction ? 1L : null,
            "IS_MEMBER" => TryResolveSqlServerRoleMembership(evalArg(0)?.ToString()),
            "IS_ROLEMEMBER" => TryResolveSqlServerRoleMembership(evalArg(0)?.ToString()),
            "IS_SRVROLEMEMBER" => TryResolveSqlServerServerRoleMembership(evalArg(0)?.ToString()),
            "ORIGINAL_LOGIN" => "sa",
            "SESSION_ID" => 1,
            "XACT_STATE" => _cnn.HasActiveTransaction ? 1 : 0,
            _ => null
        };

        return result is not null;
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

    private static bool TryEvalDataLengthFunction(
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
        result = System.Text.Encoding.Unicode.GetByteCount(text);
        return true;
    }

    private bool TryEvalDateNameFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATENAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATENAME() espera 2 argumentos.");

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var value = evalArg(1);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            "YEAR" or "YEARS" => dateTime.Year.ToString(CultureInfo.InvariantCulture),
            "MONTH" or "MONTHS" => dateTime.ToString("MMMM", CultureInfo.InvariantCulture),
            "DAY" or "DAYS" => dateTime.Day.ToString(CultureInfo.InvariantCulture),
            "HOUR" or "HOURS" => dateTime.Hour.ToString(CultureInfo.InvariantCulture),
            "MINUTE" or "MINUTES" => dateTime.Minute.ToString(CultureInfo.InvariantCulture),
            "SECOND" or "SECONDS" => dateTime.Second.ToString(CultureInfo.InvariantCulture),
            _ => null
        };
        return true;
    }

    private bool TryEvalDatePartFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DATEPART" or "DAY" or "MONTH" or "YEAR" or "HOUR" or "MINUTE" or "SECOND"))
        {
            result = null;
            return false;
        }

        if (name == "DATEPART" && fn.Args.Count < 2)
            throw new InvalidOperationException("DATEPART() espera 2 argumentos.");

        var unit = name == "DATEPART" ? GetDateAddUnit(fn.Args[0], row, group, ctes) : name;
        var value = evalArg(name == "DATEPART" ? 1 : 0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            "YEAR" or "YEARS" => dateTime.Year,
            "MONTH" or "MONTHS" => dateTime.Month,
            "DAY" or "DAYS" => dateTime.Day,
            "HOUR" or "HOURS" => dateTime.Hour,
            "MINUTE" or "MINUTES" => dateTime.Minute,
            "SECOND" or "SECONDS" => dateTime.Second,
            _ => null
        };
        return true;
    }

    private static bool TryEvalDb2DateAliasFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DAYNAME" or "DAYOFMONTH" or "DAYOFWEEK" or "DAYOFWEEK_ISO" or "DAYOFYEAR" or "WEEK_ISO"))
            return false;

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
            return true;

        result = name switch
        {
            "DAYNAME" => dateTime.ToString("dddd", CultureInfo.InvariantCulture),
            "DAYOFMONTH" => dateTime.Day,
            "DAYOFWEEK" => (int)dateTime.DayOfWeek + 1,
            "DAYOFWEEK_ISO" => ((int)dateTime.DayOfWeek + 6) % 7 + 1,
            "DAYOFYEAR" => dateTime.DayOfYear,
            "WEEK_ISO" => GetIsoWeekOfYear(dateTime),
            _ => null
        };
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

    private bool TryEvalDateDiffBigFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATEDIFF_BIG", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 3)
            throw new InvalidOperationException("DATEDIFF_BIG() espera 3 argumentos.");

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);
        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            "DAY" or "DAYS" => (long)(end.Date - start.Date).TotalDays,
            "HOUR" or "HOURS" => (long)(end - start).TotalHours,
            "MINUTE" or "MINUTES" => (long)(end - start).TotalMinutes,
            "SECOND" or "SECONDS" => (long)(end - start).TotalSeconds,
            "MONTH" or "MONTHS" => (long)DiffMonths(start, end),
            "YEAR" or "YEARS" => (long)DiffYears(start, end),
            _ => null
        };
        return true;
    }

    private static bool TryEvalDateFromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATEFROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("DATEFROMPARTS() espera ano, mês e dia.");

        var yearValue = evalArg(0);
        var monthValue = evalArg(1);
        var dayValue = evalArg(2);
        if (IsNullish(yearValue) || IsNullish(monthValue) || IsNullish(dayValue))
        {
            result = null;
            return true;
        }

        try
        {
            result = new DateTime(
                Convert.ToInt32(yearValue.ToDec()),
                Convert.ToInt32(monthValue.ToDec()),
                Convert.ToInt32(dayValue.ToDec()));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTimeFromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 6)
            throw new InvalidOperationException("DATETIMEFROMPARTS() espera ao menos 6 argumentos.");

        var values = new object?[6];
        for (var i = 0; i < 6; i++)
            values[i] = evalArg(i);

        if (values.Any(IsNullish))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, second);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTime2FromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATETIME2FROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 7)
            throw new InvalidOperationException("DATETIME2FROMPARTS() espera ao menos 7 argumentos.");

        var values = new object?[7];
        for (var i = 0; i < 7; i++)
            values[i] = evalArg(i);

        if (values.Any(IsNullish))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            var fraction = Convert.ToInt32(values[6]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, second)
                .AddTicks(fraction * 10L);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalDateTimeOffsetFromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATETIMEOFFSETFROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 8)
            throw new InvalidOperationException("DATETIMEOFFSETFROMPARTS() espera ao menos 8 argumentos.");

        var values = new object?[8];
        for (var i = 0; i < 8; i++)
            values[i] = evalArg(i);

        if (values.Any(IsNullish))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            var second = Convert.ToInt32(values[5]!.ToDec());
            var fraction = Convert.ToInt32(values[6]!.ToDec());
            var offsetMinutes = Convert.ToInt32(values[7]!.ToDec());
            var offset = TimeSpan.FromMinutes(offsetMinutes);
            result = new DateTimeOffset(new DateTime(year, month, day, hour, minute, second).AddTicks(fraction * 10L), offset);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTimeFromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIMEFROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 5)
            throw new InvalidOperationException("TIMEFROMPARTS() espera ao menos 5 argumentos.");

        var values = new object?[5];
        for (var i = 0; i < 5; i++)
            values[i] = evalArg(i);

        if (values.Any(IsNullish))
        {
            result = null;
            return true;
        }

        try
        {
            var hour = Convert.ToInt32(values[0]!.ToDec());
            var minute = Convert.ToInt32(values[1]!.ToDec());
            var second = Convert.ToInt32(values[2]!.ToDec());
            var fractions = Convert.ToInt32(values[3]!.ToDec());
            _ = Convert.ToInt32(values[4]!.ToDec());
            result = new TimeSpan(0, hour, minute, second).Add(TimeSpan.FromTicks(fractions * 10L));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSmallDateTimeFromPartsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SMALLDATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 5)
            throw new InvalidOperationException("SMALLDATETIMEFROMPARTS() espera ao menos 5 argumentos.");

        var values = new object?[5];
        for (var i = 0; i < 5; i++)
            values[i] = evalArg(i);

        if (values.Any(IsNullish))
        {
            result = null;
            return true;
        }

        try
        {
            var year = Convert.ToInt32(values[0]!.ToDec());
            var month = Convert.ToInt32(values[1]!.ToDec());
            var day = Convert.ToInt32(values[2]!.ToDec());
            var hour = Convert.ToInt32(values[3]!.ToDec());
            var minute = Convert.ToInt32(values[4]!.ToDec());
            result = new DateTime(year, month, day, hour, minute, 0);
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

    private static bool TryEvalErrorFunctions(
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

    private static bool TryEvalSqlServerFormatFunction(
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

    private static bool TryEvalSqlServerFormatMessageFunction(
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
            Enumerable.Range(1, Math.Max(0, fn.Args.Count - 1))
                .Select(evalArg)
                .ToArray());
        return true;
    }

    private static bool TryEvalSqlServerCompressFunction(
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

    private static bool TryEvalSqlServerDecompressFunction(
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

    private static bool TryEvalSqlServerChecksumFunction(
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

    private static bool TryEvalGetUtcDateFunction(
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

    private static bool TryEvalGroupingFunctions(
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

        if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
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

    private static bool TryEvalSqlServerGuidFunctions(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!(fn.Name.Equals("NEWID", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NEWSEQUENTIALID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = Guid.NewGuid().ToString("D");
        return true;
    }

    private static bool TryEvalSqlServerStringEscapeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRING_ESCAPE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STRING_ESCAPE() espera texto e tipo.");

        var textValue = evalArg(0);
        if (IsNullish(textValue))
        {
            result = null;
            return true;
        }

        var typeValue = evalArg(1)?.ToString() ?? string.Empty;
        if (!typeValue.Equals("json", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("STRING_ESCAPE() currently supports only 'json' in the mock.");

        result = EscapeSqlServerJsonString(textValue?.ToString() ?? string.Empty);
        return true;
    }

    private static string EscapeSqlServerJsonString(string text)
    {
        var builder = new StringBuilder(text.Length);
        foreach (var ch in text)
        {
            builder.Append(ch switch
            {
                '"' => "\\\"",
                '\\' => "\\\\",
                '\b' => "\\b",
                '\f' => "\\f",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                _ when ch < 0x20 => $"\\u{(int)ch:x4}",
                _ => ch.ToString()
            });
        }

        return builder.ToString();
    }

    private static bool TryEvalSqlServerStrFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STR", StringComparison.OrdinalIgnoreCase))
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

        if (!TryConvertNumericToDecimal(value, out var number))
        {
            result = null;
            return true;
        }

        var length = fn.Args.Count > 1 ? Convert.ToInt32(evalArg(1).ToDec(), CultureInfo.InvariantCulture) : 10;
        var decimals = fn.Args.Count > 2 ? Convert.ToInt32(evalArg(2).ToDec(), CultureInfo.InvariantCulture) : 0;
        decimals = Math.Min(16, Math.Max(0, decimals));

        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        var rounded = Math.Round(number, decimals, MidpointRounding.AwayFromZero);
        var text = rounded.ToString($"F{decimals}", CultureInfo.InvariantCulture);
        if (text.Length > length)
        {
            result = new string('*', length);
            return true;
        }

        result = text.PadLeft(length, ' ');
        return true;
    }

    private static bool TryEvalSqlServerDateTimeOffsetFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("TODATETIMEOFFSET" or "SWITCHOFFSET"))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() expects value and offset.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        var offsetText = evalArg(1)?.ToString() ?? string.Empty;
        if (!TryParseOffset(offsetText, out var offset))
        {
            result = null;
            return true;
        }

        if (name == "TODATETIMEOFFSET")
        {
            if (!TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            result = new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified), offset);
            return true;
        }

        DateTimeOffset dto;
        if (baseValue is DateTimeOffset directDto)
        {
            dto = directDto;
        }
        else if (!DateTimeOffset.TryParse(baseValue!.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dto))
        {
            result = null;
            return true;
        }

        result = dto.ToOffset(offset);
        return true;
    }

    private static bool TryEvalIsDateFunction(
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

        result = DateTime.TryParse(value?.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalIsJsonFunction(
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
            using var _ = System.Text.Json.JsonDocument.Parse(value?.ToString() ?? string.Empty);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    private static bool TryEvalIsNumericFunction(
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

    private static bool TryEvalAddDateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADDDATE() espera 2 argumentos.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var addValue = evalArg(1);
        if (addValue is IntervalValue interval)
        {
            result = dateTime.Add(interval.Span);
            return true;
        }

        if (TryConvertNumericToDouble(addValue, out var dayOffset))
        {
            result = dateTime.AddDays(dayOffset);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalAddTimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ADDTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADDTIME() espera 2 argumentos.");

        var baseValue = evalArg(0);
        var addValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(addValue))
        {
            result = null;
            return true;
        }

        if (TryCoerceDateTime(baseValue, out var dateTime) && TryCoerceTimeSpan(addValue, out var addSpan))
        {
            result = dateTime.Add(addSpan);
            return true;
        }

        if (TryCoerceTimeSpan(baseValue, out var baseSpan) && TryCoerceTimeSpan(addValue, out var addSpan2))
        {
            result = baseSpan.Add(addSpan2);
            return true;
        }

        result = null;
        return true;
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
        if (!System.Net.IPAddress.TryParse(text, out var ip))
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
            using var doc = System.Text.Json.JsonDocument.Parse(text!);
            result = GetJsonDepth(doc.RootElement);
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

    private static bool TryEvalJsonUtilityFunctions(
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
                using var _ = System.Text.Json.JsonDocument.Parse(text!);
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
                System.Text.Json.JsonValueKind.Object => "OBJECT",
                System.Text.Json.JsonValueKind.Array => "ARRAY",
                System.Text.Json.JsonValueKind.String => "STRING",
                System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out _)
                    ? "INTEGER"
                    : "DOUBLE",
                System.Text.Json.JsonValueKind.True => "BOOLEAN",
                System.Text.Json.JsonValueKind.False => "BOOLEAN",
                System.Text.Json.JsonValueKind.Null => "NULL",
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
                System.Text.Json.JsonValueKind.Array => element.GetArrayLength(),
                System.Text.Json.JsonValueKind.Object => element.EnumerateObject().Count(),
                _ => 1
            };
            return true;
        }

        if (fn.Name.Equals("JSON_STORAGE_SIZE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
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
            if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
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
            result = System.Text.Json.JsonSerializer.Serialize(text);
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
            result = System.Text.Json.JsonSerializer.Serialize(element, options)
                .Replace("\r\n", "\n");
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

            if (element.ValueKind != System.Text.Json.JsonValueKind.Object)
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

    private static bool TryEvalMinMaxFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isGreatest = fn.Name.Equals("GREATEST", StringComparison.OrdinalIgnoreCase);
        var isLeast = fn.Name.Equals("LEAST", StringComparison.OrdinalIgnoreCase);
        if (!isGreatest && !isLeast)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        object? current = null;
        foreach (var index in Enumerable.Range(0, fn.Args.Count))
        {
            var value = evalArg(index);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (current is null)
            {
                current = value;
                continue;
            }

            var comparison = current.Compare(value!, dialect);
            if (isGreatest && comparison < 0)
                current = value;
            else if (isLeast && comparison > 0)
                current = value;
        }

        result = current;
        return true;
    }

    private static bool TryEvalLastDayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LAST_DAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var lastDay = DateTime.DaysInMonth(dateTime.Year, dateTime.Month);
        result = new DateTime(dateTime.Year, dateTime.Month, lastDay);
        return true;
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

    private static bool TryEvalLocateFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LOCATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var startPosition = fn.Args.Count > 2 ? evalArg(2) : null;
        var startIndex = 0;

        if (!IsNullish(startPosition))
        {
            startIndex = Convert.ToInt32(startPosition.ToDec()) - 1;
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

        var index = haystack.IndexOf(needle, startIndex, dialect.TextComparison);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalLogFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isLn = fn.Name.Equals("LN", StringComparison.OrdinalIgnoreCase);
        var isLog = fn.Name.Equals("LOG", StringComparison.OrdinalIgnoreCase);
        var isLog10 = fn.Name.Equals("LOG10", StringComparison.OrdinalIgnoreCase);
        var isLog2 = fn.Name.Equals("LOG2", StringComparison.OrdinalIgnoreCase);
        if (!isLn && !isLog && !isLog10 && !isLog2)
        {
            result = null;
            return false;
        }

        var value = evalArg(isLog && fn.Args.Count > 1 ? 1 : 0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        double number;
        try
        {
            number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }
        catch
        {
            result = null;
            return true;
        }

        if (number <= 0)
        {
            result = null;
            return true;
        }

        if (isLog10)
        {
            result = Math.Log10(number);
            return true;
        }

        if (isLog2)
        {
            result = Log2(number);
            return true;
        }

        if (isLog && fn.Args.Count > 1)
        {
            var baseValue = evalArg(0);
            if (IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            double baseNumber;
            try
            {
                baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            }
            catch
            {
                result = null;
                return true;
            }

            if (baseNumber <= 0 || baseNumber == 1)
            {
                result = null;
                return true;
            }

            result = Math.Log(number, baseNumber);
            return true;
        }

        var isPostgreSql = dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase);
        result = isLog && isPostgreSql
            ? Math.Log10(number)
            : Math.Log(number);
        return true;
    }

    private static bool TryEvalInstrFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("INSTR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var haystack = evalArg(0);
        var needle = evalArg(1);
        if (IsNullish(haystack) || IsNullish(needle))
        {
            result = null;
            return true;
        }

        var haystackText = haystack?.ToString() ?? string.Empty;
        var needleText = needle?.ToString() ?? string.Empty;
        if (needleText.Length == 0)
        {
            result = 1;
            return true;
        }

        var index = haystackText.IndexOf(needleText, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalGlobFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("GLOB", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var pattern = evalArg(1);
        if (IsNullish(value) || IsNullish(pattern))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var patternText = pattern?.ToString() ?? string.Empty;
        var regex = GlobToRegex(patternText);
        result = regex.IsMatch(text) ? 1 : 0;
        return true;
    }

    private static bool TryEvalLikeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LIKE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var pattern = evalArg(1);
        if (IsNullish(value) || IsNullish(pattern))
        {
            result = null;
            return true;
        }

        var escape = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var escapeText = string.IsNullOrEmpty(escape) ? null : escape![0].ToString();
        var matches = value!.ToString()!.Like(pattern!.ToString()!, dialect, escapeText);
        result = matches ? 1 : 0;
        return true;
    }

    private static bool TryEvalPatIndexFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("PATINDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var pattern = evalArg(0);
        var value = evalArg(1);
        if (IsNullish(pattern) || IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value!.ToString()!.PatIndex(pattern!.ToString()!, dialect);
        return true;
    }

    private static Regex GlobToRegex(string pattern)
    {
        var builder = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var ch = pattern[i];
            switch (ch)
            {
                case '*':
                    builder.Append(".*");
                    break;
                case '?':
                    builder.Append(".");
                    break;
                case '[':
                    var end = pattern.IndexOf(']', i + 1);
                    if (end > i)
                    {
                        var content = pattern.Substring(i + 1, end - i - 1);
                        builder.Append('[').Append(Regex.Escape(content).Replace("\\-", "-")).Append(']');
                        i = end;
                    }
                    else
                    {
                        builder.Append("\\[");
                    }
                    break;
                default:
                    builder.Append(Regex.Escape(ch.ToString()));
                    break;
            }
        }

        builder.Append("$");
        return new Regex(builder.ToString(), RegexOptions.CultureInvariant);
    }

    private static bool TryEvalStrftimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRFTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var format = evalArg(0)?.ToString() ?? string.Empty;
        DateTime dateTime;
        if (fn.Args.Count > 1)
        {
            var baseValue = evalArg(1);
            if (IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            if (TryCoerceDateTime(baseValue, out var parsed))
            {
                dateTime = parsed;
            }
            else if (TryConvertNumericToDouble(baseValue, out var epoch))
            {
                dateTime = DateTimeOffset.FromUnixTimeSeconds((long)epoch).DateTime;
            }
            else
            {
                result = null;
                return true;
            }
        }
        else
        {
            dateTime = DateTime.Now;
        }

        for (var i = 2; i < fn.Args.Count; i++)
        {
            var modifier = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(modifier))
                continue;

            if (modifier!.Equals("utc", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = dateTime.ToUniversalTime();
                continue;
            }

            if (modifier.Equals("localtime", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = dateTime.ToLocalTime();
                continue;
            }

            if (modifier.Equals("unixepoch", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = DateTimeOffset.FromUnixTimeSeconds((long)dateTime.ToUniversalTime().Subtract(_unixEpoch).TotalSeconds).DateTime;
                continue;
            }

            if (TryParseDateModifier(modifier!, out var unit, out var amount))
            {
                dateTime = ApplyDateDelta(dateTime, unit, amount);
            }
        }

        result = FormatSqliteStrftime(format, dateTime);
        return true;
    }

    private static string FormatSqliteStrftime(string format, DateTime dateTime)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            builder.Append(token switch
            {
                'Y' => dateTime.ToString("yyyy", CultureInfo.InvariantCulture),
                'm' => dateTime.ToString("MM", CultureInfo.InvariantCulture),
                'd' => dateTime.ToString("dd", CultureInfo.InvariantCulture),
                'H' => dateTime.ToString("HH", CultureInfo.InvariantCulture),
                'M' => dateTime.ToString("mm", CultureInfo.InvariantCulture),
                'S' => dateTime.ToString("ss", CultureInfo.InvariantCulture),
                'f' => dateTime.ToString("ss.fff", CultureInfo.InvariantCulture),
                's' => new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
                'J' => (dateTime.ToOADate() + 2415018.5d).ToString("0.000000", CultureInfo.InvariantCulture),
                '%' => "%",
                _ => $"%{token}"
            });
        }

        return builder.ToString();
    }

    private static bool TryEvalPrintfFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("PRINTF", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("FORMAT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SQLITE3_MPRINTF", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var format = evalArg(0)?.ToString() ?? string.Empty;
        var args = new object?[Math.Max(0, fn.Args.Count - 1)];
        for (var i = 1; i < fn.Args.Count; i++)
            args[i - 1] = evalArg(i);

        if (fn.Name.Equals("FORMAT", StringComparison.OrdinalIgnoreCase)
            && dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = FormatPostgreSql(format, args);
            return true;
        }

        result = FormatPrintf(format, args);
        return true;
    }

    private static string FormatPrintf(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            var text = token switch
            {
                'd' or 'i' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                'f' => IsNullish(value) ? "0" : Convert.ToDouble(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                's' => value?.ToString() ?? string.Empty,
                'x' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("x", CultureInfo.InvariantCulture),
                'X' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture),
                _ => value?.ToString() ?? string.Empty
            };

            builder.Append(text);
        }

        return builder.ToString();
    }

    private static string FormatPostgreSql(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            builder.Append(token switch
            {
                's' => value?.ToString() ?? string.Empty,
                'I' => QuoteFormatIdentifier(value),
                'L' => QuoteFormatLiteral(value),
                _ => value?.ToString() ?? string.Empty
            });
        }

        return builder.ToString();
    }

    private static string QuoteFormatIdentifier(object? value)
    {
        var text = value?.ToString() ?? string.Empty;
        return $"\"{text.Replace("\"", "\"\"")}\"";
    }

    private static string QuoteFormatLiteral(object? value)
    {
        if (value is null || value is DBNull)
            return "NULL";

        var text = value.ToString() ?? string.Empty;
        return $"'{text.Replace("'", "''")}'";
    }

    private static bool TryEvalRandomFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("RANDOM", StringComparison.OrdinalIgnoreCase))
        {
            result = dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase)
                ? NextRandomDouble()
                : NextRandomInt64();
            return true;
        }

        if (fn.Name.Equals("RANDOMBLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("ZEROBLOB", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SQLITE3_RESULT_ZEROBLOB", StringComparison.OrdinalIgnoreCase))
        {
            var lengthValue = evalArg(0);
            if (IsNullish(lengthValue))
            {
                result = null;
                return true;
            }

            var length = Convert.ToInt32(lengthValue.ToDec());
            if (length <= 0)
            {
                result = Array.Empty<byte>();
                return true;
            }

            var buffer = new byte[length];
            if (fn.Name.Equals("RANDOMBLOB", StringComparison.OrdinalIgnoreCase))
            {
                lock (_randomLock)
                    _sharedRandom.NextBytes(buffer);
            }

            result = buffer;
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalTypeofFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TYPEOF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = "null";
            return true;
        }

        result = value switch
        {
            sbyte or byte or short or ushort or int or uint or long or ulong or bool => "integer",
            float or double or decimal => "real",
            byte[] => "blob",
            _ => "text"
        };
        return true;
    }

    private static bool TryEvalUnicodeFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("UNICODE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            if (text.Length == 0)
            {
                result = null;
                return true;
            }

            var codePoint = text.Length >= 2 && char.IsSurrogatePair(text, 0)
                ? char.ConvertToUtf32(text, 0)
                : text[0];
            result = codePoint;
            return true;
        }

        if (fn.Name.Equals("UNISTR", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            result = UnescapeUnicodeLiteral(value?.ToString() ?? string.Empty);
            return true;
        }

        if (fn.Name.Equals("UNISTR_QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = $"'{text.Replace("'", "''")}'";
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalLikelihoodFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("LIKELY", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UNLIKELY", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LIKELIHOOD", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = evalArg(0);
        return true;
    }

    private static string UnescapeUnicodeLiteral(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var builder = new StringBuilder();
        for (var i = 0; i < input.Length; i++)
        {
            var ch = input[i];
            if (ch == '\\' && i + 1 < input.Length)
            {
                if (input[i + 1] == '+' && i + 9 < input.Length)
                {
                    var hex = input.Substring(i + 2, 6);
                    if (int.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var codePoint))
                    {
                        builder.Append(char.ConvertFromUtf32(codePoint));
                        i += 7;
                        continue;
                    }
                }

                if (i + 5 <= input.Length)
                {
                    var hex = input.Substring(i + 1, 4);
                    if (int.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var codeUnit))
                    {
                        builder.Append((char)codeUnit);
                        i += 4;
                        continue;
                    }
                }
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }

    private static bool TryParsePostgresInetValue(
        object? value,
        out System.Net.IPAddress address,
        out int prefixLength)
    {
        address = System.Net.IPAddress.None;
        prefixLength = 0;

        var text = value?.ToString()?.Trim() ?? string.Empty;
        if (string.IsNullOrEmpty(text))
            return false;

        var slashIndex = text.IndexOf('/');
        var addressText = slashIndex >= 0 ? text[..slashIndex] : text;
        if (!System.Net.IPAddress.TryParse(addressText, out var parsedAddress))
            return false;

        address = parsedAddress;

        var maxPrefix = address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 32 : 128;
        if (slashIndex < 0)
        {
            prefixLength = maxPrefix;
            return true;
        }

        var prefixText = text[(slashIndex + 1)..];
        if (!int.TryParse(prefixText, NumberStyles.Integer, CultureInfo.InvariantCulture, out prefixLength))
            return false;

        return prefixLength >= 0 && prefixLength <= maxPrefix;
    }

    private static byte[] BuildPrefixMaskBytes(int byteLength, int prefixLength)
    {
        var mask = new byte[byteLength];
        for (var i = 0; i < byteLength; i++)
        {
            var remainingBits = prefixLength - (i * 8);
            mask[i] = remainingBits switch
            {
                >= 8 => 0xFF,
                <= 0 => 0x00,
                _ => (byte)(0xFF << (8 - remainingBits))
            };
        }

        return mask;
    }

    private static byte[] ApplyNetworkMask(byte[] addressBytes, byte[] maskBytes)
    {
        var networkBytes = new byte[addressBytes.Length];
        for (var i = 0; i < addressBytes.Length; i++)
            networkBytes[i] = (byte)(addressBytes[i] & maskBytes[i]);

        return networkBytes;
    }

    private static long ComputeGreatestCommonDivisor(long left, long right)
    {
        while (right != 0)
        {
            var remainder = left % right;
            left = right;
            right = remainder;
        }

        return Math.Abs(left);
    }

    private static int GetMinimumNumericScale(object value)
    {
        var text = value switch
        {
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            double dbl => dbl.ToString("G17", CultureInfo.InvariantCulture),
            float flt => flt.ToString("G9", CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };

        var exponentIndex = text.IndexOfAny(['e', 'E']);
        if (exponentIndex >= 0)
        {
            if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDecimal))
                text = parsedDecimal.ToString(CultureInfo.InvariantCulture);
            else
                text = text[..exponentIndex];
        }

        var dotIndex = text.IndexOf('.');
        if (dotIndex < 0)
            return 0;

        var fractional = text[(dotIndex + 1)..].TrimEnd('0');
        return fractional.Length;
    }

    private static bool TryParsePostgresIdentifierParts(string text, out List<string> parts)
    {
        parts = [];
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var current = new StringBuilder();
        var insideQuotes = false;
        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (insideQuotes)
            {
                if (ch == '"')
                {
                    if (i + 1 < text.Length && text[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                        continue;
                    }

                    insideQuotes = false;
                    continue;
                }

                current.Append(ch);
                continue;
            }

            if (ch == '"')
            {
                insideQuotes = true;
                continue;
            }

            if (ch == '.')
            {
                parts.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        if (insideQuotes)
            return false;

        parts.Add(current.ToString().Trim());
        return parts.Count > 0 && parts.All(static part => part.Length > 0);
    }

    private static string ConvertToAscii(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var decomposed = text.Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed)
        {
            var category = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(ch);
            if (category is System.Globalization.UnicodeCategory.NonSpacingMark
                or System.Globalization.UnicodeCategory.SpacingCombiningMark
                or System.Globalization.UnicodeCategory.EnclosingMark)
            {
                continue;
            }

            if (ch <= 0x7F)
                builder.Append(ch);
        }

        return builder.ToString().Normalize(NormalizationForm.FormC);
    }

    private bool TryEvalSqliteSystemFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is "SQLITE_VERSION" or "SQLITE3_LIBVERSION")
        {
            var version = dialect?.Version ?? 3;
            result = $"{version}.0.0";
            return true;
        }

        if (name is "SQLITE_SOURCE_ID" or "SQLITE3_SOURCEID")
        {
            result = "DbSqlLikeMem.Sqlite";
            return true;
        }

        if (name is "SQLITE_COMPILEOPTION_GET" or "SQLITE3_COMPILEOPTION_GET")
        {
            result = null;
            return true;
        }

        if (name is "SQLITE_COMPILEOPTION_USED" or "SQLITE3_COMPILEOPTION_USED")
        {
            result = 0;
            return true;
        }

        if (name is "SQLITE_OFFSET")
        {
            result = 0;
            return true;
        }

        if (name is "SQLITE3_CHANGES64" or "SQLITE3_TOTAL_CHANGES64" or "TOTAL_CHANGES")
        {
            result = _cnn.GetLastFoundRows();
            return true;
        }

        if (name is "LOAD_EXTENSION" or "SQLITE3_LOAD_EXTENSION" or "SQLITE3_ENABLE_LOAD_EXTENSION")
        {
            result = 0;
            return true;
        }

        if (name is "READFILE")
        {
            var path = evalArg(0)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            {
                result = null;
                return true;
            }

            result = File.ReadAllBytes(path);
            return true;
        }

        if (name is "SQLITE3_LAST_INSERT_ROWID")
        {
            result = _cnn.GetLastInsertId() ?? 0;
            return true;
        }

        if (name is "SQLITE3_CREATE_FUNCTION"
            or "SQLITE3_CREATE_WINDOW_FUNCTION"
            or "SQLITE3_STEP"
            or "SQLITE3_RESULT_ZEROBLOB")
        {
            result = 0;
            return true;
        }

        if (name is "SQLITE3_MPRINTF")
        {
            result = FormatPrintf(evalArg(0)?.ToString() ?? string.Empty, Enumerable.Range(1, Math.Max(0, fn.Args.Count - 1))
                .Select(evalArg)
                .ToArray());
            return true;
        }

        if (name is "XFINAL" or "XINVERSE" or "XSTEP" or "XVALUE")
        {
            result = null;
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalSqliteJsonFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("JSON", StringComparison.OrdinalIgnoreCase))
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

            result = element.GetRawText();
            return true;
        }

        if (fn.Name.Equals("JSON_ARRAY_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind == System.Text.Json.JsonValueKind.Array
                ? element.GetArrayLength()
                : 0;
            return true;
        }

        if (fn.Name.Equals("JSON_ERROR_POSITION", StringComparison.OrdinalIgnoreCase))
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
                result = 1;
                return true;
            }

            try
            {
                using var _ = System.Text.Json.JsonDocument.Parse(text!);
                result = 0;
            }
            catch
            {
                result = 1;
            }

            return true;
        }

        if (fn.Name.Equals("JSON_PATCH", StringComparison.OrdinalIgnoreCase))
        {
            var baseValue = evalArg(0);
            var patchValue = evalArg(1);
            if (IsNullish(baseValue) || IsNullish(patchValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(baseValue!, out var baseNode) || baseNode is null)
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(patchValue!, out var patchNode) || patchNode is null)
            {
                result = null;
                return true;
            }

            ApplyJsonMergePatch(ref baseNode, patchNode);
            result = baseNode.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return false;
            }

            if (dialect.Version < 56)
                throw SqlUnsupported.ForDialect(dialect, "JSON_MERGE_PATCH");

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_MERGE_PATCH() espera dois JSONs.");

            var baseValue = evalArg(0);
            if (IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(baseValue!, out var baseNode) || baseNode is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var patchValue = evalArg(i);
                if (IsNullish(patchValue))
                {
                    result = null;
                    return true;
                }

                if (!TryParseJsonNode(patchValue!, out var patchNode) || patchNode is null)
                {
                    result = null;
                    return true;
                }

                ApplyJsonMergePatch(ref baseNode, patchNode);
            }

            result = baseNode.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_MERGE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return false;
            }

            if (dialect.Version < 56 || (dialect.Version >= 84 && fn.Name.Equals("JSON_MERGE", StringComparison.OrdinalIgnoreCase)))
                throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());

            if (dialect.Version >= 84 && fn.Name.Equals("JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase))
                throw SqlUnsupported.ForDialect(dialect, "JSON_MERGE_PRESERVE");

            if (fn.Args.Count < 2)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera dois JSONs.");

            var firstValue = evalArg(0);
            if (IsNullish(firstValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(firstValue!, out var merged) || merged is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var nextValue = evalArg(i);
                if (IsNullish(nextValue))
                {
                    result = null;
                    return true;
                }

                if (!TryParseJsonNode(nextValue!, out var nextNode) || nextNode is null)
                {
                    result = null;
                    return true;
                }

                merged = MergeJsonPreserve(merged, nextNode);
            }

            result = merged.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_APPEND", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_ARRAY_APPEND", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return false;
            }

            if (fn.Name.Equals("JSON_APPEND", StringComparison.OrdinalIgnoreCase))
            {
                if (dialect.Version < 56 || dialect.Version >= 80)
                    throw SqlUnsupported.ForDialect(dialect, "JSON_APPEND");
            }
            else if (dialect.Version < 56 || dialect.Version >= 84)
            {
                throw SqlUnsupported.ForDialect(dialect, "JSON_ARRAY_APPEND");
            }

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
                if (!TryAppendJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (fn.Name.Equals("JSON_ARRAY_INSERT", StringComparison.OrdinalIgnoreCase))
        {
            if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase) && dialect.Version < 56)
                throw SqlUnsupported.ForDialect(dialect, "JSON_ARRAY_INSERT");

            if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            {
                if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                    throw new InvalidOperationException("JSON_ARRAY_INSERT() espera um JSON seguido de pares path/valor.");

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
                    if (!TryInsertJsonPathValue(ref root, tokens, value))
                    {
                        result = null;
                        return true;
                    }
                }

                result = root.ToJsonString();
                return true;
            }

            var shim = new FunctionCallExpr("JSON_INSERT", fn.Args);
            result = TryEvalJsonUtilityFunctions(shim, dialect, evalArg, out var jsonInsertResult)
                ? jsonInsertResult
                : null;
            return true;
        }

        if (fn.Name.Equals("JSON_EACH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_TREE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSONB_EACH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSONB_TREE", StringComparison.OrdinalIgnoreCase))
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

            result = element.GetRawText();
            return true;
        }

        if (fn.Name.Equals("JSONB_EXTRACT", StringComparison.OrdinalIgnoreCase))
        {
            var shim = new FunctionCallExpr("JSON_EXTRACT", fn.Args);
            result = TryEvalJsonExtractionFunction(shim, dialect, evalArg, out var jsonExtractResult)
                ? jsonExtractResult
                : null;
            return true;
        }

        result = null;
        return false;
    }

    private static void ApplyJsonMergePatch(ref System.Text.Json.Nodes.JsonNode baseNode, System.Text.Json.Nodes.JsonNode patchNode)
    {
        if (patchNode is System.Text.Json.Nodes.JsonObject patchObject
            && baseNode is System.Text.Json.Nodes.JsonObject baseObject)
        {
            foreach (var pair in patchObject)
            {
                if (pair.Value is null)
                {
                    baseObject.Remove(pair.Key);
                    continue;
                }

                if (pair.Value is System.Text.Json.Nodes.JsonObject patchChild
                    && baseObject[pair.Key] is System.Text.Json.Nodes.JsonObject baseChild)
                {
                    var child = (System.Text.Json.Nodes.JsonNode)baseChild;
                    ApplyJsonMergePatch(ref child, patchChild);
                    baseObject[pair.Key] = child;
                    continue;
                }

                baseObject[pair.Key] = CloneJsonNode(pair.Value!);
            }

            return;
        }

        baseNode = CloneJsonNode(patchNode);
    }

    private static void StripJsonNullProperties(System.Text.Json.Nodes.JsonNode node)
    {
        if (node is System.Text.Json.Nodes.JsonObject obj)
        {
            var propertyNames = obj.Select(static pair => pair.Key).ToList();
            foreach (var propertyName in propertyNames)
            {
                var child = obj[propertyName];
                if (child is null)
                {
                    obj.Remove(propertyName);
                    continue;
                }

                StripJsonNullProperties(child);
            }

            return;
        }

        if (node is System.Text.Json.Nodes.JsonArray array)
        {
            foreach (var child in array)
            {
                if (child is not null)
                    StripJsonNullProperties(child);
            }
        }
    }

    private static System.Text.Json.Nodes.JsonNode CloneJsonNode(System.Text.Json.Nodes.JsonNode node)
        => System.Text.Json.Nodes.JsonNode.Parse(node.ToJsonString())!;

    private static bool TryEvalPadFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LPAD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var lenValue = evalArg(1);
        var padValue = fn.Args.Count > 2 ? evalArg(2) : " ";

        if (IsNullish(value) || IsNullish(lenValue) || IsNullish(padValue))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var padText = padValue?.ToString() ?? string.Empty;
        var len = Convert.ToInt32(lenValue.ToDec());

        if (len < 0 || padText.Length == 0)
        {
            result = null;
            return true;
        }

        if (len == 0)
        {
            result = string.Empty;
            return true;
        }

        if (text.Length >= len)
        {
            result = text.Substring(0, len);
            return true;
        }

        var padNeeded = len - text.Length;
        var sb = new StringBuilder(len);
        while (sb.Length < padNeeded)
            sb.Append(padText);

        var prefix = sb.ToString().Substring(0, padNeeded);
        result = prefix + text;
        return true;
    }

    private static bool TryEvalMakeDateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MAKEDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MAKEDATE() espera ano e dia do ano.");

        var yearValue = evalArg(0);
        var dayValue = evalArg(1);
        if (IsNullish(yearValue) || IsNullish(dayValue))
        {
            result = null;
            return true;
        }

        var year = Convert.ToInt32(yearValue.ToDec());
        var dayOfYear = Convert.ToInt32(dayValue.ToDec());
        if (dayOfYear <= 0)
        {
            result = null;
            return true;
        }

        try
        {
            result = new DateTime(year, 1, 1).AddDays(dayOfYear - 1);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMakeTimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MAKETIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("MAKETIME() espera hora, minuto e segundo.");

        var hourValue = evalArg(0);
        var minuteValue = evalArg(1);
        var secondValue = evalArg(2);
        if (IsNullish(hourValue) || IsNullish(minuteValue) || IsNullish(secondValue))
        {
            result = null;
            return true;
        }

        try
        {
            var hours = Convert.ToInt32(hourValue.ToDec());
            var minutes = Convert.ToInt32(minuteValue.ToDec());
            var seconds = Convert.ToDouble(secondValue, CultureInfo.InvariantCulture);
            var secondsInt = (int)Math.Truncate(seconds);
            var microseconds = (int)Math.Round((seconds - secondsInt) * 1_000_000d);
            var time = new TimeSpan(0, hours, minutes, secondsInt, 0)
                .Add(TimeSpan.FromTicks(microseconds * 10L));
            result = time;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMicrosecondFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MICROSECOND", StringComparison.OrdinalIgnoreCase))
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

        if (TryCoerceDateTime(value, out var dateTime))
        {
            var micro = (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10);
            result = micro;
            return true;
        }

        if (TryCoerceTimeSpan(value, out var span))
        {
            var micro = (int)((span.Ticks % TimeSpan.TicksPerSecond) / 10);
            result = micro;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalMd5Function(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MD5", StringComparison.OrdinalIgnoreCase))
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
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        using var md5 = System.Security.Cryptography.MD5.Create();
        var hash = ComputeHash(md5, bytes);
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));

        result = sb.ToString();
        return true;
    }

    private static bool TryParseJsonElement(object value, out System.Text.Json.JsonElement element)
    {
        if (value is System.Text.Json.JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(text);
            element = document.RootElement.Clone();
            return true;
        }
        catch
        {
            element = default;
            return false;
        }
    }

    private static bool TryParseJsonCandidate(object value, out System.Text.Json.JsonElement element)
    {
        if (value is System.Text.Json.JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        if (text.TrimStart().StartsWith("{", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("[", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("\"", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("true", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("false", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("null", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("-", StringComparison.Ordinal)
            || char.IsDigit(text.TrimStart()[0]))
        {
            try
            {
                using var document = System.Text.Json.JsonDocument.Parse(text);
                element = document.RootElement.Clone();
                return true;
            }
            catch
            {
                // fallthrough to treat as string
            }
        }

        var quoted = System.Text.Json.JsonSerializer.Serialize(text);
        using (var document = System.Text.Json.JsonDocument.Parse(quoted))
        {
            element = document.RootElement.Clone();
            return true;
        }
    }

    private static bool JsonContains(System.Text.Json.JsonElement target, System.Text.Json.JsonElement candidate)
    {
        if (candidate.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            if (target.ValueKind != System.Text.Json.JsonValueKind.Object)
                return false;

            foreach (var prop in candidate.EnumerateObject())
            {
                if (!target.TryGetProperty(prop.Name, out var targetProp))
                    return false;

                if (!JsonContains(targetProp, prop.Value))
                    return false;
            }

            return true;
        }

        if (candidate.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            if (target.ValueKind != System.Text.Json.JsonValueKind.Array)
                return false;

            var targetItems = target.EnumerateArray().ToArray();
            foreach (var candidateItem in candidate.EnumerateArray())
            {
                if (!targetItems.Any(item => JsonContains(item, candidateItem)))
                    return false;
            }

            return true;
        }

        return JsonElementEquals(target, candidate);
    }

    private static bool JsonElementEquals(System.Text.Json.JsonElement left, System.Text.Json.JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == System.Text.Json.JsonValueKind.Number
                && right.ValueKind == System.Text.Json.JsonValueKind.Number)
            {
                if (left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec))
                    return ldec == rdec;
                return left.GetDouble().Equals(right.GetDouble());
            }

            return false;
        }

        return left.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => left.GetString() == right.GetString(),
            System.Text.Json.JsonValueKind.Number => left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec)
                ? ldec == rdec
                : left.GetDouble().Equals(right.GetDouble()),
            System.Text.Json.JsonValueKind.True => right.ValueKind == System.Text.Json.JsonValueKind.True,
            System.Text.Json.JsonValueKind.False => right.ValueKind == System.Text.Json.JsonValueKind.False,
            System.Text.Json.JsonValueKind.Null => true,
            _ => left.GetRawText() == right.GetRawText()
        };
    }

    private static bool JsonOverlaps(System.Text.Json.JsonElement left, System.Text.Json.JsonElement right)
    {
        if (left.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var item in left.EnumerateArray())
            {
                if (JsonOverlaps(item, right))
                    return true;
            }

            return false;
        }

        if (right.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var item in right.EnumerateArray())
            {
                if (JsonOverlaps(left, item))
                    return true;
            }

            return false;
        }

        if (left.ValueKind == System.Text.Json.JsonValueKind.Object
            && right.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var prop in left.EnumerateObject())
            {
                if (right.TryGetProperty(prop.Name, out var rightProp)
                    && JsonOverlaps(prop.Value, rightProp))
                {
                    return true;
                }
            }

            return false;
        }

        if (left.ValueKind == System.Text.Json.JsonValueKind.Object
            || right.ValueKind == System.Text.Json.JsonValueKind.Object)
            return false;

        return JsonElementEquals(left, right);
    }

    private static System.Text.Json.Nodes.JsonNode MergeJsonPreserve(
        System.Text.Json.Nodes.JsonNode left,
        System.Text.Json.Nodes.JsonNode right)
    {
        if (left is System.Text.Json.Nodes.JsonObject leftObj
            && right is System.Text.Json.Nodes.JsonObject rightObj)
        {
            var merged = new System.Text.Json.Nodes.JsonObject();
            foreach (var pair in leftObj)
                merged[pair.Key] = pair.Value is null ? null : CloneJsonNode(pair.Value);

            foreach (var pair in rightObj)
            {
                if (merged.TryGetPropertyValue(pair.Key, out var existing) && existing is not null && pair.Value is not null)
                {
                    merged[pair.Key] = MergeJsonPreserve(existing, pair.Value);
                    continue;
                }

                merged[pair.Key] = pair.Value is null ? null : CloneJsonNode(pair.Value);
            }

            return merged;
        }

        if (left is System.Text.Json.Nodes.JsonArray leftArray
            && right is System.Text.Json.Nodes.JsonArray rightArray)
        {
            var merged = new System.Text.Json.Nodes.JsonArray();
            foreach (var item in leftArray)
                merged.Add(item is null ? null : CloneJsonNode(item));
            foreach (var item in rightArray)
                merged.Add(item is null ? null : CloneJsonNode(item));
            return merged;
        }

        return new System.Text.Json.Nodes.JsonArray
        {
            left is null ? null : CloneJsonNode(left),
            right is null ? null : CloneJsonNode(right)
        };
    }

    private static bool TryAppendJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (!TryGetJsonNodeAtPath(root, tokens, out var node))
            return false;

        if (node is System.Text.Json.Nodes.JsonArray array)
        {
            array.Add(CreateJsonNodeFromValue(value));
            return true;
        }

        var newArray = new System.Text.Json.Nodes.JsonArray
        {
            node is null ? null : CloneJsonNode(node),
            CreateJsonNodeFromValue(value)
        };

        return TrySetJsonPathValue(ref root, tokens, newArray);
    }

    private static bool TryInsertJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        var last = tokens[^1];
        if (last.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var parentTokens = tokens.Take(tokens.Count - 1).ToList();
            if (parentTokens.Count == 0)
            {
                if (root is not System.Text.Json.Nodes.JsonArray rootArray)
                    return false;

                var index = Math.Max(0, last.ArrayIndex ?? 0);
                var insertIndex = Math.Min(index, rootArray.Count);
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is not System.Text.Json.Nodes.JsonArray parentArray)
                return false;

            var parentIndex = Math.Max(0, last.ArrayIndex ?? 0);
            var targetIndex = Math.Min(parentIndex, parentArray.Count);
            parentArray.Insert(targetIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        if (!TryGetJsonNodeAtPath(root, tokens, out var node) || node is not System.Text.Json.Nodes.JsonArray array)
            return false;

        array.Add(CreateJsonNodeFromValue(value));
        return true;
    }

    private static bool TryReadPostgresJsonPathElement(
        System.Text.Json.JsonElement element,
        string pathSegment,
        out System.Text.Json.JsonElement target)
    {
        target = default;
        if (string.IsNullOrEmpty(pathSegment))
            return false;

        if (element.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            if (element.TryGetProperty(pathSegment, out target))
                return true;

            return false;
        }

        if (element.ValueKind == System.Text.Json.JsonValueKind.Array
            && int.TryParse(pathSegment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
            && index >= 0)
        {
            var currentIndex = 0;
            foreach (var item in element.EnumerateArray())
            {
                if (currentIndex == index)
                {
                    target = item;
                    return true;
                }

                currentIndex++;
            }
        }

        return false;
    }

    private static bool TryReadPostgresJsonPath(
        System.Text.Json.JsonElement element,
        string path,
        out System.Text.Json.JsonElement target)
    {
        target = default;
        if (!TryParseJsonPathTokens(path, out var tokens))
            return false;

        var current = element;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (!TryReadPostgresJsonPathElement(current, token.PropertyName ?? string.Empty, out current))
                    return false;

                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (!TryReadPostgresJsonPathElement(current, (token.ArrayIndex ?? 0).ToString(CultureInfo.InvariantCulture), out current))
                    return false;
            }
        }

        target = current;
        return true;
    }

    private static void CollectJsonSearchMatches(
        System.Text.Json.JsonElement element,
        string currentPath,
        string search,
        List<string> results)
    {
        if (element.ValueKind == System.Text.Json.JsonValueKind.String)
        {
            var text = element.GetString() ?? string.Empty;
            if (text.Contains(search, StringComparison.Ordinal))
                results.Add(currentPath);
            return;
        }

        if (element.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            var index = 0;
            foreach (var item in element.EnumerateArray())
            {
                CollectJsonSearchMatches(item, $"{currentPath}[{index}]", search, results);
                index++;
            }

            return;
        }

        if (element.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                CollectJsonSearchMatches(prop.Value, $"{currentPath}.{prop.Name}", search, results);
            }
        }
    }

    private static string BuildJsonArray(IEnumerable<object?> values)
    {
        var parts = values.Select(static value =>
        {
            if (value is null or DBNull)
                return "null";

            if (value is System.Text.Json.JsonElement element)
                return element.GetRawText();

            return System.Text.Json.JsonSerializer.Serialize(value);
        });

        return "[" + string.Join(",", parts) + "]";
    }

    private static string BuildJsonObject(IEnumerable<(string Key, object? Value)> pairs)
    {
        var parts = pairs.Select(static pair =>
        {
            var key = System.Text.Json.JsonSerializer.Serialize(pair.Key ?? string.Empty);
            var value = pair.Value;
            if (value is null or DBNull)
                return $"{key}:null";

            if (value is System.Text.Json.JsonElement element)
                return $"{key}:{element.GetRawText()}";

            return $"{key}:{System.Text.Json.JsonSerializer.Serialize(value)}";
        });

        return "{" + string.Join(",", parts) + "}";
    }

    private enum JsonPathTokenKind
    {
        Property,
        ArrayIndex
    }

    private readonly record struct JsonPathToken(JsonPathTokenKind Kind, string? PropertyName, int? ArrayIndex);

    private static bool TryParseJsonPathTokens(string path, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[4..].TrimStart();
        else if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[7..].TrimStart();

        if (trimmed.Length == 0 || trimmed[0] != '$')
            return false;

        var i = 1;
        while (i < trimmed.Length)
        {
            while (i < trimmed.Length && char.IsWhiteSpace(trimmed[i]))
                i++;

            if (i >= trimmed.Length)
                break;

            if (trimmed[i] == '.')
            {
                i++;
                var start = i;
                while (i < trimmed.Length && (char.IsLetterOrDigit(trimmed[i]) || trimmed[i] == '_'))
                    i++;

                if (i == start)
                    return false;

                var property = trimmed[start..i];
                tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                continue;
            }

            if (trimmed[i] == '[')
            {
                i++;
                if (i >= trimmed.Length)
                    return false;

                if (trimmed[i] is '"' or '\'')
                {
                    var quote = trimmed[i];
                    i++;
                    var start = i;
                    while (i < trimmed.Length && trimmed[i] != quote)
                        i++;

                    if (i >= trimmed.Length)
                        return false;

                    var property = trimmed[start..i];
                    i++; // closing quote
                    if (i >= trimmed.Length || trimmed[i] != ']')
                        return false;
                    i++; // closing bracket
                    tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                    continue;
                }

                var indexStart = i;
                while (i < trimmed.Length && char.IsDigit(trimmed[i]))
                    i++;

                if (i == indexStart || i >= trimmed.Length || trimmed[i] != ']')
                    return false;

                if (!int.TryParse(trimmed[indexStart..i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
                    return false;

                i++; // closing bracket
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            return false;
        }

        return tokens.Count > 0;
    }

    private static bool TryParseSqlServerJsonModifyPath(
        string path,
        out List<JsonPathToken> tokens,
        out bool append,
        out bool strict)
    {
        tokens = [];
        append = false;
        strict = false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("append ", StringComparison.OrdinalIgnoreCase))
        {
            append = true;
            trimmed = trimmed[7..].TrimStart();
        }

        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            strict = true;

        return TryParseJsonPathTokens(trimmed, out tokens);
    }

    private static bool TryParseJsonNode(object json, out System.Text.Json.Nodes.JsonNode? node)
    {
        if (json is System.Text.Json.Nodes.JsonNode jsonNode)
        {
            node = jsonNode;
            return true;
        }

        var text = json.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            node = null;
            return false;
        }

        try
        {
            node = System.Text.Json.Nodes.JsonNode.Parse(text);
            return node is not null;
        }
        catch
        {
            node = null;
            return false;
        }
    }

    private static bool TryReadPostgresTextArray(object? value, out List<string> items)
    {
        items = [];
        if (IsNullish(value))
            return false;

        if (value is IEnumerable enumerable && value is not string)
        {
            foreach (var item in enumerable)
                items.Add(item?.ToString() ?? string.Empty);

            return true;
        }

        return false;
    }

    private static bool TryParsePostgresJsonPathTokens(object value, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (!TryReadPostgresTextArray(value, out var segments))
            return false;

        foreach (var segment in segments)
        {
            if (int.TryParse(segment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
                && index >= 0)
            {
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, segment, null));
        }

        return tokens.Count > 0;
    }

    private static System.Text.Json.Nodes.JsonNode CreateJsonNodeFromValue(object? value)
    {
        if (value is null or DBNull)
        {
            return System.Text.Json.Nodes.JsonValue.Create((string?)null)
                ?? System.Text.Json.Nodes.JsonNode.Parse("null")!;
        }

        if (value is System.Text.Json.JsonElement element)
            return System.Text.Json.Nodes.JsonNode.Parse(element.GetRawText())!;

        if (value is System.Text.Json.Nodes.JsonNode node)
            return node;

        return System.Text.Json.Nodes.JsonValue.Create(value)
            ?? System.Text.Json.Nodes.JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(value))!;
    }

    private static System.Text.Json.Nodes.JsonNode CreateJsonContainer(JsonPathToken nextToken)
        => nextToken.Kind == JsonPathTokenKind.ArrayIndex
            ? new System.Text.Json.Nodes.JsonArray()
            : new System.Text.Json.Nodes.JsonObject();

    private static bool TrySetJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        System.Text.Json.Nodes.JsonNode? current = root;
        System.Text.Json.Nodes.JsonNode? parent = null;
        JsonPathToken? parentToken = null;

        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            var isLast = i == tokens.Count - 1;

            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not System.Text.Json.Nodes.JsonObject obj)
                {
                    if (current is null or System.Text.Json.Nodes.JsonValue)
                    {
                        obj = new System.Text.Json.Nodes.JsonObject();
                        AssignJsonChild(ref root, parent, parentToken, obj);
                    }
                    else
                    {
                        return false;
                    }
                }

                if (isLast)
                {
                    obj[token.PropertyName!] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = obj[token.PropertyName!];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    obj[token.PropertyName!] = child;
                }

                parent = obj;
                parentToken = token;
                current = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not System.Text.Json.Nodes.JsonArray array)
                {
                    if (current is null or System.Text.Json.Nodes.JsonValue)
                    {
                        array = new System.Text.Json.Nodes.JsonArray();
                        AssignJsonChild(ref root, parent, parentToken, array);
                    }
                    else
                    {
                        return false;
                    }
                }

                var index = token.ArrayIndex ?? 0;
                while (array.Count <= index)
                    array.Add(null);

                if (isLast)
                {
                    array[index] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = array[index];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    array[index] = child;
                }

                parent = array;
                parentToken = token;
                current = child;
            }
        }

        return false;
    }

    private static bool TryGetJsonNodeAtPath(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        out System.Text.Json.Nodes.JsonNode? node)
    {
        node = root;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (node is not System.Text.Json.Nodes.JsonObject obj)
                {
                    node = null;
                    return false;
                }

                if (!obj.TryGetPropertyValue(token.PropertyName!, out var child))
                {
                    node = null;
                    return false;
                }

                node = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (node is not System.Text.Json.Nodes.JsonArray array)
                {
                    node = null;
                    return false;
                }

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                {
                    node = null;
                    return false;
                }

                node = array[index];
            }
        }

        return node is not null;
    }

    private static void AssignJsonChild(
        ref System.Text.Json.Nodes.JsonNode root,
        System.Text.Json.Nodes.JsonNode? parent,
        JsonPathToken? parentToken,
        System.Text.Json.Nodes.JsonNode child)
    {
        if (parent is null)
        {
            root = child;
            return;
        }

        if (parent is System.Text.Json.Nodes.JsonObject obj && parentToken?.Kind == JsonPathTokenKind.Property)
        {
            obj[parentToken.Value.PropertyName!] = child;
            return;
        }

        if (parent is System.Text.Json.Nodes.JsonArray array && parentToken?.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var index = parentToken.Value.ArrayIndex ?? 0;
            while (array.Count <= index)
                array.Add(null);
            array[index] = child;
        }
    }

    private static bool TryRemoveJsonPathValue(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens)
    {
        if (tokens.Count == 0)
            return false;

        System.Text.Json.Nodes.JsonNode? current = root;
        for (var i = 0; i < tokens.Count - 1; i++)
        {
            var token = tokens[i];
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not System.Text.Json.Nodes.JsonObject obj)
                    return true;

                current = obj[token.PropertyName!];
                if (current is null)
                    return true;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not System.Text.Json.Nodes.JsonArray array)
                    return true;

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                    return true;
                current = array[index];
                if (current is null)
                    return true;
            }
        }

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (current is not System.Text.Json.Nodes.JsonObject obj)
                return true;

            obj.Remove(lastToken.PropertyName!);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (current is not System.Text.Json.Nodes.JsonArray array)
                return true;

            var index = lastToken.ArrayIndex ?? 0;
            if (index < 0 || index >= array.Count)
                return true;

            array.RemoveAt(index);
            return true;
        }

        return true;
    }

    private static bool TryInsertJsonPathValue(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value,
        bool insertAfter)
    {
        if (tokens.Count == 0)
            return false;

        if (tokens.Count == 1)
        {
            var targetToken = tokens[0];
            if (targetToken.Kind == JsonPathTokenKind.Property && root is System.Text.Json.Nodes.JsonObject rootObject)
            {
                if (rootObject[targetToken.PropertyName!] is not null)
                    return true;

                rootObject[targetToken.PropertyName!] = CreateJsonNodeFromValue(value);
                return true;
            }

            if (targetToken.Kind == JsonPathTokenKind.ArrayIndex && root is System.Text.Json.Nodes.JsonArray rootArray)
            {
                var insertIndex = targetToken.ArrayIndex ?? 0;
                if (insertAfter)
                    insertIndex++;

                insertIndex = Math.Max(0, Math.Min(insertIndex, rootArray.Count));
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            return false;
        }

        var parentTokens = tokens.Take(tokens.Count - 1).ToList();
        if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is null)
            return false;

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (parent is not System.Text.Json.Nodes.JsonObject obj)
                return false;

            if (obj[lastToken.PropertyName!] is not null)
                return true;

            obj[lastToken.PropertyName!] = CreateJsonNodeFromValue(value);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (parent is not System.Text.Json.Nodes.JsonArray array)
                return false;

            var insertIndex = lastToken.ArrayIndex ?? 0;
            if (insertAfter)
                insertIndex++;

            insertIndex = Math.Max(0, Math.Min(insertIndex, array.Count));
            array.Insert(insertIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        return false;
    }

    private static bool TryEvalDateConstructionFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TIME", StringComparison.OrdinalIgnoreCase))
            || fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        for (int i = 1; i < fn.Args.Count; i++)
        {
            var modifier = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(modifier)
                || !TryParseDateModifier(modifier!, out var unit, out var amount))
            {
                continue;
            }

            dateTime = ApplyDateDelta(dateTime, unit, amount);
        }

        if (fn.Name.Equals("TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = dateTime.TimeOfDay;
            return true;
        }

        result = fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            ? dateTime.Date
            : dateTime;
        return true;
    }

    private static bool TryEvalFieldFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FIELD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var target = evalArg(0);
        if (IsNullish(target))
        {
            result = 0;
            return true;
        }

        for (int argIndex = 1; argIndex < fn.Args.Count; argIndex++)
        {
            var candidate = evalArg(argIndex);
            if (!IsNullish(candidate) && target.EqualsSql(candidate, dialect))
            {
                result = argIndex;
                return true;
            }
        }

        result = 0;
        return true;
    }

    private static bool TryEvalBasicStringFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);

        if (fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LCASE", StringComparison.OrdinalIgnoreCase))
        {
#pragma warning disable CA1308 // Normalize strings to uppercase
            result = IsNullish(value) ? null : value!.ToString()!.ToLowerInvariant();
#pragma warning restore CA1308 // Normalize strings to uppercase
            return true;
        }

        if (fn.Name.Equals("UPPER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UCASE", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.ToUpperInvariant();
            return true;
        }

        if (fn.Name.Equals("TRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.Trim();
            return true;
        }

        if (fn.Name.Equals("RTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.TrimEnd();
            return true;
        }

        if (fn.Name.Equals("LTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.TrimStart();
            return true;
        }

        if (fn.Name.Equals("TO_CHAR", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString() ?? string.Empty;
            return true;
        }

        if (fn.Name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("CHAR_LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("CHARACTER_LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LEN", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : (long)(value!.ToString()!.Length);
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalSubstringFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("MID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var source = evalArg(0);
        if (IsNullish(source))
        {
            result = null;
            return true;
        }

        var text = source!.ToString() ?? string.Empty;
        var position = evalArg(1);
        if (IsNullish(position))
        {
            result = null;
            return true;
        }

        var start = Convert.ToInt32(position.ToDec()) - 1;
        if (start < 0)
            start = 0;

        if (start >= text.Length)
        {
            result = string.Empty;
            return true;
        }

        var lengthValue = evalArg(2);
        if (IsNullish(lengthValue))
        {
            result = text[start..];
            return true;
        }

        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (start + length > text.Length)
            length = text.Length - start;

        result = text.Substring(start, length);
        return true;
    }

    private static bool TryEvalModFunction(
        FunctionCallExpr fn,
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
            if (r == 0)
            {
                result = null;
                return true;
            }

            result = l % r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMonthNameFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MONTHNAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = dateTime.ToString("MMMM", CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalOctFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("OCT", StringComparison.OrdinalIgnoreCase))
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
            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = Convert.ToString(number, 8);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalHexFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("HEX", StringComparison.OrdinalIgnoreCase))
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
            result = BytesToHex(bytes);
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = BytesToHex(System.Text.Encoding.UTF8.GetBytes(text));
        return true;
    }

    private static bool TryEvalUnhexFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("UNHEX", StringComparison.OrdinalIgnoreCase))
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
        try
        {
            if (text.Length % 2 != 0)
                text = "0" + text;

            var bytes = new byte[text.Length / 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(text.Substring(i * 2, 2), 16);
            }

            result = bytes;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOctetLengthFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("OCTET_LENGTH", StringComparison.OrdinalIgnoreCase))
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
        result = System.Text.Encoding.UTF8.GetByteCount(text);
        return true;
    }

    private static bool TryEvalNameConstFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("NAME_CONST", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var nameValue = evalArg(0);
        var value = evalArg(1);
        if (IsNullish(nameValue))
        {
            result = null;
            return true;
        }

        result = value;
        return true;
    }

    private static bool TryEvalOrdFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ORD", StringComparison.OrdinalIgnoreCase))
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
        if (text.Length == 0)
        {
            result = 0;
            return true;
        }

        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        result = bytes[0];
        return true;
    }

    private static bool TryEvalPositionFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("POSITION", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        if (needle.Length == 0)
        {
            result = 1;
            return true;
        }

        var index = haystack.IndexOf(needle, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalPiFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("PI", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = Math.PI;
        return true;
    }

    private static bool TryEvalPowerFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isPower = fn.Name.Equals("POWER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("POW", StringComparison.OrdinalIgnoreCase);
        if (!isPower)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("POWER() espera base e expoente.");

        var baseValue = evalArg(0);
        var expValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(expValue))
        {
            result = null;
            return true;
        }

        try
        {
            var baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            var expNumber = Convert.ToDouble(expValue, CultureInfo.InvariantCulture);
            result = Math.Pow(baseNumber, expNumber);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalPeriodFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isAdd = fn.Name.Equals("PERIOD_ADD", StringComparison.OrdinalIgnoreCase);
        var isDiff = fn.Name.Equals("PERIOD_DIFF", StringComparison.OrdinalIgnoreCase);
        if (!isAdd && !isDiff)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera dois argumentos.");

        var periodValue = evalArg(0);
        var secondValue = evalArg(1);
        if (IsNullish(periodValue) || IsNullish(secondValue))
        {
            result = null;
            return true;
        }

        if (!TryParsePeriodValue(periodValue!, out var year, out var month))
        {
            result = null;
            return true;
        }

        if (isAdd)
        {
            var delta = Convert.ToInt32(secondValue.ToDec());
            var totalMonths = year * 12 + (month - 1) + delta;
            var newYear = totalMonths / 12;
            var newMonth = (totalMonths % 12) + 1;
            result = newYear * 100 + newMonth;
            return true;
        }

        if (!TryParsePeriodValue(secondValue!, out var otherYear, out var otherMonth))
        {
            result = null;
            return true;
        }

        var diff = (year * 12 + (month - 1)) - (otherYear * 12 + (otherMonth - 1));
        result = diff;
        return true;
    }

    private static bool TryParsePeriodValue(object value, out int year, out int month)
    {
        year = 0;
        month = 0;

        try
        {
            var num = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            var abs = Math.Abs(num);
            year = abs / 100;
            month = abs % 100;
            if (month is < 1 or > 12)
                return false;

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryEvalQuarterFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("QUARTER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = ((dateTime.Month - 1) / 3) + 1;
        return true;
    }

    private static bool TryEvalQuoteFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = "NULL";
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var escaped = text.Replace("\\", "\\\\").Replace("'", "\\'");
        result = $"'{escaped}'";
        return true;
    }

    private static bool TryEvalSqlServerScalarFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        switch (name)
        {
            case "QUOTENAME":
            {
                var value = evalArg(0);
                if (IsNullish(value))
                {
                    result = null;
                    return true;
                }

                var text = value?.ToString() ?? string.Empty;
                var delimiter = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                var quoteChar = string.IsNullOrEmpty(delimiter) ? "[" : delimiter![0].ToString();
                var closingChar = quoteChar switch
                {
                    "[" => "]",
                    "(" => ")",
                    "<" => ">",
                    "{" => "}",
                    _ => quoteChar
                };
                var escaped = text.Replace(closingChar, closingChar + closingChar);
                result = quoteChar + escaped + closingChar;
                return true;
            }
            case "REPLICATE":
            {
                var textValue = evalArg(0);
                var countValue = evalArg(1);
                if (IsNullish(textValue) || IsNullish(countValue))
                {
                    result = null;
                    return true;
                }

                var text = textValue?.ToString() ?? string.Empty;
                var count = Convert.ToInt32(countValue.ToDec());
                if (count <= 0)
                {
                    result = string.Empty;
                    return true;
                }

                var sb = new StringBuilder(text.Length * count);
                for (var i = 0; i < count; i++)
                    sb.Append(text);
                result = sb.ToString();
                return true;
            }
            case "SQUARE":
            {
                var value = evalArg(0);
                if (IsNullish(value))
                {
                    result = null;
                    return true;
                }

                try
                {
                    var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                    result = number * number;
                    return true;
                }
                catch
                {
                    result = null;
                    return true;
                }
            }
            case "STUFF":
            {
                if (fn.Args.Count < 4)
                    throw new InvalidOperationException("STUFF() espera 4 argumentos.");

                var sourceValue = evalArg(0);
                var startValue = evalArg(1);
                var lengthValue = evalArg(2);
                var replaceValue = evalArg(3);
                if (IsNullish(sourceValue) || IsNullish(startValue) || IsNullish(lengthValue) || IsNullish(replaceValue))
                {
                    result = null;
                    return true;
                }

                var source = sourceValue?.ToString() ?? string.Empty;
                var start = Convert.ToInt32(startValue.ToDec());
                var length = Convert.ToInt32(lengthValue.ToDec());
                var replacement = replaceValue?.ToString() ?? string.Empty;
                if (start <= 0 || length < 0 || start > source.Length + 1)
                {
                    result = null;
                    return true;
                }

                var zeroBasedStart = start - 1;
                var safeLength = Math.Min(length, source.Length - zeroBasedStart);
                result = source.Remove(zeroBasedStart, safeLength).Insert(zeroBasedStart, replacement);
                return true;
            }
            case "PARSENAME":
            {
                var objectNameValue = evalArg(0);
                var pieceValue = evalArg(1);
                if (IsNullish(objectNameValue) || IsNullish(pieceValue))
                {
                    result = null;
                    return true;
                }

                var objectName = objectNameValue?.ToString() ?? string.Empty;
                var piece = Convert.ToInt32(pieceValue.ToDec());
                if (piece is < 1 or > 4)
                {
                    result = null;
                    return true;
                }

                var parts = objectName.Split('.');
                var indexFromEnd = piece - 1;
                result = indexFromEnd < parts.Length
                    ? parts[^(indexFromEnd + 1)]
                    : null;
                return true;
            }
            default:
                return false;
        }
    }

    private static bool TryEvalRadiansFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("RADIANS", StringComparison.OrdinalIgnoreCase))
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
            var degrees = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = degrees * (Math.PI / 180d);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalRandFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("RAND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var seedValue = fn.Args.Count > 0 ? evalArg(0) : null;
        double next;
        if (IsNullish(seedValue))
        {
            lock (_randomLock)
                next = _sharedRandom.NextDouble();
        }
        else
        {
            var seeded = new Random(Convert.ToInt32(seedValue.ToDec()));
            next = seeded.NextDouble();
        }

        result = next;
        return true;
    }

    private static bool TryEvalRepeatFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("REPEAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var countValue = evalArg(1);
        if (IsNullish(textValue) || IsNullish(countValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var count = Convert.ToInt32(countValue.ToDec());
        if (count <= 0)
        {
            result = string.Empty;
            return true;
        }

        var sb = new StringBuilder(text.Length * count);
        for (var i = 0; i < count; i++)
            sb.Append(text);
        result = sb.ToString();
        return true;
    }

    private static bool TryEvalReverseFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("REVERSE", StringComparison.OrdinalIgnoreCase))
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
        var chars = text.ToCharArray();
        Array.Reverse(chars);
        result = new string(chars);
        return true;
    }

    private static bool TryEvalLeftFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("LEFT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var lengthValue = evalArg(1);
        if (IsNullish(textValue) || IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (length >= text.Length)
        {
            result = text;
            return true;
        }

        result = text[..length];
        return true;
    }

    private static bool TryEvalRightFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("RIGHT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var lengthValue = evalArg(1);
        if (IsNullish(textValue) || IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (length >= text.Length)
        {
            result = text;
            return true;
        }

        result = text[^length..];
        return true;
    }

    private static bool TryEvalRoundFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ROUND", StringComparison.OrdinalIgnoreCase))
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

        var decimals = fn.Args.Count > 1 ? evalArg(1) : null;
        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (IsNullish(decimals))
            {
                result = Math.Round(number, 0, MidpointRounding.AwayFromZero);
                return true;
            }

            var digits = Convert.ToInt32(decimals.ToDec());
            result = Math.Round(number, digits, MidpointRounding.AwayFromZero);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalPadRightFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("RPAD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var lenValue = evalArg(1);
        var padValue = fn.Args.Count > 2 ? evalArg(2) : " ";

        if (IsNullish(value) || IsNullish(lenValue) || IsNullish(padValue))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var padText = padValue?.ToString() ?? string.Empty;
        var len = Convert.ToInt32(lenValue.ToDec());

        if (len < 0 || padText.Length == 0)
        {
            result = null;
            return true;
        }

        if (len == 0)
        {
            result = string.Empty;
            return true;
        }

        if (text.Length >= len)
        {
            result = text.Substring(0, len);
            return true;
        }

        var padNeeded = len - text.Length;
        var sb = new StringBuilder(len);
        sb.Append(text);
        while (sb.Length < len)
            sb.Append(padText);

        result = sb.ToString().Substring(0, len);
        return true;
    }

    private static bool TryEvalSecToTimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SEC_TO_TIME", StringComparison.OrdinalIgnoreCase))
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
            var seconds = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = TimeSpan.FromSeconds(seconds);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalShaFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("SHA", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SHA1", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SHA2", StringComparison.OrdinalIgnoreCase)))
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
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);

        if (name.Equals("SHA2", StringComparison.OrdinalIgnoreCase))
        {
            var lengthArg = fn.Args.Count > 1 ? evalArg(1) : null;
            var length = IsNullish(lengthArg) ? 256 : Convert.ToInt32(lengthArg.ToDec());
            byte[] hash = length switch
            {
                224 => ComputeHash(System.Security.Cryptography.SHA256.Create(), bytes),
                256 => ComputeHash(System.Security.Cryptography.SHA256.Create(), bytes),
                384 => ComputeHash(System.Security.Cryptography.SHA384.Create(), bytes),
                512 => ComputeHash(System.Security.Cryptography.SHA512.Create(), bytes),
                _ => ComputeHash(System.Security.Cryptography.SHA256.Create(), bytes)
            };

            result = BytesToHex(hash);
            return true;
        }

        using var sha1 = System.Security.Cryptography.SHA1.Create();
        var sha = ComputeHash(sha1, bytes);
        result = BytesToHex(sha);
        return true;
    }

    private static bool TryEvalSinFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SIN", StringComparison.OrdinalIgnoreCase))
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
            result = Math.Sin(radians);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSoundexFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SOUNDEX", StringComparison.OrdinalIgnoreCase))
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
        if (text.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var firstLetter = char.ToUpperInvariant(text[0]);
        var codes = new StringBuilder();

        char? lastCode = null;
        foreach (var ch in text.Skip(1))
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

        result = soundex.ToString();
        return true;
    }

    private static char? GetSoundexCode(char ch)
    {
        ch = char.ToUpperInvariant(ch);
        if (ch is 'A' or 'E' or 'I' or 'O' or 'U' or 'Y' or 'H' or 'W')
            return null;

        return ch switch
        {
            'B' or 'F' or 'P' or 'V' => '1',
            'C' or 'G' or 'J' or 'K' or 'Q' or 'S' or 'X' or 'Z' => '2',
            'D' or 'T' => '3',
            'L' => '4',
            'M' or 'N' => '5',
            'R' => '6',
            _ => null
        };
    }

    private static bool TryEvalSpaceFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SPACE", StringComparison.OrdinalIgnoreCase))
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

        var count = Convert.ToInt32(value.ToDec());
        if (count <= 0)
        {
            result = string.Empty;
            return true;
        }

        result = new string(' ', count);
        return true;
    }

    private static bool TryEvalSqrtFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SQRT", StringComparison.OrdinalIgnoreCase))
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
            if (number < 0)
            {
                result = null;
                return true;
            }

            result = Math.Sqrt(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private bool TryEvalSubDateFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SUBDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("SUBDATE() espera data e intervalo.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalExpr = fn.Args[1];
        if (intervalExpr is CallExpr intervalCall && intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
        {
            var intervalValue = ParseIntervalValue(intervalCall, row, group, ctes);
            if (intervalValue is null)
            {
                result = null;
                return true;
            }

            result = dateTime.Subtract(intervalValue.Span);
            return true;
        }

        var value = evalArg(1);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (TryConvertNumericToDouble(value, out var dayOffset))
        {
            result = dateTime.AddDays(-dayOffset);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalSubTimeFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SUBTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("SUBTIME() espera base e intervalo.");

        var baseValue = evalArg(0);
        var intervalValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(intervalValue))
        {
            result = null;
            return true;
        }

        if (baseValue is TimeSpan baseTimeSpan && TryCoerceTimeSpan(intervalValue, out var span))
        {
            result = baseTimeSpan.Subtract(span);
            return true;
        }

        if (baseValue is string baseText
            && LooksLikeTimeOnly(baseText)
            && TryCoerceTimeSpan(baseText, out var baseSpanText)
            && TryCoerceTimeSpan(intervalValue, out var spanText))
        {
            result = baseSpanText.Subtract(spanText);
            return true;
        }

        if (TryCoerceDateTime(baseValue, out var dateTime) && TryCoerceTimeSpan(intervalValue, out var spanDate))
        {
            result = dateTime.Subtract(spanDate);
            return true;
        }

        if (TryCoerceTimeSpan(baseValue, out var baseSpan) && TryCoerceTimeSpan(intervalValue, out var span2))
        {
            result = baseSpan.Subtract(span2);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalSubstringIndexFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SUBSTRING_INDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var delimValue = evalArg(1);
        var countValue = evalArg(2);
        if (IsNullish(textValue) || IsNullish(delimValue) || IsNullish(countValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var delim = delimValue?.ToString() ?? string.Empty;
        var count = Convert.ToInt32(countValue.ToDec());
        if (count == 0 || delim.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var parts = text.Split([delim], StringSplitOptions.None);
        if (Math.Abs(count) >= parts.Length)
        {
            result = text;
            return true;
        }

        if (count > 0)
        {
            result = string.Join(delim, parts.Take(count));
            return true;
        }

        var take = Math.Abs(count);
        result = string.Join(delim, parts.Skip(parts.Length - take));
        return true;
    }

    private static bool TryEvalTanFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TAN", StringComparison.OrdinalIgnoreCase))
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
            result = Math.Tan(radians);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTimeFormatFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIME_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var format = evalArg(1)?.ToString() ?? string.Empty;
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var isDateTime = TryCoerceDateTime(value, out var dateTime);
        var isTimeSpan = TryCoerceTimeSpan(value, out var timeSpan);
        if (!isDateTime && !isTimeSpan)
        {
            result = null;
            return true;
        }

        var formatNet = ConvertMySqlTimeFormat(format);
        var formatted = isDateTime
            ? dateTime.ToString(formatNet, CultureInfo.InvariantCulture)
            : DateTime.Today.Add(timeSpan).ToString(formatNet, CultureInfo.InvariantCulture);

        result = formatted;
        return true;
    }

    private static string ConvertMySqlTimeFormat(string format)
    {
        if (string.IsNullOrEmpty(format))
            return format;

        var sb = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                sb.Append(ch);
                continue;
            }

            var token = format[i + 1];
            i++;
            sb.Append(token switch
            {
                'H' => "HH",
                'k' => "H",
                'h' => "hh",
                'I' => "hh",
                'l' => "h",
                'i' => "mm",
                's' => "ss",
                'S' => "ss",
                'f' => "ffffff",
                'p' => "tt",
                'r' => "hh:mm:ss tt",
                'T' => "HH:mm:ss",
                _ => $"%{token}"
            });
        }

        return sb.ToString();
    }

    private static bool TryEvalTimeToSecFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIME_TO_SEC", StringComparison.OrdinalIgnoreCase))
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

        if (TryCoerceTimeSpan(value, out var span))
        {
            result = (long)span.TotalSeconds;
            return true;
        }

        if (TryCoerceDateTime(value, out var dateTime))
        {
            result = (long)dateTime.TimeOfDay.TotalSeconds;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalTimeDiffFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIMEDIFF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        if (TryCoerceDateTime(left, out var leftDate) && TryCoerceDateTime(right, out var rightDate))
        {
            result = leftDate - rightDate;
            return true;
        }

        if (TryCoerceTimeSpan(left, out var leftSpan) && TryCoerceTimeSpan(right, out var rightSpan))
        {
            result = leftSpan - rightSpan;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalSessionUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            ? "dbo"
            : null;
        return true;
    }

    private static bool TryEvalSystemUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            ? "sa"
            : "root@localhost";
        return true;
    }

    private static bool TryEvalToDaysFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TO_DAYS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var days = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
        result = days;
        return true;
    }

    private static bool TryEvalToSecondsFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TO_SECONDS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var seconds = (long)(dateTime.ToUniversalTime() - DateTime.MinValue.ToUniversalTime()).TotalSeconds + 1;
        result = seconds;
        return true;
    }

    private static bool TryEvalTruncateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TRUNCATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var decimalsValue = evalArg(1);
        if (IsNullish(value) || IsNullish(decimalsValue))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            var decimals = Convert.ToInt32(decimalsValue.ToDec());
            var factor = (decimal)Math.Pow(10d, decimals);
            result = Math.Truncate(number * factor) / factor;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalUnixTimestampFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("UNIX_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UNIXEPOCH", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = fn.Args.Count > 0 ? evalArg(0) : null;
        if (IsNullish(value))
        {
            result = (long)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            return true;
        }

        DateTime dateTime;
        if (value is string textValue)
        {
            if (!DateTime.TryParse(
                    textValue,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out dateTime))
            {
                result = null;
                return true;
            }
        }
        else if (!TryCoerceDateTime(value, out dateTime))
        {
            result = null;
            return true;
        }

        if (dateTime.Kind == DateTimeKind.Unspecified)
            dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);

        result = new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeSeconds();
        return true;
    }

    private static bool TryEvalUserFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "root@localhost";
        return true;
    }

    private static bool TryEvalUtcDateFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_DATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalUtcTimeFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalUtcTimestampFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow;
        return true;
    }

    private bool TryEvalUuidShortFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UUID_SHORT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count > 0)
            throw new InvalidOperationException("UUID_SHORT() não aceita argumentos.");

        var baseValue = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
        lock (_uuidShortCounterLock)
        {
            if (_uuidShortCounter < baseValue)
                _uuidShortCounter = baseValue;

            _uuidShortCounter++;
            result = _uuidShortCounter;
        }
        return true;
    }

    private static bool TryEvalWeekFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("WEEK", StringComparison.OrdinalIgnoreCase)
            || name.Equals("WEEKDAY", StringComparison.OrdinalIgnoreCase)
            || name.Equals("WEEKOFYEAR", StringComparison.OrdinalIgnoreCase)
            || name.Equals("YEARWEEK", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (name.Equals("WEEKDAY", StringComparison.OrdinalIgnoreCase))
        {
            var weekday = ((int)dateTime.DayOfWeek + 6) % 7;
            result = weekday;
            return true;
        }

        if (name.Equals("WEEKOFYEAR", StringComparison.OrdinalIgnoreCase))
        {
            var week = GetIsoWeekOfYear(dateTime);
            result = week;
            return true;
        }

        if (name.Equals("YEARWEEK", StringComparison.OrdinalIgnoreCase))
        {
            var week = GetIsoWeekOfYear(dateTime);
            var year = GetIsoWeekYear(dateTime);
            result = year * 100 + week;
            return true;
        }

        if (dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = GetIsoWeekOfYear(dateTime);
            return true;
        }

        // MySQL WEEK(date) default mode is 0: Sunday-first, range 0-53.
        var firstDayOfYear = new DateTime(dateTime.Year, 1, 1);
        var dayOffset = (int)firstDayOfYear.DayOfWeek;
        var dayOfYearZeroBased = dateTime.DayOfYear - 1;
        result = (dayOfYearZeroBased + dayOffset) / 7;
        return true;
    }

    private static string BytesToHex(byte[] hash)
    {
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    private static byte[] ComputeHash(System.Security.Cryptography.HashAlgorithm algorithm, byte[] bytes)
    {
        using (algorithm)
            return algorithm.ComputeHash(bytes);
    }

    private static int GetIsoWeekOfYear(DateTime dateTime)
    {
        var day = CultureInfo.InvariantCulture.Calendar.GetDayOfWeek(dateTime);
        if (day is DayOfWeek.Monday or DayOfWeek.Tuesday or DayOfWeek.Wednesday)
            dateTime = dateTime.AddDays(3);

        return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(
            dateTime,
            CalendarWeekRule.FirstFourDayWeek,
            DayOfWeek.Monday);
    }

    private static int GetIsoWeekYear(DateTime dateTime)
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
        if (!fn.Name.Equals("REPLACE", StringComparison.OrdinalIgnoreCase))
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

    private static string FlattenMatchAgainstTarget(object? value)
    {
        if (value is object?[] values)
            return string.Join(" ", values.Where(v => !IsNullish(v)).Select(v => v?.ToString() ?? string.Empty));

        return value?.ToString() ?? string.Empty;
    }

    private static IReadOnlyList<MatchAgainstTerm> ExtractMatchAgainstTerms(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        var matches = Regex.Matches(
            query,
            @"(?<sign>[+\-]?)(?:""(?<phrase>[^""]+)""|(?<term>[\p{L}\p{N}_*]+))",
            RegexOptions.CultureInvariant);

        return matches
            .Cast<Match>()
            .Select(m =>
            {
                var sign = m.Groups["sign"].Value;
                var phrase = m.Groups["phrase"].Value;
                var token = !string.IsNullOrWhiteSpace(phrase)
                    ? phrase
                    : m.Groups["term"].Value;

                var prefixWildcard = token.EndsWith("*", StringComparison.Ordinal);
                if (prefixWildcard)
                    token = token[..^1];

                return new MatchAgainstTerm(
                    token,
                    Required: sign == "+",
                    Prohibited: sign == "-",
                    PrefixWildcard: prefixWildcard,
                    IsPhrase: !string.IsNullOrWhiteSpace(phrase));
            })
            .Where(t => !string.IsNullOrWhiteSpace(t.Value))
            .Distinct()
            .ToArray();
    }

    private static IReadOnlyList<string> ExtractMatchAgainstWords(string haystack)
    {
        if (string.IsNullOrWhiteSpace(haystack))
            return [];

        return Regex.Matches(haystack, @"[\p{L}\p{N}_]+", RegexOptions.CultureInvariant)
            .Cast<Match>()
            .Select(m => m.Value)
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .ToArray();
    }

    private static bool ContainsMatchAgainstTerm(
        string haystack,
        IReadOnlyList<string> haystackWords,
        MatchAgainstTerm term,
        StringComparison comparison)
    {
        if (term.IsPhrase)
            return haystack.IndexOf(term.Value, comparison) >= 0;

        if (term.PrefixWildcard)
            return haystackWords.Any(word => word.StartsWith(term.Value, comparison));

        return haystackWords.Any(word => word.Equals(term.Value, comparison));
    }

    private readonly record struct MatchAgainstTerm(
        string Value,
        bool Required,
        bool Prohibited,
        bool PrefixWildcard,
        bool IsPhrase);

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
                if (DateTime.TryParse(value!.ToString(), culture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate))
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

                    using var _ = System.Text.Json.JsonDocument.Parse(normalizedJson);
                    return normalizedJson;
                }

                if (v is string s)
                    return ValidateJsonOrNull(s);

                if (v is System.Text.Json.JsonElement je)
                    return ValidateJsonOrNull(je.GetRawText());

                var serialized = System.Text.Json.JsonSerializer.Serialize(v);
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

    private object? TryEvalDateAddFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out bool handled)
    {
        handled = true;
        if (TryEvalDateAddStyleFunction(fn, row, group, ctes, evalArg, out var dateAddResult))
            return dateAddResult;

        if (TryEvalTimestampAddStyleFunction(fn, row, group, ctes, evalArg, out var timestampAddResult))
            return timestampAddResult;

        handled = false;
        return null;
    }

    private bool TryEvalDateAddStyleFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para operações de data.");
        if (!dialect.SupportsDateAddFunction("DATE_ADD"))
        {
            result = null;
            return true;
        }

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalExpression = fn.Args.Count > 1 ? fn.Args[1] : null;
        if (intervalExpression is not CallExpr intervalCall
            || !intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
            || intervalCall.Args.Count < 2)
        {
            result = dateTime;
            return true;
        }

        var amountObject = Eval(intervalCall.Args[0], row, group, ctes);
        var unit = intervalCall.Args[1] is RawSqlExpr raw
            ? raw.Sql
            : Eval(intervalCall.Args[1], row, group, ctes)?.ToString() ?? "DAY";
        result = ApplyDateDelta(dateTime, unit, Convert.ToInt32((amountObject ?? 0m).ToDec()));
        return true;
    }

    private bool TryEvalTimestampAddStyleFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var featureName = fn.Name.ToUpperInvariant();
        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para operações de data.");
        if (!dialect.SupportsDateAddFunction(featureName) || fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var baseValue = evalArg(2);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var amountObject = evalArg(1);
        result = ApplyDateDelta(dateTime, unit, Convert.ToInt32((amountObject ?? 0m).ToDec()));
        return true;
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

        if (!dialect.SupportsSqlServerScalarFunction(fn.Name))
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

    private bool TryEvalJsonExtractionFunction(
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
        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonExtractFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonQueryFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonValueFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");
    }

    private object? TryEvalJsonExtractionValue(FunctionCallExpr fn, object json, string path)
    {
        try
        {
            if (fn.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json, path, out var element))
                    return null;

                return element.ValueKind is System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array
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
        using var document = System.Text.Json.JsonDocument.Parse(json.ToString() ?? string.Empty);
        var root = document.RootElement;
        return root.ValueKind is System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }

    private static bool TryEvalOpenJsonFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.SupportsOpenJsonFunction)
            throw SqlUnsupported.ForDialect(dialect, "OPENJSON");

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

    private static bool TryEvalToNumberFunction(
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

    private static void LogFunctionEvaluationFailure(Exception exception)
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

    private static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
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

        return DateTime.TryParse(
            baseVal.ToString(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeLocal,
            out dt);
    }

    private static bool TryCoerceTimeSpan(object? baseVal, out TimeSpan span)
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

        if (TimeSpan.TryParse(baseVal.ToString(), CultureInfo.InvariantCulture, out var parsed))
        {
            span = parsed;
            return true;
        }

        if (DateTime.TryParse(baseVal.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            span = parsedDate.TimeOfDay;
            return true;
        }

        return false;
    }

    private static bool LooksLikeTimeOnly(string value)
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

    private static DateTime ApplyDateDelta(DateTime dt, string unit, int amount)
    {
        var normalized = unit.Trim().ToUpperInvariant();
        return normalized switch
        {
            "YEAR" or "YEARS" or "YY" or "YYYY" => dt.AddYears(amount),
            "MONTH" or "MONTHS" or "MM" => dt.AddMonths(amount),
            "DAY" or "DAYS" or "DD" or "D" => dt.AddDays(amount),
            "HOUR" or "HOURS" or "HH" => dt.AddHours(amount),
            "MINUTE" or "MINUTES" or "MI" or "N" => dt.AddMinutes(amount),
            "SECOND" or "SECONDS" or "SS" or "S" => dt.AddSeconds(amount),
            _ => dt
        };
    }

    private static bool TryParseDateModifier(string modifier, out string unit, out int amount)
    {
        unit = string.Empty;
        amount = 0;

        var match = _dateModifierRegex.Match(modifier.Trim());
        if (!match.Success)
            return false;

        if (!int.TryParse(match.Groups["amount"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        unit = match.Groups["unit"].Value;
        return !string.IsNullOrWhiteSpace(unit);
    }

    private object? EvalCall(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Aggregate?
        if (group is not null && _aggFns.Contains(fn.Name))
            return EvalAggregate(fn, group, ctes);

        if (fn.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return ParseIntervalValue(fn, row, group, ctes);

        // se não for agregado, trata como função "normal" reaproveitando EvalFunction
        // (Distinct em função escalar não faz sentido aqui, então ignoramos)
        var shim = new FunctionCallExpr(fn.Name, fn.Args);
        return EvalFunction(shim, row, group, ctes);
    }

    private static bool IsNullish(object? v) => v is null || v is DBNull;

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

    private sealed record IntervalValue(TimeSpan Span);

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
        out string unit)
    {
        value = 0m;
        unit = string.Empty;

        if (fn.Args.Count < 2)
            return false;

        unit = TryGetUnitText(fn.Args[1], row, group, ctes);
        if (string.IsNullOrWhiteSpace(unit))
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

    private static bool TryEvalTruncFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TRUNC", StringComparison.OrdinalIgnoreCase)
            || fn.Args.Count < 1)
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

        if (value is DateTime or DateTimeOffset)
        {
            TryCoerceDateTime(value, out var dateTime);
            result = dateTime.Date;
            return true;
        }

        try
        {
            var dec = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            result = Math.Truncate(dec);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalJulianDayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JULIANDAY", StringComparison.OrdinalIgnoreCase)
            || fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var julianDay = dateTime.ToOADate() + 2415018.5d;
        result = julianDay;
        return true;
    }

    private bool TryEvalExtractFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase)
            || fn.Args.Count < 2)
        {
            result = null;
            return false;
        }

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var value = evalArg(1);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (TryCoerceDateTime(value, out var dateTime))
        {
            result = unit switch
            {
                "DAY" or "DAYS" => dateTime.Day,
                "MONTH" or "MONTHS" => dateTime.Month,
                "YEAR" or "YEARS" => dateTime.Year,
                "HOUR" or "HOURS" => dateTime.Hour,
                "MINUTE" or "MINUTES" => dateTime.Minute,
                "SECOND" or "SECONDS" => dateTime.Second,
                _ => null
            };
            return true;
        }

        if (TryConvertNumericToDouble(value, out var numeric))
        {
            result = unit switch
            {
                "DAY" or "DAYS" => (int)Math.Truncate(numeric),
                _ => null
            };
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalDaysFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DAYS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 1)
            throw new InvalidOperationException("DAYS() espera 1 argumento.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var days = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
        result = days;
        return true;
    }

    private bool TryEvalTimestampDiffFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIMESTAMPDIFF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 3)
            throw new InvalidOperationException("TIMESTAMPDIFF() espera 3 argumentos.");

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);

        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            "DAY" or "DAYS" => (int)(end.Date - start.Date).TotalDays,
            "HOUR" or "HOURS" => (int)(end - start).TotalHours,
            "MINUTE" or "MINUTES" => (int)(end - start).TotalMinutes,
            "SECOND" or "SECONDS" => (int)(end - start).TotalSeconds,
            "MONTH" or "MONTHS" => DiffMonths(start, end),
            "YEAR" or "YEARS" => DiffYears(start, end),
            _ => null
        };

        return true;
    }

    private bool TryEvalDateDiffFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para operações de data.");

        if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count != 2)
                throw new InvalidOperationException("DATEDIFF() no MySQL espera 2 argumentos.");

            var startValue = evalArg(0);
            var endValue = evalArg(1);
            if (IsNullish(startValue) || IsNullish(endValue)
                || !TryCoerceDateTime(startValue, out var start)
                || !TryCoerceDateTime(endValue, out var end))
            {
                result = null;
                return true;
            }

            result = (int)(end.Date - start.Date).TotalDays;
            return true;
        }

        if (fn.Args.Count != 3)
            throw new InvalidOperationException("DATEDIFF() espera 3 argumentos.");

        var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
        var startValue = evalArg(1);
        var endValue = evalArg(2);

        if (IsNullish(startValue) || IsNullish(endValue)
            || !TryCoerceDateTime(startValue, out var start)
            || !TryCoerceDateTime(endValue, out var end))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            "DAY" or "DAYS" => (int)(end.Date - start.Date).TotalDays,
            "HOUR" or "HOURS" => (int)(end - start).TotalHours,
            "MINUTE" or "MINUTES" => (int)(end - start).TotalMinutes,
            "SECOND" or "SECONDS" => (int)(end - start).TotalSeconds,
            "MONTH" or "MONTHS" => DiffMonths(start, end),
            "YEAR" or "YEARS" => DiffYears(start, end),
            _ => null
        };

        return true;
    }

    private static int DiffMonths(DateTime start, DateTime end)
    {
        var months = (end.Year - start.Year) * 12 + (end.Month - start.Month);
        if (end.Day < start.Day)
            months -= 1;
        return months;
    }

    private static int DiffYears(DateTime start, DateTime end)
    {
        var years = end.Year - start.Year;
        if (end.Month < start.Month || (end.Month == start.Month && end.Day < start.Day))
            years -= 1;
        return years;
    }

    private string TryGetUnitText(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => GetDateAddUnit(expr, row, group, ctes);

    private static bool TryParseIntervalLiteral(string raw, out decimal value, out string unit)
    {
        value = 0;
        unit = string.Empty;

        var normalized = raw.Trim();
        if (normalized.Contains('\\'))
            normalized = normalized.Replace("\\", string.Empty);

        var match = _intervalLiteralRegex.Match(normalized);
        if (!match.Success)
            return false;

        if (!decimal.TryParse(match.Groups["num"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out value))
            return false;

        unit = match.Groups["unit"].Value.ToUpperInvariant();
        return true;
    }

    private static TimeSpan? TryConvertIntervalToTimeSpan(decimal value, string unit)
        => unit switch
        {
            "DAY" or "DAYS" => TimeSpan.FromDays((double)value),
            "HOUR" or "HOURS" => TimeSpan.FromHours((double)value),
            "MINUTE" or "MINUTES" => TimeSpan.FromMinutes((double)value),
            "SECOND" or "SECONDS" => TimeSpan.FromSeconds((double)value),
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
            if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                && (dialect.Version < 56 || dialect.Version >= 84)
                && name == "JSON_OBJECTAGG")
            {
                throw SqlUnsupported.ForDialect(dialect, name);
            }

            return EvalJsonGroupObjectAggregate(fn, group, ctes);
        }

        if (name is "JSON_OBJECT_AGG" or "JSON_OBJECT_AGG_STRICT" or "JSON_OBJECT_AGG_UNIQUE" or "JSON_OBJECT_AGG_UNIQUE_STRICT"
            or "JSONB_OBJECT_AGG" or "JSONB_OBJECT_AGG_STRICT" or "JSONB_OBJECT_AGG_UNIQUE" or "JSONB_OBJECT_AGG_UNIQUE_STRICT")
            return EvalJsonGroupObjectAggregate(fn, group, ctes);

        if (name is "CORR" or "CORR_K" or "CORR_S" or "COVAR_POP" or "COVAR_SAMP")
            return EvalCorrelationAggregate(fn, group, ctes, name);

        if (name is "GROUP_ID")
            return 0;

        if (name.StartsWith("APPROX_", StringComparison.OrdinalIgnoreCase))
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            if (!dialect.SupportsApproximateAggregateFunction(name))
                throw SqlUnsupported.ForDialect(dialect, name);

            return EvalApproxAggregate(fn, group, ctes, name);
        }

        if (name.StartsWith("REGR_", StringComparison.OrdinalIgnoreCase))
            return EvalRegressionAggregate(fn, group, ctes, name);

        if (name.StartsWith("STATS_", StringComparison.OrdinalIgnoreCase))
            return null;

        if (name is "STD" or "STDDEV" or "STDDEV_POP" or "STDDEV_SAMP")
        {
            var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
            if (dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                && (name is "STD" or "STDDEV_POP")
                && dialect.Version >= 84)
            {
                throw SqlUnsupported.ForDialect(dialect, name);
            }

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
            if (!dialect.SupportsSqlServerAggregateFunction(name))
                throw SqlUnsupported.ForDialect(dialect, name);
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
            "VAR_SAMP" => AggregateVariance(values, sample: true),
            "CV" => AggregateCoefficientOfVariation(values),
            _ => null
        };
    }

    private static int? AggregateChecksumValues(IReadOnlyList<object?> values, bool binary)
    {
        var filtered = values.Where(static value => !IsNullish(value)).ToArray();
        if (filtered.Length == 0)
            return null;

        var hash = new HashCode();
        foreach (var value in filtered)
        {
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

        return hash.ToHashCode();
    }

    private static object? AggregateTotal(IReadOnlyList<object?> values)
    {
        var numeric = values
            .Where(static value => !IsNullish(value))
            .Select(static value => Convert.ToDouble(value, CultureInfo.InvariantCulture))
            .ToArray();

        if (numeric.Length == 0)
            return 0d;

        return numeric.Sum();
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

        var values = new List<double>();
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

        var pairs = new List<(double X, double Y)>();
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

        var pairs = new List<(double X, double Y)>();
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

        var numeric = values
            .Where(static value => !IsNullish(value))
            .Select(static value => Convert.ToDouble(value, CultureInfo.InvariantCulture))
            .ToArray();

        if (numeric.Length == 0)
            return null;

        var mean = numeric.Average();
        var sum = numeric.Sum(v => Math.Pow(v - mean, 2d));
        var denominator = name == "STDDEV_SAMP" ? numeric.Length - 1 : numeric.Length;
        if (denominator <= 0)
            return null;

        var variance = sum / denominator;
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
        var filtered = values.Where(static value => !IsNullish(value)).ToList();
        if (filtered.Count == 0)
            return null;

        var acc = Convert.ToInt64(filtered[0], CultureInfo.InvariantCulture);
        for (var i = 1; i < filtered.Count; i++)
        {
            var next = Convert.ToInt64(filtered[i], CultureInfo.InvariantCulture);
            acc = operation switch
            {
                BitwiseAggregateOperation.And => acc & next,
                BitwiseAggregateOperation.Or => acc | next,
                BitwiseAggregateOperation.Xor => acc ^ next,
                _ => acc
            };
        }

        return acc;
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

        return values.Where(static value => !IsNullish(value)).ToArray();
    }

    private static object? AggregateVariance(IReadOnlyList<object?> values, bool sample)
    {
        var numeric = values
            .Where(static value => !IsNullish(value))
            .Select(static value => Convert.ToDouble(value, CultureInfo.InvariantCulture))
            .ToArray();

        if (numeric.Length == 0)
            return null;

        if (sample && numeric.Length < 2)
            return null;

        var mean = numeric.Average();
        var sumSq = numeric.Sum(v =>
        {
            var diff = v - mean;
            return diff * diff;
        });

        var divisor = sample ? numeric.Length - 1 : numeric.Length;
        return sumSq / divisor;
    }

    private static object? AggregateCoefficientOfVariation(IReadOnlyList<object?> values)
    {
        var numeric = values
            .Where(static value => !IsNullish(value))
            .Select(static value => Convert.ToDouble(value, CultureInfo.InvariantCulture))
            .ToArray();

        if (numeric.Length == 0)
            return null;

        var mean = numeric.Average();
        if (mean == 0d)
            return null;

        var variance = numeric.Sum(v =>
        {
            var diff = v - mean;
            return diff * diff;
        }) / numeric.Length;

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

        var useDouble = values.Any(static value => value is float or double);
        if (useDouble)
        {
            var numericValues = values.Select(static value => Convert.ToDouble(value, CultureInfo.InvariantCulture)).ToArray();
            return operation switch
            {
                AggregateNumericOperation.Sum => numericValues.Sum(),
                AggregateNumericOperation.Average => numericValues.Average(),
                AggregateNumericOperation.Min => numericValues.Min(),
                AggregateNumericOperation.Max => numericValues.Max(),
                _ => null
            };
        }

        var decimalValues = values.Select(static value => value!.ToDec()).ToArray();
        return operation switch
        {
            AggregateNumericOperation.Sum => decimalValues.Sum(),
            AggregateNumericOperation.Average => decimalValues.Sum() / decimalValues.Length,
            AggregateNumericOperation.Min => decimalValues.Min(),
            AggregateNumericOperation.Max => decimalValues.Max(),
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

        // COUNT(DISTINCT ...)
        if (name == "COUNT" && fn.Distinct)
            return EvalCountDistinct(fn, filteredGroup, ctes);

        if (name is "GROUP_CONCAT" or "STRING_AGG" or "LISTAGG")
        {
            if (!dialect.SupportsStringAggregateFunction(name))
                throw SqlUnsupported.ForDialect(dialect, name);

            return EvalStringAggregateForCallExpr(fn, filteredGroup, ctes, name);
        }

        // para os outros casos (sem DISTINCT), reaproveita o existente
        var shim = new FunctionCallExpr(fn.Name, fn.Args);
        return EvalAggregate(shim, filteredGroup, ctes);
    }

    private EvalGroup ApplyAggregateFilter(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Filter is null)
            return group;

        var filteredRows = group.Rows
            .Where(row => Eval(fn.Filter, row, null, ctes).ToBool())
            .ToList();
        return new EvalGroup(filteredRows);
    }

    private long EvalCountDistinct(CallExpr fn, EvalGroup group, IDictionary<string, Source> ctes)
    {
        // COUNT(DISTINCT *) não faz sentido no MySQL; se acontecer, trata como COUNT(*)
        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
            return group.Rows.Count;

        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in group.Rows)
        {
            if (TryBuildCountDistinctKey(fn, row, ctes, out var key))
                set.Add(key);
        }

        return set.Count;
    }

    private bool TryBuildCountDistinctKey(
        CallExpr fn,
        EvalRow row,
        IDictionary<string, Source> ctes,
        out string key)
    {
        key = string.Empty;
        var builder = new StringBuilder();

        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = Eval(fn.Args[i], row, null, ctes);
            if (IsNullish(value))
                return false;

            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(NormalizeDistinctKey(value, Dialect));
        }

        key = builder.ToString();
        return true;
    }

    private static string NormalizeDistinctKey(object? v, ISqlDialect? dialect = null)
        => QueryRowValueHelper.NormalizeDistinctKey(v, dialect);

    private static string? EvalStringAggregate(IReadOnlyList<object?> values, object? separatorObj, string defaultSeparator)
    {
        if (values.Count == 0)
            return null;

        var separator = separatorObj?.ToString() ?? defaultSeparator;
        return string.Join(separator, values.Select(v => v?.ToString() ?? string.Empty));
    }

    private string? EvalStringAggregateForCallExpr(CallExpr fn, EvalGroup group, IDictionary<string, Source> ctes, string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var separator = GetAggregateSeparator(fn, group, ctes);
        var rows = GetStringAggregateRows(fn, group, ctes);
        var values = CollectStringAggregateValues(fn, rows, ctes);
        return EvalStringAggregate(values, separator, GetStringAggregateDefaultSeparator(name));
    }

    private object? GetAggregateSeparator(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
        => fn.Args.Count > 1 && group.Rows.Count > 0
            ? Eval(fn.Args[1], group.Rows[0], null, ctes)
            : null;

    private IEnumerable<EvalRow> GetStringAggregateRows(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var orderBy = fn.WithinGroupOrderBy;
        if (orderBy is not { Count: > 0 })
            return group.Rows;

        return group.Rows.OrderBy(
            row => EvaluateWithinGroupOrderKey(orderBy, row, ctes),
            CreateWithinGroupOrderKeyComparer(orderBy));
    }

    private object?[] EvaluateWithinGroupOrderKey(
        IReadOnlyList<WindowOrderItem> orderBy,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => orderBy
            .Select(order => Eval(order.Expr, row, null, ctes))
            .ToArray();

    private IComparer<object?[]> CreateWithinGroupOrderKeyComparer(IReadOnlyList<WindowOrderItem> orderBy)
        => Comparer<object?[]>.Create((left, right) =>
        {
            for (var i = 0; i < orderBy.Count; i++)
            {
                var comparison = CompareSql(left[i], right[i]);
                if (comparison == 0)
                    continue;

                return orderBy[i].Desc ? -comparison : comparison;
            }

            return 0;
        });

    private List<object?> CollectStringAggregateValues(
        CallExpr fn,
        IEnumerable<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        var values = new List<object?>();
        HashSet<string>? distinct = fn.Distinct ? new HashSet<string>(StringComparer.Ordinal) : null;

        foreach (var row in rows)
        {
            var value = Eval(fn.Args[0], row, null, ctes);
            if (IsNullish(value))
                continue;

            if (distinct is null || distinct.Add(NormalizeDistinctKey(value, Dialect)))
                values.Add(value);
        }

        return values;
    }

    private static string GetStringAggregateDefaultSeparator(string name)
        => name == "LISTAGG" ? string.Empty : ",";

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

        var values = new List<object?>();
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
        var looksAggregatedOutsideSubqueries = LooksLikeAggregateExpression(exprRaw)
            || ContainsAggregateFunctionName(exprRaw);
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
            return LooksLikeAggregateExpression(exprRaw)
                || ContainsAggregateFunctionName(exprRaw);
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    private static bool ContainsAggregateFunctionName(string exprRaw)
    {
        var sanitized = AggregateExpressionInspector.RemoveSubqueryBodies(exprRaw);
        if (string.IsNullOrWhiteSpace(sanitized))
            return false;

        foreach (var fn in _aggFns)
        {
            if (sanitized.IndexOf(fn + "(", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }

        return false;
    }

    private static bool LooksLikeAggregateExpression(string exprRaw)
        => AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);

    private static bool WalkHasAggregate(SqlExpr expr)
        => AggregateExpressionInspector.WalkHasAggregate(expr);

    // ---------------- RESOLUTION HELPERS ----------------

    private object? ResolveParam(
        string name)
        => QueryRowValueHelper.ResolveParam(_pars, name);

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
        HashSet<string> MissingForcedIndexes);

    internal sealed class Source
    {
        internal ITableMock? Physical { get; }
        private readonly TableResultMock? _result;
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
        private Source(string name, string alias, ITableMock physical, IReadOnlyList<SqlMySqlIndexHint>? mySqlIndexHints = null)
        {
            Alias = alias;
            Name = name;
            Physical = physical;
            _result = null;
            ColumnNames = [.. physical.Columns.OrderBy(kv => kv.Value.Index).Select(kv => kv.Key!)];
            MySqlIndexHints = mySqlIndexHints ?? [];
        }
        private Source(string name, string alias, TableResultMock result)
        {
            Alias = alias;
            Name = name;
            _result = result;
            Physical = null;
            ColumnNames = [.. result.Columns.OrderBy(c => c.ColumIndex).Select(c => c.ColumnAlias)];
            MySqlIndexHints = [];
        }
        /// <summary>
        /// EN: Implements WithAlias.
        /// PT: Implementa WithAlias.
        /// </summary>
        public Source WithAlias(string alias)
        {
            if (Physical is not null)
                return FromPhysical(Name, alias, Physical, MySqlIndexHints);
            return FromResult(Name, alias, _result!);
        }

        internal bool TryGetColumnMetadata(string columnName, out TableResultColMock metadata)
        {
            metadata = null!;
            if (_result is null)
                return false;

            var matched = _result.Columns.FirstOrDefault(column =>
                column.ColumnAlias.Equals(columnName, StringComparison.OrdinalIgnoreCase)
                || column.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (matched is null)
                return false;

            metadata = matched;
            return true;
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
                    var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    foreach (var col in ColumnNames)
                    {
                        var idx = Physical.Columns[col].Index;
                        dict[$"{Alias}.{col}"] = row?.TryGetValue(idx, out var v) == true
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
                    var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < _result.Columns.Count; i++)
                    {
                        var c = _result.Columns[i];
                        dict[$"{Alias}.{c.ColumnAlias}"] = row.TryGetValue(i, out var v)
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
                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (var col in ColumnNames)
                {
                    var idx = Physical.Columns[col].Index;
                    dict[$"{Alias}.{col}"] = row.TryGetValue(idx, out var v)
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
        public static Source FromPhysical(string tableName, string alias, ITableMock physical, IReadOnlyList<SqlMySqlIndexHint>? mySqlIndexHints = null)
            => new(tableName, alias, physical, mySqlIndexHints);
       
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

    private readonly record struct InMembershipState(bool Matched, bool HasNullCandidate);
    private enum SqlTruthValue { True, False, Unknown }

    internal sealed record EvalRow(
        Dictionary<string, object?> Fields,
        Dictionary<string, Source> Sources)
    {
        /// <summary>
        /// EN: Implements FromProjected.
        /// PT: Implementa FromProjected.
        /// </summary>
        public static EvalRow FromProjected(
            TableResultMock res,
            Dictionary<int, object?> row,
            Dictionary<string, int> aliasToIndex)
        {
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in aliasToIndex)
            {
                fields[kv.Key] = row.TryGetValue(kv.Value, out var v) ? v : null;
            }
            return new EvalRow(fields, new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));
        }

        /// <summary>
        /// EN: Implements CloneRow.
        /// PT: Implementa CloneRow.
        /// </summary>
        public EvalRow CloneRow()
            => new(new Dictionary<string, object?>(Fields, StringComparer.OrdinalIgnoreCase),
                   new Dictionary<string, Source>(Sources, StringComparer.OrdinalIgnoreCase));

        /// <summary>
        /// EN: Returns an empty evaluation row placeholder.
        /// PT: Retorna um placeholder de linha de avaliação vazia.
        /// </summary>
        public static EvalRow Empty()
            => new(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                   new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase));

        /// <summary>
        /// EN: Implements AddSource.
        /// PT: Implementa AddSource.
        /// </summary>
        public void AddSource(Source src) => Sources[src.Alias] = src;

        /// <summary>
        /// EN: Implements AddFields.
        /// PT: Implementa AddFields.
        /// </summary>
        public void AddFields(Dictionary<string, object?> fields)
        {
            foreach (var it in fields)
                Fields[it.Key] = it.Value;

            // also expose unqualified columns (first wins) for convenience
            foreach (var it in fields)
            {
                var dot = it.Key.IndexOf('.');
                if (dot > 0)
                {
                    var col = it.Key[(dot + 1)..];
                    if (!Fields.ContainsKey(col))
                        Fields[col] = it.Value;
                }
            }
        }


        /// <summary>
        /// EN: Gets a field value by qualified or unqualified column name.
        /// PT: Obtém o valor de um campo por nome de coluna qualificado ou não qualificado.
        /// </summary>
        /// <param name="columnName">EN: Column name to read. PT: Nome da coluna a ler.</param>
        /// <returns>EN: The field value when present; otherwise null. PT: O valor do campo quando presente; caso contrário, null.</returns>
        public object? GetByName(string columnName)
        {
            if (Fields.TryGetValue(columnName, out var direct))
                return direct;

            var hit = Fields.FirstOrDefault(kv => kv.Key.EndsWith($".{columnName}", StringComparison.OrdinalIgnoreCase));
            return hit.Equals(default(KeyValuePair<string, object?>)) ? null : hit.Value;
        }
    }

    private static EvalRow AttachOuterRow(
        EvalRow inner,
        EvalRow outer)
    {
        // inner vence sempre (regra: o que o subselect produziu não deve ser sobrescrito)
        var merged = inner.CloneRow();

        // 1) Fields do outer entram só se não existirem no inner
        foreach (var it in outer.Fields)
        {
            if (!merged.Fields.ContainsKey(it.Key))
                merged.Fields[it.Key] = it.Value;
        }

        // 2) Também expõe colunas não qualificadas do outer (sem sobrescrever)
        foreach (var it in outer.Fields)
        {
            var dot = it.Key.IndexOf('.');
            if (dot > 0)
            {
                var col = it.Key[(dot + 1)..];
                if (!merged.Fields.ContainsKey(col))
                    merged.Fields[col] = it.Value;
            }
        }

        // 3) Sources: não sobrescreve alias do inner
        foreach (var it in outer.Sources)
        {
            if (!merged.Sources.ContainsKey(it.Key))
                merged.Sources[it.Key] = it.Value;
        }

        return merged;
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
           && dialect.SupportsLastFoundRowsIdentifier(identifier);

    private static bool IsFoundRowsEquivalentFunction(string functionName, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return dialect.SupportsLastFoundRowsFunction(functionName);
    }

    private bool HasSqlCalcFoundRows(SqlSelectQuery query)
        => Dialect?.SupportsSqlCalcFoundRowsModifier == true
           && !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}
