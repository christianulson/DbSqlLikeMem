using DbSqlLikeMem.Interfaces;
using System.Diagnostics;
using DbSqlLikeMem.Models;
using System.Text;

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
        @"\b(?:FROM|JOIN)\s+(?:[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?([A-Z_][A-Z0-9_$]*)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _simpleAliasTokenRegex = new(
        @"^[A-Z_][A-Z0-9_$]*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _aggregateExpressionRegex = new(
        @"\b(COUNT|SUM|MIN|MAX|AVG|GROUP_CONCAT|STRING_AGG|LISTAGG)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _dateModifierRegex = new(
        @"^(?<amount>[+-]?\d+)\s*(?<unit>\w+)s?$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex _intervalLiteralRegex = new(
        @"^(?<num>-?\d+(?:\.\d+)?)\s*(?<unit>[a-zA-Z]+)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

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
        "COUNT","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG"
    };
    private static readonly HashSet<string> _sqlAliasReservedTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","JOIN","INNER","LEFT","RIGHT","FULL","CROSS","ON","WHERE","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","UNION","ALL","AS","USING","WHEN","THEN","ELSE","END"
    };

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // Resolution is delegated to a shared resolver so this base stays focused on query execution.
    private SqlExpr ParseExpr(string raw)
    {
        var dialectInstance = (object)(Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para parse de expressão."));
        return SqlExpressionParserResolver.ParseWhere(raw, dialectInstance);
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
            FormatUnionInputsDebugDetails(parts, allFlags));

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

            ValidateUnionColumnTypes(result.Columns, tables[i].Columns, i, sqlContextForErrors, dialect);
        }

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
            FormatUnionCombineDebugDetails(parts, allFlags));

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

    private sealed class SqlRowDictionaryComparer(ISqlDialect dialect)
        : IEqualityComparer<Dictionary<int, object?>>
    {
        public bool Equals(Dictionary<int, object?>? x, Dictionary<int, object?>? y)
        {
            if (ReferenceEquals(x, y))
                return true;
            if (x is null || y is null || x.Count != y.Count)
                return false;

            foreach (var item in x)
            {
                if (!y.TryGetValue(item.Key, out var rightValue))
                    return false;

                if (!item.Value.EqualsSql(rightValue, dialect))
                    return false;
            }

            return true;
        }

        public int GetHashCode(Dictionary<int, object?> row)
        {
            var hash = new HashCode();
            foreach (var key in row.Keys.OrderBy(k => k))
            {
                hash.Add(key);
                hash.Add(NormalizeHash(row[key]));
            }

            return hash.ToHashCode();
        }

        private object? NormalizeHash(object? value)
        {
            if (value is null || value is DBNull)
                return null;

            if (TryNormalizeNumericHash(value, out var numericHash))
                return numericHash;

            if (value is string text)
                return dialect.TextComparison == StringComparison.OrdinalIgnoreCase
                    ? text.ToUpperInvariant()
                    : text;

            return value;
        }

        private bool TryNormalizeNumericHash(object value, out string normalized)
        {
            normalized = string.Empty;

            if (TryGetNumericValue(value, out var numeric))
            {
                normalized = numeric.ToString("G29", CultureInfo.InvariantCulture);
                return true;
            }

            if (!dialect.SupportsImplicitNumericStringComparison)
                return false;

            if (value is string text
                && decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                normalized = parsed.ToString("G29", CultureInfo.InvariantCulture);
                return true;
            }

            return false;
        }

        private static bool TryGetNumericValue(object value, out decimal numeric)
        {
            switch (value)
            {
                case byte b:
                    numeric = b;
                    return true;
                case short s:
                    numeric = s;
                    return true;
                case int i:
                    numeric = i;
                    return true;
                case long l:
                    numeric = l;
                    return true;
                case float f:
                    numeric = (decimal)f;
                    return true;
                case double d:
                    numeric = (decimal)d;
                    return true;
                case decimal m:
                    numeric = m;
                    return true;
                default:
                    numeric = default;
                    return false;
            }
        }
    }

    private static void ValidateUnionColumnTypes(
        IList<TableResultColMock> expected,
        IList<TableResultColMock> current,
        int currentIndex,
        string? sqlContextForErrors,
        ISqlDialect dialect)
    {
        for (int i = 0; i < expected.Count; i++)
        {
            if (dialect.AreUnionColumnTypesCompatible(expected[i].DbType, current[i].DbType))
                continue;

            var msg =
                "UNION: tipo de coluna incompatível. " +
                $"Coluna[{i}] Primeiro={expected[i].DbType}, SELECT[{currentIndex}]={current[i].DbType}.";
            if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                msg += "\nSQL: " + sqlContextForErrors;

            throw new InvalidOperationException(msg);
        }
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
        var planWarnings = BuildPlanWarnings(q, metrics);
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
    {
        return new SqlPlanMockRuntimeContext(
            _cnn.SimulatedLatencyMs,
            _cnn.DropProbability,
            _cnn.Db.ThreadSafe);
    }


    private static IReadOnlyList<SqlPlanWarning> BuildPlanWarnings(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        const long HighReadThreshold = 100;
        const long VeryHighReadThreshold = 1000;
        const long CriticalReadThreshold = 5000;
        const double LowSelectivityThresholdPct = 60d;
        const double VeryLowSelectivityThresholdPct = 85d;

        if (metrics.EstimatedRowsRead < HighReadThreshold)
            return [];

        var warnings = new List<SqlPlanWarning>();

        static bool HasTopPrefixInProjection(SqlSelectQuery q)
        {
            if (Regex.IsMatch(
                q.RawSql,
                @"^\s*SELECT\s+(?:DISTINCT\s+)?TOP\s*(\(\s*\d+\s*\)|\d+)\b",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
                return true;

            return q.SelectItems.Any(i => Regex.IsMatch(
                i.Raw,
                @"^\s*TOP\s*(\(\s*\d+\s*\)|\d+)\b",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant));
        }

        if (query.OrderBy.Count > 0 && query.RowLimit is null && !HasTopPrefixInProjection(query))
        {
            warnings.Add(new SqlPlanWarning(
                "PW001",
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitMessage(),
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitAction(),
                SqlPlanWarningSeverity.High,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold))));
        }

        if (metrics.SelectivityPct >= LowSelectivityThresholdPct)
        {
            var severity = metrics.SelectivityPct >= VeryLowSelectivityThresholdPct
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningLowSelectivityHighImpactMessage()
                : SqlExecutionPlanMessages.WarningLowSelectivityMessage();

            warnings.Add(new SqlPlanWarning(
                "PW002",
                message,
                SqlExecutionPlanMessages.WarningLowSelectivityReason(metrics.SelectivityPct, metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningLowSelectivityAction(),
                severity,
                "SelectivityPct",
                metrics.SelectivityPct.ToString("F2", CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", LowSelectivityThresholdPct), ("highImpactGte", VeryLowSelectivityThresholdPct))));
        }

        if (HasSelectStar(query))
        {
            var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
                ? SqlPlanWarningSeverity.High
                : metrics.EstimatedRowsRead >= VeryHighReadThreshold
                    ? SqlPlanWarningSeverity.Warning
                    : SqlPlanWarningSeverity.Info;

            var message = severity switch
            {
                SqlPlanWarningSeverity.High => SqlExecutionPlanMessages.WarningSelectStarCriticalImpactMessage(),
                SqlPlanWarningSeverity.Warning => SqlExecutionPlanMessages.WarningSelectStarHighImpactMessage(),
                _ => SqlExecutionPlanMessages.WarningSelectStarMessage()
            };

            warnings.Add(new SqlPlanWarning(
                "PW003",
                message,
                SqlExecutionPlanMessages.WarningSelectStarReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningSelectStarAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("warningGte", VeryHighReadThreshold), ("highGte", CriticalReadThreshold))));
        }

        if (query.Where is null && !query.Distinct)
        {
            var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningNoWhereHighReadHighImpactMessage()
                : SqlExecutionPlanMessages.WarningNoWhereHighReadMessage();

            warnings.Add(new SqlPlanWarning(
                "PW004",
                message,
                SqlExecutionPlanMessages.WarningNoWhereHighReadReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningNoWhereHighReadAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold))));
        }


        if (query.Distinct)
        {
            var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningDistinctHighReadHighImpactMessage()
                : SqlExecutionPlanMessages.WarningDistinctHighReadMessage();

            warnings.Add(new SqlPlanWarning(
                "PW005",
                message,
                SqlExecutionPlanMessages.WarningDistinctHighReadReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningDistinctHighReadAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold))));
        }

        return warnings;
    }

    private static bool HasSelectStar(SqlSelectQuery query)
        => query.SelectItems.Any(static item => string.Equals(item.Raw?.Trim(), "*", StringComparison.Ordinal));


    private static string BuildTechnicalThreshold(params (string Key, IFormattable Value)[] values)
    {
        if (values.Length == 0)
            return string.Empty;

        return string.Join(";", values.Select(static v =>
        {
            if (string.IsNullOrWhiteSpace(v.Key))
                throw new ArgumentException("Threshold key must be provided.", nameof(values));

            return $"{v.Key}:{v.Value.ToString(null, CultureInfo.InvariantCulture)}";
        }));
    }
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

        return source.Name ?? "<unknown_table>";
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
        => source.Name is not null && source.Derived is null && source.DerivedUnion is null;

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
            var res = ExecuteSelect(cte.Query, ctes, outerRow);
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
                    $"Join({j.Type.ToString().ToUpperInvariant()})",
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
            FormatProjectDebugDetails(selectQuery.SelectItems));

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
                FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            _cnn.SetLastFoundRows(projected.Count);

        // 7) ORDER BY / LIMIT
        projected = ApplyOrderAndLimit(projected, selectQuery, ctes, debugTrace);
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
                FormatGroupDebugDetails(q));
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
        var rightSrc = ResolveSource(join.Table, ctes);


        if (rightSrc.Physical is not null)
        {
            var hintPlan = BuildMySqlIndexHintPlan(join.Table.MySqlIndexHints, rightSrc.Physical, hasOrderBy, hasGroupBy);
            if (hintPlan?.MissingForcedIndexes.Count > 0)
                throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");
        }

        // FULL is not MySQL; accept as INNER for test/mock purposes
        var jt = //join.Type == SqlJoinType.Full ? SqlJoinType.Inner : 
            join.Type;

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
        IDictionary<string, Source> ctes)
    {
        var alias = ts.Alias ?? ts.Name ?? ts.DbName ?? "t";

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
            return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
        }

        if (ts.Derived is not null)
        {
            var res = ExecuteSelect(ts.Derived);
            source = Source.FromResult(alias, res);
            return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
        }

        if (!string.IsNullOrWhiteSpace(ts.Name)
            && ctes.TryGetValue(ts.Name!, out var cteSrc))
        {
            source = cteSrc.WithAlias(alias);
            return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
        }

        if (string.IsNullOrWhiteSpace(ts.Name))
            throw new InvalidOperationException("FROM sem nome de tabela/CTE/derived não suportado.");

        var tableName = ts.Name!.NormalizeName();

        if (_cnn.TryGetView(tableName, out var viewSelect, ts.DbName)
            && viewSelect != null)
        {
            var viewRes = ExecuteSelect(viewSelect, ctes, outerRow: null);
            source = Source.FromResult(alias, viewRes);
            return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
        }

        if (tableName.Equals("DUAL", StringComparison.OrdinalIgnoreCase))
        {
            var one = new TableResultMock
            {
                ([])
            };
            source = Source.FromResult("DUAL", alias, one);
            return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
        }

        _cnn.Metrics.IncrementTableHint(tableName);
        var tb = _cnn.GetTable(tableName, ts.DbName);
        source = Source.FromPhysical(tableName, alias, tb, ts.MySqlIndexHints);
        return ApplyPivotIfNeeded(source, ts.Pivot, ctes);
    }

    private Source ApplyPivotIfNeeded(Source source, SqlPivotSpec? pivot, IDictionary<string, Source> ctes)
    {
        if (pivot is null)
            return source;

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para PIVOT.");
        var inputRows = source.Rows()
            .Select(fields =>
            {
                var rowSources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
                {
                    [source.Alias] = source,
                    [source.Name] = source
                };
                return new EvalRow(new Dictionary<string, object?>(fields, StringComparer.OrdinalIgnoreCase), rowSources);
            })
            .ToList();

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
            result.Columns.Add(new TableResultColMock(source.Alias, groupColumns[i], groupColumns[i], i, DbType.Object, true));

        for (int i = 0; i < inItems.Count; i++)
            result.Columns.Add(new TableResultColMock(source.Alias, inItems[i].Alias, inItems[i].Alias, groupColumns.Count + i, DbType.Object, true));

        foreach (var group in grouped)
        {
            var first = group.First();
            var outRow = new Dictionary<int, object?>();

            for (int i = 0; i < groupColumns.Count; i++)
                outRow[i] = first.GetByName(groupColumns[i]);

            for (int i = 0; i < inItems.Count; i++)
            {
                var bucket = group.Where(r => forValues[r].EqualsSql(inItems[i].Value, dialect)).ToList();
                outRow[groupColumns.Count + i] = AggregatePivotBucket(pivot.AggregateFunction, aggArgExpr, bucket, ctes);
            }

            result.Add(outRow);
        }

        return Source.FromResult(source.Name, source.Alias, result);
    }

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

        if (aggregateFunction.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("AVG", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("MIN", StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals("MAX", StringComparison.OrdinalIgnoreCase))
        {
            var group = new EvalGroup(rows);
            var aggregateExpr = new FunctionCallExpr(aggregateFunction, [aggArgExpr]);
            return EvalAggregate(aggregateExpr, group, ctes);
        }

        throw new NotSupportedException($"PIVOT aggregate '{aggregateFunction}' not supported yet.");
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
                FormatDistinctDebugDetails(q.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(q))
            _cnn.SetLastFoundRows(res.Count);

        // ORDER / LIMIT
        debugTrace?.AddStep(
            "Project",
            groupsList.Count,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            FormatProjectDebugDetails(q.SelectItems));
        res = ApplyOrderAndLimit(res, q, ctes, debugTrace);
        return res;
    }

    private sealed class SelectPlan
    {
        /// <summary>
        /// EN: Gets or sets Columns.
        /// PT: Obtém ou define Columns.
        /// </summary>
        public required List<TableResultColMock> Columns { get; init; }
        /// <summary>
        /// EN: Gets or sets Evaluators.
        /// PT: Obtém ou define Evaluators.
        /// </summary>
        public required List<Func<EvalRow, EvalGroup?, object?>> Evaluators { get; init; }

        // Window functions computed over the current rowset (e.g. ROW_NUMBER() OVER (...))
        /// <summary>
        /// EN: Gets or sets WindowSlots.
        /// PT: Obtém ou define WindowSlots.
        /// </summary>
        public required List<WindowSlot> WindowSlots { get; init; }
    }

    private sealed class WindowSlot
    {
        /// <summary>
        /// EN: Gets or sets Expr.
        /// PT: Obtém ou define Expr.
        /// </summary>
        public required WindowFunctionExpr Expr { get; init; }
        /// <summary>
        /// EN: Gets or sets Map.
        /// PT: Obtém ou define Map.
        /// </summary>
        public required Dictionary<EvalRow, object?> Map { get; init; }
    }

    private sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
        where T : class
    {
        /// <summary>
        /// EN: Implements new.
        /// PT: Implementa new.
        /// </summary>
        public static readonly ReferenceEqualityComparer<T> Instance = new();
        /// <summary>
        /// EN: Implements Equals.
        /// PT: Implementa Equals.
        /// </summary>
        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);
        /// <summary>
        /// EN: Implements GetHashCode.
        /// PT: Implementa GetHashCode.
        /// </summary>
        public int GetHashCode(T obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }


    private static string MakeUniqueAlias(
        List<TableResultColMock> cols,
        string preferred,
        string tableAlias)
    {
        if (!cols.Any(c => c.ColumnAlias.Equals(preferred, StringComparison.OrdinalIgnoreCase)))
            return preferred;

        var alt = $"{tableAlias}_{preferred}";
        if (!cols.Any(c => c.ColumnAlias.Equals(alt, StringComparison.OrdinalIgnoreCase)))
            return alt;

        int n = 2;
        while (true)
        {
            var a = $"{alt}_{n}";
            if (!cols.Any(c => c.ColumnAlias.Equals(a, StringComparison.OrdinalIgnoreCase)))
                return a;
            n++;
        }
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

            // Partitioning
            var partitions = new Dictionary<string, List<EvalRow>>(StringComparer.Ordinal);

            foreach (var r in rows)
            {
                string key;

                if (w.Spec.PartitionBy.Count == 0)
                {
                    key = "__all__";
                }
                else
                {
                    var parts = w.Spec.PartitionBy
                        .Select(e => NormalizeDistinctKey(Eval(e, r, null, ctes)))
                        .ToArray();
                    key = string.Join("\u001F", parts);
                }

                if (!partitions.TryGetValue(key, out var list))
                {
                    list = [];
                    partitions[key] = list;
                }
                list.Add(r);
            }

            foreach (var part in partitions.Values)
            {
                // ORDER BY inside OVER
                if (w.Spec.OrderBy.Count > 0)
                {
                    part.Sort((a, b) =>
                    {
                        foreach (var oi in w.Spec.OrderBy)
                        {
                            var av = Eval(oi.Expr, a, null, ctes);
                            var bv = Eval(oi.Expr, b, null, ctes);

                            var cmp = CompareSql(av, bv);
                            if (cmp != 0)
                                return oi.Desc ? -cmp : cmp;
                        }
                        return 0;
                    });
                }

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
            return ResolveRowsFrameRange(frame, part.Count, rowIndex);

        if (orderBy.Count == 0)
            throw new InvalidOperationException($"Window frame unit '{frame.Unit}' requires ORDER BY in OVER clause.");

        var orderValuesByRow = BuildWindowOrderValuesByRow(part, orderBy, ctes);
        return frame.Unit switch
        {
            WindowFrameUnit.Groups => ResolveGroupsFrameRange(frame, part, rowIndex, orderValuesByRow),
            WindowFrameUnit.Range => ResolveRangeFrameRange(frame, part, rowIndex, orderValuesByRow, orderBy),
            _ => ResolveRowsFrameRange(frame, part.Count, rowIndex)
        };
    }

    /// <summary>
    /// EN: Resolves row index boundaries for a ROWS window frame for the current row.
    /// PT: Resolve os limites de índice de linha para um frame ROWS da janela na linha atual.
    /// </summary>
    private static RowsFrameRange ResolveRowsFrameRange(WindowFrameSpec? frame, int partitionSize, int rowIndex)
    {
        if (partitionSize <= 0)
            return RowsFrameRange.Empty;

        if (frame is null)
            return new RowsFrameRange(0, partitionSize - 1, IsEmpty: false);

        var lastIndex = partitionSize - 1;
        var rawStartIndex = ResolveRowsFrameBoundIndex(frame.Start, rowIndex, partitionSize, isStartBound: true);
        var rawEndIndex = ResolveRowsFrameBoundIndex(frame.End, rowIndex, partitionSize, isStartBound: false);

        if (rawStartIndex > rawEndIndex)
            return RowsFrameRange.Empty;

        var startIndex = Math.Max(rawStartIndex, 0);
        var endIndex = Math.Min(rawEndIndex, lastIndex);
        if (startIndex > endIndex)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(startIndex, endIndex, IsEmpty: false);
    }

    private static int ResolveRowsFrameBoundIndex(WindowFrameBound bound, int rowIndex, int partitionSize, bool isStartBound)
    {
        var lastIndex = partitionSize - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => lastIndex,
            WindowFrameBoundKind.CurrentRow => rowIndex,
            WindowFrameBoundKind.Preceding => rowIndex - bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.Following => rowIndex + bound.Offset.GetValueOrDefault(),
            _ => isStartBound ? 0 : lastIndex
        };
    }

    private readonly record struct RowsFrameRange(int StartIndex, int EndIndex, bool IsEmpty)
    {
        public static RowsFrameRange Empty => new(0, -1, IsEmpty: true);
    }

    /// <summary>
    /// EN: Resolves GROUPS frame bounds using peer groups derived from ORDER BY values.
    /// PT: Resolve limites de frame GROUPS usando grupos de peers derivados dos valores de ORDER BY.
    /// </summary>
    private RowsFrameRange ResolveGroupsFrameRange(
        WindowFrameSpec frame,
        List<EvalRow> part,
        int rowIndex,
        Dictionary<EvalRow, object?[]> orderValuesByRow)
    {
        var groups = BuildPeerGroups(part, orderValuesByRow);
        var currentGroupIndex = groups.FindIndex(g => rowIndex >= g.Start && rowIndex <= g.End);
        if (currentGroupIndex < 0)
            return RowsFrameRange.Empty;

        var startGroup = ResolveGroupsBoundIndex(frame.Start, currentGroupIndex, groups.Count, isStartBound: true);
        var endGroup = ResolveGroupsBoundIndex(frame.End, currentGroupIndex, groups.Count, isStartBound: false);
        if (startGroup > endGroup)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(groups[startGroup].Start, groups[endGroup].End, IsEmpty: false);
    }

    /// <summary>
    /// EN: Resolves RANGE frame bounds for ORDER BY using peer-aware and offset-aware range semantics.
    /// PT: Resolve limites de frame RANGE no ORDER BY com semântica de range por peers e offsets.
    /// </summary>
    private RowsFrameRange ResolveRangeFrameRange(
        WindowFrameSpec frame,
        List<EvalRow> part,
        int rowIndex,
        Dictionary<EvalRow, object?[]> orderValuesByRow,
        IReadOnlyList<WindowOrderItem> orderBy)
    {
        var hasOffsetBound = frame.Start.Kind is WindowFrameBoundKind.Preceding or WindowFrameBoundKind.Following
            || frame.End.Kind is WindowFrameBoundKind.Preceding or WindowFrameBoundKind.Following;

        ValidateRangeOffsetOrderBy(orderBy, hasOffsetBound);

        var peerRange = ResolvePeerRange(part, rowIndex, orderValuesByRow);

        int startIndex;
        int endIndex;
        if (hasOffsetBound)
        {
            var scalarValues = BuildRangeScalarValues(part, orderValuesByRow, orderBy);
            var current = scalarValues[rowIndex];
            startIndex = ResolveRangeBoundIndex(frame.Start, scalarValues, current, peerRange, isStartBound: true);
            endIndex = ResolveRangeBoundIndex(frame.End, scalarValues, current, peerRange, isStartBound: false);
        }
        else
        {
            startIndex = ResolveRangeBoundIndexWithoutOffsets(frame.Start, part.Count, peerRange, isStartBound: true);
            endIndex = ResolveRangeBoundIndexWithoutOffsets(frame.End, part.Count, peerRange, isStartBound: false);
        }

        if (startIndex > endIndex)
            return RowsFrameRange.Empty;

        return new RowsFrameRange(startIndex, endIndex, IsEmpty: false);
    }

    /// <summary>
    /// EN: Validates ORDER BY shape required by RANGE offset semantics.
    /// PT: Valida o formato de ORDER BY exigido pela semântica de RANGE com offset.
    /// </summary>
    private static void ValidateRangeOffsetOrderBy(IReadOnlyList<WindowOrderItem> orderBy, bool hasOffsetBound)
    {
        if (!hasOffsetBound)
            return;

        if (orderBy.Count != 1)
            throw new InvalidOperationException("RANGE with PRECEDING/FOLLOWING offset requires exactly one ORDER BY expression.");
    }

    private List<(int Start, int End)> BuildPeerGroups(List<EvalRow> part, Dictionary<EvalRow, object?[]> orderValuesByRow)
    {
        var groups = new List<(int Start, int End)>();
        var start = 0;
        for (var i = 1; i <= part.Count; i++)
        {
            var isBoundary = i == part.Count || !WindowOrderValuesEqual(orderValuesByRow[part[i - 1]], orderValuesByRow[part[i]]);
            if (!isBoundary)
                continue;

            groups.Add((start, i - 1));
            start = i;
        }

        return groups;
    }

    private static int ResolveGroupsBoundIndex(WindowFrameBound bound, int currentGroupIndex, int groupCount, bool isStartBound)
    {
        var last = groupCount - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => currentGroupIndex,
            WindowFrameBoundKind.Preceding => (currentGroupIndex - bound.Offset.GetValueOrDefault()).Clamp(0, last),
            WindowFrameBoundKind.Following => (currentGroupIndex + bound.Offset.GetValueOrDefault()).Clamp(0, last),
            _ => isStartBound ? 0 : last
        };
    }

    private (int Start, int End) ResolvePeerRange(List<EvalRow> part, int rowIndex, Dictionary<EvalRow, object?[]> orderValuesByRow)
    {
        var current = orderValuesByRow[part[rowIndex]];
        var start = rowIndex;
        while (start > 0 && WindowOrderValuesEqual(orderValuesByRow[part[start - 1]], current))
            start--;
        var end = rowIndex;
        while (end < part.Count - 1 && WindowOrderValuesEqual(orderValuesByRow[part[end + 1]], current))
            end++;
        return (start, end);
    }

    private static decimal[] BuildRangeScalarValues(List<EvalRow> part, Dictionary<EvalRow, object?[]> orderValuesByRow, IReadOnlyList<WindowOrderItem> orderBy)
    {
        var desc = orderBy.Count > 0 && orderBy[0].Desc;
        var values = new decimal[part.Count];
        for (var i = 0; i < part.Count; i++)
        {
            var rawOrderValue = orderValuesByRow[part[i]].Length == 0 ? null : orderValuesByRow[part[i]][0];
            if (!TryConvertRangeOrderToDecimal(rawOrderValue, out var scalar))
            {
                var valueType = rawOrderValue?.GetType().Name ?? "NULL";
                throw new InvalidOperationException($"RANGE with PRECEDING/FOLLOWING offset requires numeric/date ORDER BY values. Actual ORDER BY value type: {valueType}.");
            }

            values[i] = desc ? -scalar : scalar;
        }

        return values;
    }

    private static bool TryConvertRangeOrderToDecimal(object? value, out decimal scalar)
    {
        scalar = default;
        if (value is null || value is DBNull)
            return false;
        try
        {
            scalar = value switch
            {
                DateTime dt => dt.Ticks,
                DateTimeOffset dto => dto.Ticks,
                TimeSpan ts => ts.Ticks,
                _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture)
            };
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// EN: Resolves RANGE bounds for non-offset variants (CURRENT ROW/UNBOUNDED) using peer range only.
    /// PT: Resolve limites de RANGE sem offset (CURRENT ROW/UNBOUNDED) usando apenas o intervalo de peers.
    /// </summary>
    private static int ResolveRangeBoundIndexWithoutOffsets(
        WindowFrameBound bound,
        int partitionSize,
        (int Start, int End) peerRange,
        bool isStartBound)
    {
        var last = partitionSize - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => isStartBound ? peerRange.Start : peerRange.End,
            _ => isStartBound ? 0 : last
        };
    }

    private static int ResolveRangeBoundIndex(
        WindowFrameBound bound,
        decimal[] scalarValues,
        decimal currentScalar,
        (int Start, int End) peerRange,
        bool isStartBound)
    {
        var last = scalarValues.Length - 1;
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => 0,
            WindowFrameBoundKind.UnboundedFollowing => last,
            WindowFrameBoundKind.CurrentRow => isStartBound ? peerRange.Start : peerRange.End,
            WindowFrameBoundKind.Preceding => isStartBound
                ? FirstIndexGreaterOrEqual(scalarValues, currentScalar - bound.Offset.GetValueOrDefault())
                : LastIndexLessOrEqual(scalarValues, currentScalar - bound.Offset.GetValueOrDefault()),
            WindowFrameBoundKind.Following => isStartBound
                ? FirstIndexGreaterOrEqual(scalarValues, currentScalar + bound.Offset.GetValueOrDefault())
                : LastIndexLessOrEqual(scalarValues, currentScalar + bound.Offset.GetValueOrDefault()),
            _ => isStartBound ? 0 : last
        };
    }

    private static int FirstIndexGreaterOrEqual(decimal[] sortedValues, decimal threshold)
    {
        for (var i = 0; i < sortedValues.Length; i++)
            if (sortedValues[i] >= threshold)
                return i;
        return sortedValues.Length;
    }

    private static int LastIndexLessOrEqual(decimal[] sortedValues, decimal threshold)
    {
        for (var i = sortedValues.Length - 1; i >= 0; i--)
            if (sortedValues[i] <= threshold)
                return i;
        return -1;
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

        var orderValuesByRow = BuildWindowOrderValuesByRow(part, windowFunctionExpr.Spec.OrderBy, ctes);

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = ResolveWindowFrameRange(windowFunctionExpr.Spec.Frame, part, i, windowFunctionExpr.Spec.OrderBy, ctes);
            if (frameRange.IsEmpty || !RowsFrameContainsRow(frameRange, i))
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
                if (prevValues is not null && !WindowOrderValuesEqual(prevValues, frameValues))
                {
                    rank = (frameIndex - frameRange.StartIndex) + 1;
                    denseRank++;
                }

                if (WindowOrderValuesEqual(frameValues, currentValues))
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

        var orderValuesByRow = BuildWindowOrderValuesByRow(part, orderBy, ctes);

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var row = part[rowIndex];
            var frameRange = ResolveWindowFrameRange(frame, part, rowIndex, orderBy, ctes);
            if (frameRange.IsEmpty || !RowsFrameContainsRow(frameRange, rowIndex))
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
                if (WindowOrderValuesEqual(frameValues, currentValues))
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
            if (frameRange.IsEmpty || !RowsFrameContainsRow(frameRange, rowIndex))
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
    /// EN: Checks whether the current row index is visible inside the resolved ROWS frame.
    /// PT: Verifica se o índice da linha atual está visível dentro do frame ROWS resolvido.
    /// </summary>
    private static bool RowsFrameContainsRow(RowsFrameRange frameRange, int rowIndex)
        => rowIndex >= frameRange.StartIndex && rowIndex <= frameRange.EndIndex;

    /// <summary>
    /// EN: Builds evaluated ORDER BY values for each row in the ordered partition.
    /// PT: Constrói os valores de ORDER BY avaliados para cada linha da partição ordenada.
    /// </summary>
    private Dictionary<EvalRow, object?[]> BuildWindowOrderValuesByRow(
        List<EvalRow> part,
        IReadOnlyList<WindowOrderItem> orderBy,
        IDictionary<string, Source> ctes)
    {
        var orderValuesByRow = new Dictionary<EvalRow, object?[]>(ReferenceEqualityComparer<EvalRow>.Instance);
        foreach (var row in part)
        {
            orderValuesByRow[row] = orderBy
                .Select(oi => Eval(oi.Expr, row, null, ctes))
                .ToArray();
        }

        return orderValuesByRow;
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
    private bool WindowOrderValuesEqual(
        object?[] left,
        object?[] right)
    {
        if (left.Length != right.Length)
            return false;

        for (int i = 0; i < left.Length; i++)
        {
            var cmp = CompareSql(left[i], right[i]);
            if (cmp != 0)
                return false;
        }

        return true;
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
        var cols = new List<TableResultColMock>();
        var evals = new List<Func<EvalRow, EvalGroup?, object?>>();

        var windowSlots = new List<WindowSlot>();

        var sampleFirst = sampleRows.FirstOrDefault();

        foreach (var si in q.SelectItems)
        {
            Console.WriteLine($"[SELECT ITEM RAW] '{si.Raw}'  Alias='{si.Alias}'");
            var raw0 = si.Raw.Trim();

            // ✅ separa alias mesmo se o parser não preencheu si.Alias
            var (raw, asAlias) = SplitTrailingAsAlias(raw0, si.Alias);

            // Expand SELECT *
            if (!ExpandSelectAsterisc(cols, evals, sampleFirst, raw))
                continue;

            if (!IncludExtraColumns(sampleRows, cols, evals, raw))
                continue;

            var exprAst = ParseSelectPlanExpression(raw0, raw, asAlias, si.Alias);
            AppendSelectPlanProjection(
                q,
                sampleRows,
                ctes,
                cols,
                evals,
                windowSlots,
                raw,
                si.Alias,
                asAlias,
                exprAst);
        }

#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine("RESULT COLUMNS:");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        foreach (var c in cols)
            Console.WriteLine($" - {c.ColumnAlias}");

        return new SelectPlan { Columns = cols, Evaluators = evals, WindowSlots = windowSlots };
    }

    private SqlExpr ParseSelectPlanExpression(
        string rawInput,
        string rawExpression,
        string? extractedAlias,
        string? selectItemAlias)
    {
#pragma warning disable CA1031
        try
        {
            return ParseExpr(rawExpression);
        }
        catch (Exception e)
        {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{GetType().Name}.{nameof(BuildSelectPlan)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"[SELECT-ITEM] Raw0='{rawInput}' RawExpr='{rawExpression}' AliasParsed='{extractedAlias ?? "null"}' AliasSi='{selectItemAlias ?? "null"}'");
            Console.WriteLine(e);
            return new RawSqlExpr(rawExpression);
        }
#pragma warning restore CA1031
    }

    private void AppendSelectPlanProjection(
        SqlSelectQuery query,
        List<EvalRow> sampleRows,
        IDictionary<string, Source> ctes,
        List<TableResultColMock> cols,
        List<Func<EvalRow, EvalGroup?, object?>> evals,
        List<WindowSlot> windowSlots,
        string rawExpression,
        string? selectItemAlias,
        string? extractedAlias,
        SqlExpr expression)
    {
        var tableAlias = GetSelectProjectionTableAlias(query);
        var preferredAlias = selectItemAlias ?? extractedAlias ?? InferColumnAlias(rawExpression);
        var columnAlias = MakeUniqueAlias(cols, preferredAlias, tableAlias);
        var inferredDbType = InferDbTypeFromExpression(expression, sampleRows, ctes);

        cols.Add(CreateSelectPlanColumn(tableAlias, columnAlias, cols.Count, inferredDbType));
        evals.Add(CreateSelectPlanEvaluator(expression, ctes, windowSlots));
    }

    private static string GetSelectProjectionTableAlias(SqlSelectQuery query)
        => query.Table?.Alias ?? query.Table?.Name ?? string.Empty;

    private static TableResultColMock CreateSelectPlanColumn(
        string tableAlias,
        string columnAlias,
        int columnIndex,
        DbType dbType)
        => new(
            tableAlias: tableAlias,
            columnAlias: columnAlias,
            columnName: columnAlias,
            columIndex: columnIndex,
            dbType: dbType,
            isNullable: true);

    private Func<EvalRow, EvalGroup?, object?> CreateSelectPlanEvaluator(
        SqlExpr expression,
        IDictionary<string, Source> ctes,
        List<WindowSlot> windowSlots)
    {
        if (expression is not WindowFunctionExpr windowFunction)
            return (row, group) => Eval(expression, row, group, ctes);

        EnsureWindowFunctionSupported(windowFunction);

        var slot = new WindowSlot
        {
            Expr = windowFunction,
            Map = new Dictionary<EvalRow, object?>(ReferenceEqualityComparer<EvalRow>.Instance)
        };
        windowSlots.Add(slot);
        return (row, group) => slot.Map.TryGetValue(row, out var value) ? value : null;
    }

    private void EnsureWindowFunctionSupported(WindowFunctionExpr windowFunction)
    {
        if (!(Dialect?.SupportsWindowFunctions ?? true)
            || !(Dialect?.SupportsWindowFunction(windowFunction.Name) ?? true))
            throw SqlUnsupported.ForDialect(Dialect!, $"window functions ({windowFunction.Name})");

        EnsureWindowFunctionArgumentsAtRuntime(windowFunction);
    }

    /// <summary>
    /// EN: Validates window function argument arity at runtime to protect execution paths that bypass parser validation.
    /// PT: Valida a aridade dos argumentos de funções de janela em runtime para proteger caminhos de execução que ignoram a validação do parser.
    /// </summary>
    private void EnsureWindowFunctionArgumentsAtRuntime(WindowFunctionExpr windowFunctionExpr)
    {
        if (Dialect is null)
            return;

        if (!Dialect.TryGetWindowFunctionArgumentArity(windowFunctionExpr.Name, out var minArgs, out var maxArgs))
            return;

        var actualArgs = windowFunctionExpr.Args.Count;
        if (actualArgs < minArgs || actualArgs > maxArgs)
        {
            if (minArgs == maxArgs)
                throw new InvalidOperationException($"Window function '{windowFunctionExpr.Name}' expects exactly {minArgs} argument(s), but received {actualArgs}.");

            throw new InvalidOperationException($"Window function '{windowFunctionExpr.Name}' expects between {minArgs} and {maxArgs} argument(s), but received {actualArgs}.");
        }
    }

    private DbType InferDbTypeFromExpression(
        SqlExpr exprAst,
        List<EvalRow> sampleRows,
        IDictionary<string, Source> ctes)
    {
        if (IsSequenceExpression(exprAst))
            return DbType.Int64;

        if (exprAst is WindowFunctionExpr w)
            return Dialect?.InferWindowFunctionDbType(w, arg => InferDbTypeFromExpression(arg, sampleRows, ctes))
                ?? DbType.Object;

        foreach (var row in sampleRows)
        {
            var value = Eval(exprAst, row, group: null, ctes);
            if (value is null || value is DBNull)
                continue;

            var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
            try
            {
                return type.ConvertTypeToDbType();
            }
            catch (ArgumentException)
            {
                return DbType.Object;
            }
        }

        return DbType.Object;
    }

    private static bool IsSequenceExpression(SqlExpr exprAst)
    {
        return exprAst switch
        {
            FunctionCallExpr fn => IsSequenceFunctionName(fn.Name),
            CallExpr call => IsSequenceFunctionName(call.Name),
            _ => false
        };
    }

    private static bool IsSequenceFunctionName(string? name)
        => string.Equals(name, "NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase);

    private void EnsureDialectSupportsSequenceFunction(string? functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return;

        var dialect = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para funções de sequence.");

        if (functionName!.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsNextValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(dialect, "NEXT VALUE FOR");

        if (functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsPreviousValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(dialect, "PREVIOUS VALUE FOR");

        if ((functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase))
            && !SupportsSequenceFunctionCall(dialect, functionName))
        {
            throw SqlUnsupported.ForDialect(dialect, functionName.ToUpperInvariant());
        }
    }

    private static bool SupportsSequenceFunctionCall(ISqlDialect dialect, string functionName)
    {
        if (dialect.SupportsSequenceFunctionCall(functionName))
            return true;

        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            return functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase);
        }

        if (dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            return (functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                    || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase))
                && dialect.SupportsSequenceDotValueExpression(functionName);
        }

        return false;
    }

    private static bool IncludExtraColumns(
        List<EvalRow> sampleRows,
        List<TableResultColMock> cols,
        List<Func<EvalRow, EvalGroup?, object?>> evals,
        string raw)
    {
        // tableAlias.*  (robusto: aceita espaços e crases)
        // casa "algo .*" com espaços opcionais:  <prefix> . *
        var mStar = Regex.Match(raw, @"^(?<p>.+?)\s*\.\s*\*\s*$");
        if (!mStar.Success)
            return true;

        var prefix = mStar.Groups["p"].Value.Trim();

        var sample = sampleRows.FirstOrDefault();
        if (sample is null)
            return true;

        // remove crases (caso: `t2`.*)
        prefix = prefix.Trim('`');

        // tenta achar por alias diretamente
        if (!sample.Sources.TryGetValue(prefix, out var src))
        {
            // tenta case-insensitive na mão (Sources é OrdinalIgnoreCase, mas não custa)
            var hit = sample.Sources.Keys.FirstOrDefault(k => k.Equals(prefix, StringComparison.OrdinalIgnoreCase));
            if (hit is not null)
                src = sample.Sources[hit];
        }

        if (src is null)
            return true;

        foreach (var colName in src.ColumnNames)
        {
            var alias = MakeUniqueAlias(cols, colName, src.Alias);
            cols.Add(new TableResultColMock(src.Alias, alias, colName, cols.Count, DbType.Object, true));
            evals.Add((r, g) => ResolveColumn(src.Alias, colName, r));
        }
        return false; // ✅ importante: não cai pro parser de expressão
    }

    private static bool ExpandSelectAsterisc(
        List<TableResultColMock> cols,
        List<Func<EvalRow, EvalGroup?, object?>> evals,
        EvalRow? sampleFirst, string raw)
    {
        if (raw != "*")
            return true;

        if (sampleFirst is null)
            return false;

        foreach (var src in sampleFirst.Sources.Values)
        {
            foreach (var colName in src.ColumnNames)
            {
                var alias = MakeUniqueAlias(cols, colName, src.Alias);
                cols.Add(new TableResultColMock(src.Alias, alias, colName, cols.Count, DbType.Object, true));
                evals.Add((r, g) => ResolveColumn(src.Alias, colName, r));
            }
        }
        return false;
    }

    // Remove "AS alias" somente quando:
    // - está no FINAL do select item
    // - e esse "AS" está fora de parênteses (pra não quebrar CAST(x AS CHAR))
    private static (string expr, string? alias) SplitTrailingAsAlias(
        string raw,
        string? alreadyAlias)
    {
        raw = raw.Trim();
        if (!string.IsNullOrWhiteSpace(alreadyAlias))
            return (raw, alreadyAlias);

        // varre de trás pra frente, mantendo depth de parênteses
        int depth = GetDepthAlias(raw);

        // regex final (fora de parênteses garantido pelo scanner acima? não 100%)
        // então fazemos um scanner que acha o último AS fora de parênteses.
        int asPos = -1;
        FindPositionOfAS(raw, ref depth, ref asPos);

        if (asPos < 0)
        {
            // MySQL permite alias sem AS:  SELECT COUNT(*) c1, col1 c2 FROM ...
            // Precisamos separar o último identificador (fora de parênteses) como alias,
            // sem quebrar expressões como "t2.*" ou "CAST(x AS CHAR)".
            if (TrySplitTrailingImplicitAlias(raw, out var expr0, out var alias0))
            {
                if (raw.IndexOf("<=>", StringComparison.Ordinal) >= 0
                    || Regex.IsMatch(raw, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
                {
                    Console.WriteLine($"[SPLIT-ALIAS-SUSPECT] Raw='{raw}' Expr='{expr0}' Alias='{alias0}'");
                }
                return (expr0, alias0);
            }

            return (raw, null);
        }

        // pega o sufixo depois do AS
        var after = raw[(asPos + 2)..].Trim();
        if (after.Length == 0)
            return (raw, null);

        // alias pode vir como `C` ou C
        // (aqui, só aceitamos identificador simples, que é o caso do seu teste)
        var m = Regex.Match(after, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?\s*$");
        if (!m.Success)
            return (raw, null);

        var alias = m.Groups["a"].Value;

        // expr é tudo antes do AS
        var expr = raw[..asPos].TrimEnd();
        if (expr.Length == 0)
            return (raw, null);

        return (expr, alias);
    }

    private static bool TrySplitTrailingImplicitAlias(
        string raw,
        out string expr,
        out string alias)
    {
        expr = raw;
        alias = string.Empty;

        // scanner reverso, ignorando o que está dentro de parênteses
        int depth = 0;
        int i = raw.Length - 1;
        while (i >= 0 && char.IsWhiteSpace(raw[i])) i--;
        if (i < 0) return false;

        int end = i;
        while (i >= 0)
        {
            var ch = raw[i];
            if (ch == ')') { depth++; i--; continue; }
            if (ch == '(') { depth = Math.Max(0, depth - 1); i--; continue; }
            if (depth == 0 && char.IsWhiteSpace(ch)) break;
            i--;
        }

        int start = i + 1;
        if (start > end) return false;

        var token = raw[start..(end + 1)].Trim();
        if (token.Length == 0) return false;

        // Alias tem que ser identificador simples: não aceita "t.col1", "*", "?", etc.
        var m = Regex.Match(token, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?$", RegexOptions.CultureInvariant);
        if (!m.Success) return false;

        var a = m.Groups["a"].Value;
        if (IsLikelyKeyword(a)) return false;

        var before = raw[..start].TrimEnd();
        if (before.Length == 0) return false;

        // Evita pegar alias em "t2." (ex: "t2.*" já falha pelo regex acima)
        if (before.EndsWith(".")) return false;
        if (Regex.IsMatch(before, @"(<=>|<>|!=|>=|<=|=|>|<|\+|-|\*|/|,)\s*$", RegexOptions.CultureInvariant))
            return false;
        if (Regex.IsMatch(before, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            return false;

        expr = before;
        alias = a;
        return true;
    }

    private static bool IsLikelyKeyword(string s)
    {
        // lista pequena e pragmática: só pra não confundir com sintaxe
        return s.Equals("FROM", StringComparison.OrdinalIgnoreCase)
            || s.Equals("WHERE", StringComparison.OrdinalIgnoreCase)
            || s.Equals("JOIN", StringComparison.OrdinalIgnoreCase)
            || s.Equals("LEFT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("RIGHT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("INNER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("OUTER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ON", StringComparison.OrdinalIgnoreCase)
            || s.Equals("GROUP", StringComparison.OrdinalIgnoreCase)
            || s.Equals("BY", StringComparison.OrdinalIgnoreCase)
            || s.Equals("HAVING", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ORDER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("LIMIT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("UNION", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ALL", StringComparison.OrdinalIgnoreCase)
            || s.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ASC", StringComparison.OrdinalIgnoreCase)
            || s.Equals("DESC", StringComparison.OrdinalIgnoreCase)
            || s.Equals("NULL", StringComparison.OrdinalIgnoreCase)
            || s.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
            || s.Equals("FALSE", StringComparison.OrdinalIgnoreCase);
    }

    private static void FindPositionOfAS(string raw, ref int depth, ref int asPos)
    {
        for (int i = 0; i < raw.Length - 1; i++)
        {
            UpdateParenDepth(raw[i], ref depth);
            if (depth != 0) continue;

            if (!IsAsAt(raw, i)) continue;
            if (!IsWordBoundary(raw, i, 2)) continue;

            asPos = i;
        }
    }

    private static void UpdateParenDepth(char ch, ref int depth)
    {
        if (ch == '(') { depth++; return; }
        if (ch == ')') depth = Math.Max(0, depth - 1);
    }

    private static bool IsAsAt(string s, int i)
    {
        return (s[i] == 'A' || s[i] == 'a') &&
               (s[i + 1] == 'S' || s[i + 1] == 's');
    }

    // length=2 para "AS"
    private static bool IsWordBoundary(string s, int start, int length)
    {
        int left = start - 1;
        int right = start + length;

        bool leftOk = left < 0 || !IsIdentChar(s[left]);
        bool rightOk = right >= s.Length || !IsIdentChar(s[right]);

        return leftOk && rightOk;
    }

    private static bool IsIdentChar(char c) =>
        char.IsLetterOrDigit(c) || c == '_';

    private static int GetDepthAlias(string raw)
    {
        int depth = 0;
        for (int i = raw.Length - 1; i >= 0; i--)
        {
            char ch = raw[i];
            if (ch == ')') depth++;
            else if (ch == '(') depth = Math.Max(0, depth - 1);

            // só tenta achar " AS " quando estiver fora de parênteses
            if (depth != 0) continue;

            // procura um " AS " (case-insensitive) perto do fim.
            // Ex: "COUNT(val) AS C"
            //      ^ aqui
            if (i >= 1 && raw[i] == 'S' || raw[i] == 's')
            {
                // checa "...A S..." com espaço antes e depois
                // vamos achar o padrão usando regex no sufixo por segurança
            }
        }

        return depth;
    }

    private static string InferColumnAlias(string raw)
    {
        raw = raw.Trim();

        // For identifiers/qualified columns, we want the last part, but the token printer
        // may produce spaces around dots (e.g. "u . id"). Normalize just for alias inference.
        var norm = Regex.Replace(raw, @"\s*\.\s*", ".");

        var dot = norm.LastIndexOf('.');
        if (dot >= 0 && dot + 1 < norm.Length)
            return norm[(dot + 1)..].Trim().Trim('`');

        return norm.Trim().Trim('`');
    }

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
            ApplyLimit(res, q);
            if (debugTrace is not null && q.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    res.Count,
                    TimeSpan.Zero,
                    FormatLimitDebugDetails(q.RowLimit));
            }
            return res;
        }

        // Pre-parse ORDER BY keys once
        var keys = new List<(Func<Dictionary<int, object?>, object?> Get, bool Desc, bool? NullsFirst)>();
        var joinFieldsByRow = new Dictionary<Dictionary<int, object?>, Dictionary<string, object?>>(ReferenceEqualityComparer<Dictionary<int, object?>>.Instance);
        for (int i = 0; i < res.Count && i < res.JoinFields.Count; i++)
            joinFieldsByRow[res[i]] = res.JoinFields[i];

        foreach (var it in q.OrderBy)
        {
            var raw = (it.Raw ?? string.Empty).Trim();
            if (raw.Length == 0)
                continue;

            // ordinal: ORDER BY 1,2,...
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: ORDER BY ordinal must be >= 1");

                var colIdx = ord - 1;
                if (colIdx >= res.Columns.Count)
                    throw new InvalidOperationException($"invalid: ORDER BY ordinal {ord} out of range");

                keys.Add((r => r.TryGetValue(colIdx, out var v) ? v : null, it.Desc, it.NullsFirst));
                continue;
            }

            // column/alias fast-path
            var col = res.Columns.FirstOrDefault(c =>
                c.ColumnAlias.Equals(raw, StringComparison.OrdinalIgnoreCase)
                || c.ColumnName.Equals(raw, StringComparison.OrdinalIgnoreCase));
            if (col is not null)
            {
                var colIdx = col.ColumIndex;
                keys.Add((r => r.TryGetValue(colIdx, out var v) ? v : null, it.Desc, it.NullsFirst));
                continue;
            }

            var aliasToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < res.Columns.Count; i++)
            {
                var col1 = res.Columns[i];

                if (!string.IsNullOrWhiteSpace(col1.ColumnAlias) && !aliasToIndex.ContainsKey(col1.ColumnAlias))
                    aliasToIndex[col1.ColumnAlias] = i;

                if (!string.IsNullOrWhiteSpace(col1.ColumnName) && !aliasToIndex.ContainsKey(col1.ColumnName))
                    aliasToIndex[col1.ColumnName] = i;

                var tail = col1.ColumnName;
                var dot = tail.LastIndexOf('.');
                if (dot >= 0 && dot + 1 < tail.Length)
                    tail = tail[(dot + 1)..];

                if (!string.IsNullOrWhiteSpace(tail) && !aliasToIndex.ContainsKey(tail))
                    aliasToIndex[tail] = i;
            }

            var expr = ParseExpr(raw);

            Func<Dictionary<int, object?>, object?> get = r =>
            {
                var fake = EvalRow.FromProjected(res, r, aliasToIndex);
                if (joinFieldsByRow.TryGetValue(r, out var rowFields))
                {
                    foreach (var kv in rowFields)
                    {
                        if (!fake.Fields.ContainsKey(kv.Key))
                            fake.Fields[kv.Key] = kv.Value;

                        var dot = kv.Key.IndexOf('.');
                        if (dot > 0 && dot + 1 < kv.Key.Length)
                        {
                            var unqualified = kv.Key[(dot + 1)..];
                            if (!fake.Fields.ContainsKey(unqualified))
                                fake.Fields[unqualified] = kv.Value;
                        }
                    }
                }

                return Eval(expr, fake, group: null, ctes);
            };

            keys.Add((Get: get, it.Desc, it.NullsFirst));
        }

        if (keys.Count == 0)
        {
            var limitInput = res.Count;
            ApplyLimit(res, q);
            if (debugTrace is not null && q.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    res.Count,
                    TimeSpan.Zero,
                    FormatLimitDebugDetails(q.RowLimit));
            }
            return res;
        }

        int CompareObj(object? a, object? b)
        {
            if (a is null && b is null) return 0;
            if (a is null) return -1;
            if (b is null) return 1;

            return a.Compare(b, Dialect);
        }

        int CompareRows(Dictionary<int, object?> ra, Dictionary<int, object?> rb)
        {
            foreach (var (Get, Desc, NullsFirst) in keys)
            {
                var ka = Get(ra);
                var kb = Get(rb);

                int cmp;
                var kaIsNull = IsNullish(ka);
                var kbIsNull = IsNullish(kb);
                if (kaIsNull || kbIsNull)
                {
                    if (kaIsNull && kbIsNull) cmp = 0;
                    else
                    {
                        var explicitNullsFirst = NullsFirst;
                        if (explicitNullsFirst.HasValue)
                            cmp = kaIsNull ? (explicitNullsFirst.Value ? -1 : 1) : (explicitNullsFirst.Value ? 1 : -1);
                        else
                            cmp = kaIsNull ? (Desc ? 1 : -1) : (Desc ? -1 : 1);
                    }
                }
                else
                {
                    cmp = CompareObj(ka, kb);
                    if (Desc) cmp = -cmp;
                }

                if (cmp != 0) return cmp;
            }
            return 0;
        }

        var sortStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var sortInput = res.Count;
        var sorted = res.OrderBy(r => r, Comparer<Dictionary<int, object?>>.Create(CompareRows)).ToList();
        res.Clear();
        foreach (var r in sorted) res.Add(r);

        if (res.JoinFields.Count > 0)
        {
            var sortedJoinFields = sorted
                .Select(r => joinFieldsByRow.TryGetValue(r, out var jf)
                    ? jf
                    : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase))
                .ToList();
            res.JoinFields = sortedJoinFields;
        }

        debugTrace?.AddStep(
            "Sort",
            sortInput,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(sortStart)),
            FormatOrderByDebugDetails(q.OrderBy));

        var limitInputRows = res.Count;
        ApplyLimit(res, q);
        if (debugTrace is not null && q.RowLimit is not null)
        {
            debugTrace.AddStep(
                "Limit",
                limitInputRows,
                res.Count,
                TimeSpan.Zero,
                FormatLimitDebugDetails(q.RowLimit));
        }
        return res;
    }

    private static string FormatLimitDebugDetails(SqlRowLimit rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset limit when limit.Offset.HasValue => $"count={limit.Count};offset={limit.Offset.Value}",
            SqlLimitOffset limit => $"count={limit.Count}",
            SqlFetch fetch when fetch.Offset.HasValue => $"count={fetch.Count};offset={fetch.Offset.Value}",
            SqlFetch fetch => $"count={fetch.Count}",
            SqlTop top => $"count={top.Count}",
            _ => string.Empty
        };

    private static string FormatUnionInputsDebugDetails(IReadOnlyList<SqlSelectQuery> parts, IReadOnlyList<bool> allFlags)
    {
        var distinctCount = allFlags.Count(static flag => !flag);
        var allCount = allFlags.Count - distinctCount;
        return $"parts={parts.Count};allSegments={allCount};distinctSegments={distinctCount}";
    }

    private static string FormatUnionCombineDebugDetails(IReadOnlyList<SqlSelectQuery> parts, IReadOnlyList<bool> allFlags)
    {
        var mode = allFlags.Any(static flag => !flag) ? "UNION DISTINCT" : "UNION ALL";
        return $"{FormatUnionInputsDebugDetails(parts, allFlags)};mode={mode}";
    }

    private static string FormatDistinctDebugDetails(int projectionColumnCount)
        => $"columns={projectionColumnCount}";

    private static string FormatProjectDebugDetails(IReadOnlyList<SqlSelectItem> selectItems)
    {
        var items = selectItems
            .Select(static item =>
            {
                var raw = (item.Raw ?? string.Empty).Trim();
                var alias = (item.Alias ?? string.Empty).Trim();

                if (raw.Length == 0)
                    return alias;

                return alias.Length == 0
                    ? raw
                    : $"{raw} AS {alias}";
            })
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .ToArray();
        return items.Length == 0
            ? $"columns={selectItems.Count}"
            : $"columns={selectItems.Count};items={string.Join("|", items)}";
    }

    private static string FormatOrderByDebugDetails(IReadOnlyList<SqlOrderByItem> orderBy)
    {
        var items = orderBy
            .Select(static item =>
            {
                var raw = (item.Raw ?? string.Empty).Trim();
                if (raw.Length == 0)
                    return null;

                return raw + (item.Desc ? " DESC" : " ASC");
            })
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .ToArray();
        return items.Length == 0
            ? $"keys={orderBy.Count}"
            : $"keys={orderBy.Count};items={string.Join("|", items)}";
    }

    private static string FormatGroupDebugDetails(SqlSelectQuery query)
        => query.GroupBy.Count == 0
            ? "aggregate"
            : $"keys={query.GroupBy.Count};items={string.Join("|", query.GroupBy)}";

    private static string FormatJoinDebugDetails(SqlJoin join)
    {
        var source = FormatSource(join.Table);
        var predicate = SqlExprPrinter.Print(join.On);
        return string.IsNullOrWhiteSpace(predicate)
            ? source
            : $"{source};on={predicate}";
    }

    private sealed class QueryDebugTraceBuilder(string queryType)
    {
        private readonly List<QueryDebugTraceStep> _steps = [];

        public void AddStep(
            string operatorName,
            int inputRows,
            int outputRows,
            TimeSpan executionTime,
            string? details = null)
        {
            _steps.Add(new QueryDebugTraceStep(
                operatorName,
                inputRows,
                outputRows,
                executionTime,
                details));
        }

        public QueryDebugTrace Build()
            => new(queryType, 0, null, _steps.AsReadOnly());
    }

    private static void ApplyLimit(
        TableResultMock res,
        SqlSelectQuery q)
    {
        int? offset = null;
        int take;
        switch (q.RowLimit)
        {
            case SqlLimitOffset l:
                offset = l.Offset;
                take = l.Count;
                break;
            case SqlFetch f:
                offset = f.Offset;
                take = f.Count;
                break;
            case SqlTop t:
                take = t.Count;
                break;
            default:
                return;
        }

        var skip = offset ?? 0;
        var sliced = res.Skip(skip).Take(take).ToList();
        res.Clear();
        foreach (var r in sliced) res.Add(r);
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

    private static bool TryEvalDateIntervalArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        if (left is not DateTime dateTime || right is not IntervalValue interval)
        {
            result = null;
            return false;
        }

        result = op switch
        {
            SqlBinaryOp.Add => dateTime.Add(interval.Span),
            SqlBinaryOp.Subtract => dateTime.Subtract(interval.Span),
            _ => throw new InvalidOperationException("op aritmético inválido")
        };
        return true;
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
            $@"(?<![A-Z0-9_$])(?<kw>FROM|JOIN)\s+(?<table>[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?" +
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

            if (baseValue.Compare(whenValue, Dialect) == 0)
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

        if (TryEvalCoalesceFunction(fn, EvalArg, out var coalesceResult))
            return coalesceResult;

        if (TryEvalNullIfFunction(fn, dialect, EvalArg, out var nullIfResult))
            return nullIfResult;

        EnsureDialectSupportsSequenceFunction(fn.Name);
        if (SqlSequenceEvaluator.TryEvaluateCall(_cnn, fn.Name, fn.Args, expr => Eval(expr, row, group, ctes), out var sequenceValue))
            return sequenceValue;

        var jsonNumberResult = TryEvalJsonAndNumberFunctions(fn, dialect, EvalArg, out var handledJsonNumber);
        if (handledJsonNumber)
            return jsonNumberResult;

        // TRY_CAST(x AS TYPE) - similar ao CAST, mas retorna null em falha
        if (fn.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            return EvalTryCast(fn, EvalArg);

        // CAST(x AS TYPE) - aqui chega como CallExpr("CAST", [expr, RawSqlExpr("SIGNED")]) via parser
        if (fn.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
            return EvalCast(fn, EvalArg);
        var concatResult = TryEvalConcatFunctions(fn, EvalArg, out var handledConcat);
        if (handledConcat)
            return concatResult;

        if (TryEvalBasicStringFunction(fn, EvalArg, out var basicStringResult))
            return basicStringResult;

        if (TryEvalSubstringFunction(fn, EvalArg, out var substringResult))
            return substringResult;

        if (TryEvalReplaceFunction(fn, EvalArg, out var replaceResult))
            return replaceResult;


        var dateAddResult = TryEvalDateAddFunction(fn, row, group, ctes, EvalArg, out var handledDateAdd);
        if (handledDateAdd)
            return dateAddResult;

        if (TryEvalDateConstructionFunction(fn, EvalArg, out var dateConstructionResult))
            return dateConstructionResult;

        if (TryEvalFieldFunction(fn, dialect, EvalArg, out var fieldResult))
            return fieldResult;

        if (fn.Args.Count == 0
            && SqlTemporalFunctionEvaluator.IsKnownTemporalFunctionName(fn.Name))
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for dialect '{dialect.Name}'.");

// Unknown scalar => null (don't explode tests)
        return null;

        object? EvalArg(int i) => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null;
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

    private static bool TryEvalDateConstructionFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase))
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

        if (fn.Name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("CHAR_LENGTH", StringComparison.OrdinalIgnoreCase))
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
            || fn.Name.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase)))
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
                if (decimal.TryParse(v!.ToString(), out var dx)) return dx;
                return null;
            }

            return v!.ToString();
        }
        catch
        {
            return null;
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

        if (TryEvalOpenJsonFunction(fn, dialect, evalArg, out var openJsonResult))
            return openJsonResult;

        if (TryEvalJsonUnquoteFunction(fn, evalArg, out var jsonUnquoteResult))
            return jsonUnquoteResult;

        if (TryEvalToNumberFunction(fn, evalArg, out var toNumberResult))
            return toNumberResult;

        handled = false;
        return null;
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
            || fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        EnsureJsonExtractionSupported(fn.Name, dialect);
        if (!TryGetJsonAndPathArguments(evalArg, out var json, out var path))
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

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonValueFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");
    }

    private object? TryEvalJsonExtractionValue(FunctionCallExpr fn, object json, string path)
    {
        try
        {
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

    private void LogFunctionEvaluationFailure(Exception exception)
    {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine($"{GetType().Name}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        Console.WriteLine(exception);
    }

    private object? ApplyJsonValueReturningClause(FunctionCallExpr fn, object? value)
    {
        if (IsNullish(value) || !TryGetJsonReturningTypeSql(fn, out var normalizedType))
            return value;

        return ConvertJsonValueForReturningType(value, normalizedType);
    }

    private static bool TryGetJsonReturningTypeSql(FunctionCallExpr fn, out string normalizedType)
    {
        normalizedType = string.Empty;
        if (fn.Args.Count < 3 || fn.Args[2] is not RawSqlExpr raw)
            return false;

        const string prefix = "RETURNING ";
        if (!raw.Sql.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var typeSql = raw.Sql[prefix.Length..].Trim();
        if (typeSql.Length == 0)
            return false;

        normalizedType = typeSql.ToUpperInvariant();
        return true;
    }

    private static object? ConvertJsonValueForReturningType(object? value, string normalizedType)
    {
        if (IsJsonReturningDecimalType(normalizedType))
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningFloatingType(normalizedType))
            return Convert.ToDouble(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningIntegerType(normalizedType))
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);

        if (IsJsonReturningTextType(normalizedType))
            return value?.ToString();

        return value;
    }

    private static bool IsJsonReturningDecimalType(string normalizedType)
        => normalizedType.StartsWith("NUMBER", StringComparison.Ordinal)
            || normalizedType.StartsWith("DECIMAL", StringComparison.Ordinal)
            || normalizedType.StartsWith("NUMERIC", StringComparison.Ordinal);

    private static bool IsJsonReturningFloatingType(string normalizedType)
        => normalizedType.StartsWith("DOUBLE", StringComparison.Ordinal)
            || normalizedType.StartsWith("FLOAT", StringComparison.Ordinal)
            || normalizedType.StartsWith("REAL", StringComparison.Ordinal)
            || normalizedType.StartsWith("BINARY_DOUBLE", StringComparison.Ordinal)
            || normalizedType.StartsWith("BINARY_FLOAT", StringComparison.Ordinal);

    private static bool IsJsonReturningIntegerType(string normalizedType)
        => normalizedType.StartsWith("INT", StringComparison.Ordinal)
            || normalizedType.StartsWith("INTEGER", StringComparison.Ordinal)
            || normalizedType.StartsWith("SMALLINT", StringComparison.Ordinal)
            || normalizedType.StartsWith("BIGINT", StringComparison.Ordinal);

    private static bool IsJsonReturningTextType(string normalizedType)
        => normalizedType.StartsWith("VARCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("VARCHAR2", StringComparison.Ordinal)
            || normalizedType.StartsWith("NVARCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("NVARCHAR2", StringComparison.Ordinal)
            || normalizedType.StartsWith("CHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("NCHAR", StringComparison.Ordinal)
            || normalizedType.StartsWith("CLOB", StringComparison.Ordinal);
    }

    private object? TryEvalConcatFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out bool handled)
    {
        handled = true;

        if (TryEvalConcatFunction(fn, evalArg, out var concatResult))
            return concatResult;

        if (TryEvalConcatWithSeparatorFunction(fn, evalArg, out var concatWithSeparatorResult))
            return concatWithSeparatorResult;

        handled = false;
        return null;
    }

    private bool TryEvalConcatFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONCAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var parts = new string[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
        {
            if (!TryConvertConcatArgument(evalArg(i), Dialect!.ConcatReturnsNullOnNullInput, out var part))
            {
                result = null;
                return true;
            }

            parts[i] = part;
        }

        result = string.Concat(parts);
        return true;
    }

    private static bool TryEvalConcatWithSeparatorFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var separatorValue = evalArg(0);
        if (IsNullish(separatorValue))
        {
            result = null;
            return true;
        }

        var parts = new List<string>();
        for (var i = 1; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (!IsNullish(value))
                parts.Add(value?.ToString() ?? string.Empty);
        }

        result = string.Join(separatorValue?.ToString() ?? string.Empty, parts);
        return true;
    }

    private static bool TryConvertConcatArgument(
        object? value,
        bool nullInputReturnsNull,
        out string part)
    {
        if (IsNullish(value))
        {
            part = string.Empty;
            return !nullInputReturnsNull;
        }

        part = value?.ToString() ?? string.Empty;
        return true;
    }

    private static object? TryReadJsonPathValue(object json, string path)
    {
        if (!TryParseSimpleJsonPathSegments(path, out var segments))
            return null;

        using var doc = System.Text.Json.JsonDocument.Parse(json.ToString() ?? string.Empty);
        if (!TryReadJsonPathElement(doc.RootElement, segments, out var element))
            return null;

        return ConvertJsonElementToValue(element);
    }

    private static bool TryParseSimpleJsonPathSegments(string path, out string[] segments)
    {
        segments = [];
        if (!path.StartsWith("$.", StringComparison.Ordinal))
            return false;

        var rawSegments = path[2..].Split('.');
        if (rawSegments.Length == 0)
            return false;

        segments = new string[rawSegments.Length];
        for (var i = 0; i < rawSegments.Length; i++)
        {
            var segment = rawSegments[i].Trim().Trim('"');
            if (segment.Length == 0)
                return false;

            segments[i] = segment;
        }

        return true;
    }

    private static bool TryReadJsonPathElement(
        System.Text.Json.JsonElement element,
        IReadOnlyList<string> segments,
        out System.Text.Json.JsonElement value)
    {
        value = element;
        for (var i = 0; i < segments.Count; i++)
        {
            if (value.ValueKind != System.Text.Json.JsonValueKind.Object
                || !value.TryGetProperty(segments[i], out var next))
            {
                value = default;
                return false;
            }

            value = next;
        }

        return true;
    }

    private static object? ConvertJsonElementToValue(System.Text.Json.JsonElement element)
        => element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => element.GetString(),
            System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out var integerValue) ? integerValue : element.GetDecimal(),
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Null => null,
            _ => element.ToString()
        };
    }

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

        var raw = Eval(fn.Args[0], row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (!TryParseIntervalLiteral(raw!, out var value, out var unit))
            return null;

        var span = TryConvertIntervalToTimeSpan(value, unit);
        return span is null ? null : new IntervalValue(span.Value);
    }

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

        var values = TryGetAggregateValues(fn, group, ctes);
        if (values is null)
            return null;

        if (values.Count == 0)
        {
            // MySQL: SUM/AVG/MIN/MAX sobre conjunto vazio (ou tudo NULL) => NULL
            return null;
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
            "SUM" => values.Sum(static value => value!.ToDec()),
            "AVG" => values.Sum(static value => value!.ToDec()) / values.Count,
            "MIN" => values.Min(static value => value!.ToDec()),
            "MAX" => values.Max(static value => value!.ToDec()),
            "GROUP_CONCAT" => EvalStringAggregate(values, separator, ","),
            "STRING_AGG" => EvalStringAggregate(values, separator, ","),
            "LISTAGG" => EvalStringAggregate(values, separator, string.Empty),
            _ => null
        };
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

        // COUNT(DISTINCT ...)
        if (name == "COUNT" && fn.Distinct)
            return EvalCountDistinct(fn, group, ctes);

        if (name is "GROUP_CONCAT" or "STRING_AGG" or "LISTAGG")
        {
            if (!dialect.SupportsStringAggregateFunction(name))
                throw SqlUnsupported.ForDialect(dialect, name);

            return EvalStringAggregateForCallExpr(fn, group, ctes, name);
        }

        // para os outros casos (sem DISTINCT), reaproveita o existente
        var shim = new FunctionCallExpr(fn.Name, fn.Args);
        return EvalAggregate(shim, group, ctes);
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
    {
        // chave determinística; evita variações idiotas
        if (v is null) return "NULL";
        if (v is DBNull) return "NULL";

        return v switch
        {
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            DateTime dt => dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            bool b => b ? "1" : "0",
            string s => (dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase) == StringComparison.Ordinal
                ? s
                : s.ToUpperInvariant(),
            _ => v.ToString() ?? ""
        };
    }

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
        IReadOnlyList<SqlOrderByItem> orderBy,
        EvalRow row,
        IDictionary<string, Source> ctes)
        => orderBy
            .Select(order => Eval(order.Expr, row, null, ctes))
            .ToArray();

    private IComparer<object?[]> CreateWithinGroupOrderKeyComparer(IReadOnlyList<SqlOrderByItem> orderBy)
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
        if (name != "COUNT")
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
#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            var parsedExpression = ParseExpr(exprRaw);
            return WalkHasAggregate(parsedExpression)
                || (parsedExpression is RawSqlExpr && LooksLikeAggregateExpression(exprRaw));
        }
        catch (Exception e)
        {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{GetType().Name}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine(e);

            // fallback: preserve aggregate semantics even when expression parsing fails.
            return LooksLikeAggregateExpression(exprRaw);
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    private static bool LooksLikeAggregateExpression(string exprRaw)
        => _aggregateExpressionRegex.IsMatch(exprRaw);

    private static bool WalkHasAggregate(SqlExpr expr) => expr switch
    {
        CallExpr call => HasAggregateFunctionCall(call.Name, call.Args),
        FunctionCallExpr function => HasAggregateFunctionCall(function.Name, function.Args),
        BinaryExpr binary => WalkHasAggregate(binary.Left) || WalkHasAggregate(binary.Right),
        UnaryExpr unary => WalkHasAggregate(unary.Expr),
        LikeExpr like => WalkHasAggregateLike(like),
        InExpr inExpression => WalkHasAggregateIn(inExpression),
        IsNullExpr isNull => WalkHasAggregate(isNull.Expr),
        QuantifiedComparisonExpr quantified => WalkHasAggregate(quantified.Left) || WalkHasAggregate(quantified.Subquery),

        // ⚠️ cuidado: isso aqui tá “agressivo”
        // EXISTS não é aggregate. Mas você provavelmente usa isso como “precisa de contexto especial”.
        // Vou deixar como está pra não quebrar outras coisas.
        ExistsExpr => true,
        RowExpr row => WalkHasAggregateSequence(row.Items),
        _ => false
    };

    private static bool HasAggregateFunctionCall(string name, IReadOnlyList<SqlExpr> args)
        => _aggFnsStatic.Contains(name) || WalkHasAggregateSequence(args);

    private static bool WalkHasAggregateLike(LikeExpr like)
        => WalkHasAggregate(like.Left)
            || WalkHasAggregate(like.Pattern)
            || (like.Escape is not null && WalkHasAggregate(like.Escape));

    private static bool WalkHasAggregateIn(InExpr inExpression)
        => WalkHasAggregate(inExpression.Left)
            || WalkHasAggregateSequence(inExpression.Items);

    private static bool WalkHasAggregateSequence(IEnumerable<SqlExpr> expressions)
    {
        foreach (var expression in expressions)
        {
            if (WalkHasAggregate(expression))
                return true;
        }

        return false;
    }

    private static readonly HashSet<string> _aggFnsStatic = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG"
    };

    // ---------------- RESOLUTION HELPERS ----------------

    private object? ResolveParam(
        string name)
    {
        // accept @p, :p, ?  (for ?, just take first parameter)
        if (name == "?")
            return ResolvePositionalParam();

        return ResolveNamedParam(name);
    }

    private object? ResolvePositionalParam()
        => _pars.Count > 0 ? NormalizeParameterValue((IDataParameter)_pars[0]!) : null;

    private object? ResolveNamedParam(string name)
    {
        var normalizedName = name.TrimStart('@', ':');
        foreach (IDataParameter parameter in _pars)
        {
            var parameterName = parameter.ParameterName?.TrimStart('@', ':');
            if (string.Equals(parameterName, normalizedName, StringComparison.OrdinalIgnoreCase))
                return NormalizeParameterValue(parameter);
        }

        return null;
    }

    private static object? NormalizeParameterValue(IDataParameter parameter)
        => parameter.Value is DBNull ? null : parameter.Value;

    private static bool TrySplitQualifiedIdentifier(string name, out string qualifier, out string columnName)
    {
        qualifier = string.Empty;
        columnName = string.Empty;

        var dot = name.IndexOf('.');
        if (dot < 0)
            return false;

        qualifier = name[..dot];
        columnName = name[(dot + 1)..];
        return true;
    }

    private static object? ResolveIdentifier(
        string name,
        EvalRow row)
    {
        if (TrySplitQualifiedIdentifier(name, out var qualifier, out var columnName))
            return ResolveColumn(qualifier, columnName, row);

        return TryResolveIdentifierFromSources(name, row, out var value)
            ? value
            : TryResolveProjectedIdentifier(name, row, out value)
                ? value
                : null;
    }

    private static bool TryResolveIdentifierFromSources(string name, EvalRow row, out object? value)
    {
        foreach (var source in row.Sources.Values)
        {
            if (row.Fields.TryGetValue($"{source.Alias}.{name}", out value))
                return true;
        }

        value = null;
        return false;
    }

    private static bool TryResolveProjectedIdentifier(string name, EvalRow row, out object? value)
        => row.Fields.TryGetValue(name, out value);

    private static bool TryResolveUnqualifiedColumn(string columnName, EvalRow row, out object? value)
        => row.Fields.TryGetValue(columnName, out value);

    private static bool TryResolveColumnFromSources(string columnName, EvalRow row, out object? value)
    {
        foreach (var source in row.Sources.Values)
        {
            if (TryResolveColumnFromSource(source, columnName, row, out value))
                return true;
        }

        value = null;
        return false;
    }

    private static object? ResolveColumn(
        string? qualifier,
        string col,
        EvalRow row)
    {
        col = col.NormalizeName();

        if (!string.IsNullOrWhiteSpace(qualifier))
            return ResolveQualifiedColumn(qualifier!, col, row, out _);

        // sem qualifier: tenta unqualified (você já expõe no AddFields)
        if (TryResolveUnqualifiedColumn(col, row, out var value))
            return value;

        return TryResolveColumnFromSources(col, row, out value)
            ? value
            : null;
    }
    private static object? ResolveQualifiedColumn(
        string qualifier,
        string col,
        EvalRow row,
        out Source? src)
    {
        qualifier = qualifier.NormalizeName();

        var lastQualifier = GetLastQualifierSegment(qualifier);
        src = FindQualifiedSource(qualifier, lastQualifier, row);

        if (src is null)
        {
            if (TryResolveDirectQualifiedField(qualifier, lastQualifier, col, row, out var directValue))
                return directValue;

            return null;
        }

        // wildcard qualificado? (U.*) -> aqui não resolve valor, quem trata é o SELECT planner
        if (col == "*") return null;

        return TryResolveColumnFromSource(src, col, row, out var resolvedValue)
            ? resolvedValue
            : null;
    }

    private static string GetLastQualifierSegment(string qualifier)
        => qualifier.Contains('.')
            ? qualifier.Split('.').Last()
            : qualifier;

    private static Source? FindQualifiedSource(
        string qualifier,
        string lastQualifier,
        EvalRow row)
    {
        if (row.Sources.TryGetValue(qualifier, out var source)
            || row.Sources.TryGetValue(lastQualifier, out source))
        {
            return source;
        }

        return row.Sources.Values.FirstOrDefault(candidate =>
            candidate.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
            || candidate.Name.Equals(lastQualifier, StringComparison.OrdinalIgnoreCase));
    }

    private static bool TryResolveDirectQualifiedField(
        string qualifier,
        string lastQualifier,
        string columnName,
        EvalRow row,
        out object? value)
    {
        if (row.Fields.TryGetValue($"{lastQualifier}.{columnName}", out value))
            return true;

        return row.Fields.TryGetValue($"{qualifier}.{columnName}", out value);
    }

    private static bool TryResolveColumnFromSource(
        Source source,
        string columnName,
        EvalRow row,
        out object? value)
    {
        if (row.Fields.TryGetValue($"{source.Alias}.{columnName}", out value))
            return true;

        var matchedColumn = source.ColumnNames.FirstOrDefault(name => name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
        if (matchedColumn is not null)
            return row.Fields.TryGetValue($"{source.Alias}.{matchedColumn}", out value);

        value = null;
        return false;
    }

    private static TableResultMock ApplyDistinct(
        TableResultMock res,
        ISqlDialect? dialect)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outRows = new List<Dictionary<int, object?>>();

        foreach (var row in res)
        {
            if (seen.Add(BuildDistinctRowKey(row, res.Columns.Count, dialect)))
                outRows.Add(row);
        }

        res.Clear();
        foreach (var r in outRows) res.Add(r);
        return res;
    }

    private static string BuildDistinctRowKey(
        Dictionary<int, object?> row,
        int columnCount,
        ISqlDialect? dialect)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < columnCount; i++)
        {
            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(NormalizeDistinctKey(row.TryGetValue(i, out var value) ? value : null, dialect));
        }

        return builder.ToString();
    }

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
                        dict[$"{Alias}.{col}"] = row.TryGetValue(idx, out var v)
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

    private sealed class EvalGroup
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

    //private sealed class ArrayObjectEqualityComparer : IEqualityComparer<object?[]>
    //{
    //    /// <summary>
    //    /// Auto-generated summary.
    //    /// </summary>
    //    public static readonly ArrayObjectEqualityComparer Instance = new();

    //    /// <summary>
    //    /// Auto-generated summary.
    //    /// </summary>
    //    public bool Equals(object?[]? x, object?[]? y)
    //    {
    //        if (ReferenceEquals(x, y)) return true;
    //        if (x is null || y is null) return false;
    //        if (x.Length != y.Length) return false;

    //        for (int i = 0; i < x.Length; i++)
    //        {
    //            if (!Equals(x[i], y[i])) return false;
    //        }
    //        return true;
    //    }

    //    /// <summary>
    //    /// Auto-generated summary.
    //    /// </summary>
    //    public int GetHashCode(object?[] obj)
    //    {
    //        unchecked
    //        {
    //            int h = 17;
    //            for (int i = 0; i < obj.Length; i++)
    //                h = (h * 31) + (obj[i]?.GetHashCode() ?? 0);
    //            return h;
    //        }
    //    }
    //}



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
