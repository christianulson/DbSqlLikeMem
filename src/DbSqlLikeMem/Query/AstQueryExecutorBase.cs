using DbSqlLikeMem.Interfaces;
using System.Diagnostics;
using DbSqlLikeMem.Models;
using System.Collections.Concurrent;
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
    private static readonly ConcurrentDictionary<Type, Func<string, object, SqlExpr>> _parseWhereDelegateCache = new();
    private static readonly Regex _sqlCalcFoundRowsRegex = new(
        @"\bSQL_CALC_FOUND_ROWS\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly DbConnectionMockBase _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));
    private readonly IDataParameterCollection _pars = pars ?? throw new ArgumentNullException(nameof(pars));
    private readonly object _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    private ISqlDialect? Dialect => _dialect as ISqlDialect;
    private readonly ConcurrentDictionary<string, bool> _existsSubqueryCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, List<object?>> _inSubqueryFirstColumnCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ScalarSubqueryCacheEntry> _scalarSubqueryCache = new(StringComparer.Ordinal);


    private static readonly HashSet<string> _aggFns = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG"
    };
    private static readonly HashSet<string> _sqlAliasReservedTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","JOIN","INNER","LEFT","RIGHT","FULL","CROSS","ON","WHERE","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","UNION","ALL","AS","USING","WHEN","THEN","ELSE","END"
    };

    // Dialect-aware expression parsing without hard dependency on a specific dialect type.
    // We resolve SqlExpressionParser.ParseWhere(string, <dialectType>) via reflection so the base
    // can be reused by SqlServer/Postgre/Oracle dialects.
    private SqlExpr ParseExpr(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            throw new ArgumentException("Expressão vazia.", nameof(raw));

        var dialectType = _dialect.GetType();
        var parserDelegate = _parseWhereDelegateCache.GetOrAdd(dialectType, CreateParseWhereDelegate);
        return parserDelegate(raw, _dialect);
    }

    private static Func<string, object, SqlExpr> CreateParseWhereDelegate(Type dialectType)
    {
        var mi = typeof(SqlExpressionParser)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m =>
            {
                if (m.Name != nameof(SqlExpressionParser.ParseWhere)) return false;
                var ps = m.GetParameters();
                if (ps.Length != 2) return false;
                if (ps[0].ParameterType != typeof(string)) return false;
                return ps[1].ParameterType.IsAssignableFrom(dialectType);
            });

        if (mi is null)
            throw new MissingMethodException(
                $"{nameof(SqlExpressionParser)}.{nameof(SqlExpressionParser.ParseWhere)}(string, {dialectType.Name}) não encontrado.");

        return (raw, dialectInstance) =>
        {
            try
            {
                return (SqlExpr)mi.Invoke(null, [raw, dialectInstance])!;
            }
            catch (TargetInvocationException tie) when (tie.InnerException is not null)
            {
                throw tie.InnerException;
            }
        };
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
            tables[0] = ExecuteSelect(parts[0]);
        }
        else
        {
            Parallel.For(0, parts.Count, i => tables[i] = ExecuteSelect(parts[i]));
        }

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
            result = ApplyOrderAndLimit(result, finalQ, ctes);
        }

        sw.Stop();

        var unionInputTables = parts.Sum(CountKnownInputTables);
        var unionEstimatedRead = parts.Sum(EstimateRowsRead);
        var unionMetrics = new SqlPlanRuntimeMetrics(
            InputTables: unionInputTables,
            EstimatedRowsRead: unionEstimatedRead,
            ActualRows: result.Count,
            ElapsedMs: sw.ElapsedMilliseconds);

        var plan = SqlExecutionPlanFormatter.FormatUnion(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            unionMetrics);
        result.ExecutionPlan = plan;
        _cnn.RegisterExecutionPlan(plan);
        _cnn.SetLastFoundRows(result.Count);

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
        var result = ExecuteSelect(q, null, null);
        sw.Stop();

        if (!HasSqlCalcFoundRows(q))
            _cnn.SetLastFoundRows(result.Count);

        var metrics = BuildPlanRuntimeMetrics(q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(q, metrics);
        var planWarnings = BuildPlanWarnings(q, metrics);
        var plan = SqlExecutionPlanFormatter.FormatSelect(q, metrics, indexRecommendations, planWarnings);
        result.ExecutionPlan = plan;
        _cnn.RegisterExecutionPlan(plan);
        return result;
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
        EvalRow? outerRow)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(selectQuery, nameof(selectQuery));

        // 0) Build CTE materializations (simple: materialize each CTE into a temp source)
        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var res = ExecuteSelect(cte.Query, ctes, outerRow);
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
        }

        // 1) FROM
        var rows = BuildFrom(
            selectQuery.Table,
            ctes,
            selectQuery.Where,
            hasOrderBy: selectQuery.OrderBy.Count > 0,
            hasGroupBy: selectQuery.GroupBy.Count > 0);

        // 2) JOINS
        foreach (var j in selectQuery.Joins)
            rows = ApplyJoin(
                rows,
                j,
                ctes,
                hasOrderBy: selectQuery.OrderBy.Count > 0,
                hasGroupBy: selectQuery.GroupBy.Count > 0);

        // 2.5) Correlated subquery: expose outer row fields/sources to subquery evaluation (EXISTS, IN subselect, etc.)
        if (outerRow is not null)
            rows = rows.Select(r => AttachOuterRow(r, outerRow));

        // 3) WHERE
        if (selectQuery.Where is not null)
            rows = ApplyRowPredicate(rows, selectQuery.Where, ctes);

        // 4) GROUP BY / HAVING / SELECT projection
        bool needsGrouping = selectQuery.GroupBy.Count > 0 || selectQuery.Having is not null || ContainsAggregate(selectQuery);

        if (needsGrouping)
            return ExecuteGroup(selectQuery, ctes, rows);

        // 5) Project non-grouped
        var projectedRows = rows as List<EvalRow> ?? rows.ToList();
        var projected = ProjectRows(selectQuery, projectedRows, ctes);

        // 6) DISTINCT
        if (selectQuery.Distinct)
            projected = ApplyDistinct(projected, Dialect);

        if (HasSqlCalcFoundRows(selectQuery))
            _cnn.SetLastFoundRows(projected.Count);

        // 7) ORDER BY / LIMIT
        projected = ApplyOrderAndLimit(projected, selectQuery, ctes);
        return projected;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows)
    {
        var keyExprs = BuildGroupByKeyExpressions(q);

        var grouped = rows.GroupBy(
            r => new GroupKey([.. keyExprs.Select(e => Eval(e, r, group: null, ctes))]),
            GroupKey.Comparer);

        // HAVING filter (MySQL: HAVING pode referenciar alias do SELECT)
        if (q.Having is null)
        {
            // Project grouped
            return ProjectGrouped(q, grouped, ctes);
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

        grouped = ApplyHavingPredicate(grouped, havingExpr, aliasExprs, ctes);

        // Project grouped
        return ProjectGrouped(q, grouped, ctes);
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
                    || WalkHasTemporalHavingReference(like.Pattern, dialect);

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
                    Pattern = RewriteHavingOrdinals(like.Pattern, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
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
        switch (expr)
        {
            case IdentifierExpr id:
                yield return id.Name;
                yield break;
            case ColumnExpr col:
                yield return string.IsNullOrWhiteSpace(col.Qualifier) ? col.Name : $"{col.Qualifier}.{col.Name}";
                yield break;
            case BinaryExpr b:
                foreach (var id in EnumerateIdentifiers(b.Left))
                    yield return id;
                foreach (var id in EnumerateIdentifiers(b.Right))
                    yield return id;
                yield break;
            case UnaryExpr u:
                foreach (var id in EnumerateIdentifiers(u.Expr))
                    yield return id;
                yield break;
            case IsNullExpr isn:
                foreach (var id in EnumerateIdentifiers(isn.Expr))
                    yield return id;
                yield break;
            case LikeExpr l:
                foreach (var id in EnumerateIdentifiers(l.Left))
                    yield return id;
                foreach (var id in EnumerateIdentifiers(l.Pattern))
                    yield return id;
                yield break;
            case InExpr i:
                foreach (var id in EnumerateIdentifiers(i.Left))
                    yield return id;
                foreach (var it in i.Items)
                    foreach (var id in EnumerateIdentifiers(it))
                        yield return id;
                yield break;
            case RowExpr r:
                foreach (var it in r.Items)
                    foreach (var id in EnumerateIdentifiers(it))
                        yield return id;
                yield break;
            case CaseExpr c:
                if (c.BaseExpr is not null)
                {
                    foreach (var id in EnumerateIdentifiers(c.BaseExpr))
                        yield return id;
                }

                foreach (var when in c.Whens)
                {
                    foreach (var id in EnumerateIdentifiers(when.When))
                        yield return id;
                    foreach (var id in EnumerateIdentifiers(when.Then))
                        yield return id;
                }

                if (c.ElseExpr is not null)
                {
                    foreach (var id in EnumerateIdentifiers(c.ElseExpr))
                        yield return id;
                }
                yield break;
            case FunctionCallExpr fn:
                foreach (var arg in fn.Args)
                    foreach (var id in EnumerateIdentifiers(arg))
                        yield return id;
                yield break;
            case CallExpr call:
                foreach (var arg in call.Args)
                    foreach (var id in EnumerateIdentifiers(arg))
                        yield return id;
                yield break;
            case JsonAccessExpr ja:
                foreach (var id in EnumerateIdentifiers(ja.Target))
                    yield return id;
                foreach (var id in EnumerateIdentifiers(ja.Path))
                    yield return id;
                yield break;
            case BetweenExpr bt:
                foreach (var id in EnumerateIdentifiers(bt.Expr))
                    yield return id;
                foreach (var id in EnumerateIdentifiers(bt.Low))
                    yield return id;
                foreach (var id in EnumerateIdentifiers(bt.High))
                    yield return id;
                yield break;
            default:
                yield break;
        }
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
            return rows.Count;

        if (aggregateFunction.Equals("SUM", StringComparison.OrdinalIgnoreCase))
        {
            decimal total = 0m;
            foreach (var row in rows)
            {
                var value = Eval(aggArgExpr, row, group: null, ctes);
                if (IsNullish(value))
                    continue;
                total += Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }

            return total;
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
        IDictionary<string, Source> ctes)
    {
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
            res = ApplyDistinct(res, Dialect);

        if (HasSqlCalcFoundRows(q))
            _cnn.SetLastFoundRows(res.Count);

        // ORDER / LIMIT
        res = ApplyOrderAndLimit(res, q, ctes);
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

            // Default: parse as expression (best-effort)
            SqlExpr exprAst;
#pragma warning disable CA1031
            try
            {
                exprAst = ParseExpr(raw);
            }
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(BuildSelectPlan)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"[SELECT-ITEM] Raw0='{raw0}' RawExpr='{raw}' AliasParsed='{asAlias ?? "null"}' AliasSi='{si.Alias ?? "null"}'");
                Console.WriteLine(e);
                exprAst = new RawSqlExpr(raw);
            }
#pragma warning restore CA1031

            // ✅ alias preferido: (1) parser do SELECT (si.Alias), (2) AS extraído do raw, (3) infer
            var preferred = si.Alias ?? asAlias ?? InferColumnAlias(raw);
            var tableAl = q.Table?.Alias ?? q.Table?.Name ?? "";
            var colAlias = MakeUniqueAlias(cols, preferred, tableAl);
            var inferredDbType = InferDbTypeFromExpression(exprAst, sampleRows, ctes);

            cols.Add(new TableResultColMock(
                tableAlias: q.Table?.Alias ?? q.Table?.Name ?? "",
                columnAlias: colAlias,
                columnName: colAlias,
                columIndex: cols.Count,
                dbType: inferredDbType,
                isNullable: true));

            if (exprAst is WindowFunctionExpr w)
            {
                if (!(Dialect?.SupportsWindowFunctions ?? true)
                    || !(Dialect?.SupportsWindowFunction(w.Name) ?? true))
                    throw SqlUnsupported.ForDialect(
                        Dialect!,
                        $"window functions ({w.Name})");

                EnsureWindowFunctionArgumentsAtRuntime(w);

                // slot.Map preenchido depois (quando tivermos todas as rows)
                var slot = new WindowSlot
                {
                    Expr = w,
                    Map = new Dictionary<EvalRow, object?>(ReferenceEqualityComparer<EvalRow>.Instance)
                };
                windowSlots.Add(slot);

                evals.Add((r, g) => slot.Map.TryGetValue(r, out var v) ? v : null);
            }
            else
            {
                evals.Add((r, g) => Eval(exprAst, r, g, ctes));
            }
        }

#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine("RESULT COLUMNS:");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        foreach (var c in cols)
            Console.WriteLine($" - {c.ColumnAlias}");

        return new SelectPlan { Columns = cols, Evaluators = evals, WindowSlots = windowSlots };
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
                return (expr0, alias0);

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
            || s.Equals("DESC", StringComparison.OrdinalIgnoreCase);
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
        IDictionary<string, Source> ctes)
    {
        // ORDER BY (aliases/ordinals/expressions) + LIMIT/OFFSET

        // LIMIT/OFFSET sem ORDER BY ainda precisa aplicar
        if (q.OrderBy.Count == 0)
        {
            ApplyLimit(res, q);
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
            ApplyLimit(res, q);
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

        ApplyLimit(res, q);
        return res;
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

        return _scalarSubqueryCache.GetOrAdd(
            cacheKey,
            _ =>
            {
                var r = ExecuteSelect(GetSingleSubqueryOrThrow(sq, "EVAL subquery"), ctes, row);
                var value = r.Count > 0 && r[0].TryGetValue(0, out var v) ? v : null;
                return new ScalarSubqueryCacheEntry(value);
            }).Value;
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
                if (SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(
                    Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para avaliação de função temporal."),
                    id.Name,
                    out var temporalIdentifierValue))
                    return temporalIdentifierValue;

                if (IsSqlServerRowCountIdentifier(id.Name, Dialect))
                    return _cnn.GetLastFoundRows();

                return ResolveIdentifier(id.Name, row);

            case ColumnExpr col:
                return ResolveColumn(col.Qualifier, col.Name, row);

            case StarExpr:
                // only meaningful inside COUNT(*)
                return "*";

            case IsNullExpr isn:
                {
                    var v1 = Eval(isn.Expr, row, group, ctes);
                    var isNull = v1 is null || v1 is DBNull;
                    return isn.Negated ? !isNull : isNull;
                }

            case LikeExpr like:
                {
                    var left = Eval(like.Left, row, group, ctes)?.ToString() ?? "";
                    var pat = Eval(like.Pattern, row, group, ctes)?.ToString() ?? "";
                    return left.Like(pat, Dialect);
                }

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
                if (u.Expr is InExpr notInExpr)
                    return EvalNotIn(notInExpr, row, group, ctes);

                return !Eval(u.Expr, row, group, ctes).ToBool();

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
                if (!Dialect!.SupportsJsonArrowOperators)
                    throw SqlUnsupported.ForDialect(Dialect, "JSON -> / ->> / #> / #>> operators");

                var mapped = MapJsonAccess(ja);
                return Eval(mapped, row, group, ctes);
            case FunctionCallExpr fn:
                return EvalFunction(fn, row, group, ctes);
            case CallExpr ce:
                return EvalCall(ce, row, group, ctes);
            case BetweenExpr b:
                return EvalBetween(b, row, group, ctes);
            case SubqueryExpr sq:
                return EvalScalarSubquery(sq, ctes, row);
            case RowExpr re:
                return re.Items.Select(it => Eval(it, row, group, ctes)).ToArray();

            case RawSqlExpr:
                // unsupported expression (e.g. CAST(x AS CHAR)): best-effort: null
                return null;

            default:
                throw new InvalidOperationException($"Expr não suportada no executor: {expr.GetType().Name}");
        }
    }

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
        if (b.Op == SqlBinaryOp.And)
            return Eval(b.Left, row, group, ctes).ToBool() && Eval(b.Right, row, group, ctes).ToBool();

        if (b.Op == SqlBinaryOp.Or)
            return Eval(b.Left, row, group, ctes).ToBool()
                || Eval(b.Right, row, group, ctes).ToBool();

        var l = Eval(b.Left, row, group, ctes);
        var r = Eval(b.Right, row, group, ctes);

        // arithmetic: NULL propagates (MySQL)
        if (b.Op is SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide)
        {
            if (l is null || r is null) return null;

            if (l is DateTime dt && r is IntervalValue interval)
            {
                return b.Op switch
                {
                    SqlBinaryOp.Add => dt.Add(interval.Span),
                    SqlBinaryOp.Subtract => dt.Subtract(interval.Span),
                    _ => throw new InvalidOperationException("op aritmético inválido")
                };
            }

            decimal ToDec(object v)
            {
                if (v is decimal dd) return dd;
                if (v is byte or sbyte or short or ushort or int or uint or long or ulong)
                    return Convert.ToDecimal(v, CultureInfo.InvariantCulture);
                if (v is float ff) return Convert.ToDecimal(ff, CultureInfo.InvariantCulture);
                if (v is double db) return Convert.ToDecimal(db, CultureInfo.InvariantCulture);
                if (v is string s && decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                    return parsed;
                throw new InvalidOperationException($"Não consigo converter '{v}' para número.");
            }

            var a = ToDec(l);
            var b2 = ToDec(r);

            return b.Op switch
            {
                SqlBinaryOp.Add => a + b2,
                SqlBinaryOp.Subtract => a - b2,
                SqlBinaryOp.Multiply => a * b2,
                SqlBinaryOp.Divide => b2 == 0m ? null : a / b2,
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
        }

        // NULL-safe equality
        if (b.Op == SqlBinaryOp.NullSafeEq)
        {
            if (l is null && r is null) return true;
            if (l is null || r is null) return false;
            return l.Compare(r, Dialect) == 0;
        }

        if (l is null || l is DBNull || r is null || r is DBNull)
        {
            // SQL: comparisons with NULL => false (except IS NULL handled elsewhere)
            return false;
        }

        var cmp = l.Compare(r, Dialect);

        return b.Op switch
        {
            SqlBinaryOp.Eq => cmp == 0,
            SqlBinaryOp.Neq => cmp != 0,
            SqlBinaryOp.Greater => cmp > 0,
            SqlBinaryOp.GreaterOrEqual => cmp >= 0,
            SqlBinaryOp.Less => cmp < 0,
            SqlBinaryOp.LessOrEqual => cmp <= 0,
            SqlBinaryOp.Regexp => EvalRegexp(l, r, Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para REGEXP.")),
            _ => throw new InvalidOperationException($"Binary op não suportado: {b.Op}")
        };
    }

    private static bool EvalRegexp(object l, object r, ISqlDialect dialect)
    {
        try
        {
            return Regex.IsMatch(l.ToString() ?? "", r.ToString() ?? "");
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

        // IN (subquery)
        if (i.Items.Count == 1 && i.Items[0] is SubqueryExpr sq)
        {
            var subqueryValues = GetOrEvaluateInSubqueryFirstColumnValues(sq, row, ctes);

            // MySQL: IN(subquery) considera a 1a coluna retornada
            foreach (var v in subqueryValues)
            {
                if (v is null || v is DBNull)
                {
                    hasNullCandidate = true;
                    continue;
                }

                if (leftVal.EqualsSql(v, Dialect))
                    return new InMembershipState(Matched: true, HasNullCandidate: hasNullCandidate);
            }

            return new InMembershipState(Matched: false, HasNullCandidate: hasNullCandidate);
        }

        // IN (item1, item2, ...)
        foreach (var it in i.Items)
        {
            var v = Eval(it, row, group, ctes);

            // ✅ Caso especial: IN @ids onde @ids é lista/array
            // Expande IEnumerable (mas não string) como múltiplos candidatos.
            if (v is System.Collections.IEnumerable ie && v is not string)
            {
                foreach (var item in ie)
                {
                    var cand = item;
                    if (cand is null || cand is DBNull)
                    {
                        hasNullCandidate = true;
                        continue;
                    }

                    // Row IN Row (quando o parametro é lista de tuples/rows)
                    if (leftVal is object?[] la2 && cand is object?[] ra2)
                    {
                        if (HasNullElement(la2) || HasNullElement(ra2))
                        {
                            hasNullCandidate = true;
                            continue;
                        }

                        if (la2.Length == ra2.Length && !la2.Where((t, idx) => !t.EqualsSql(ra2[idx], Dialect)).Any())
                            return new InMembershipState(Matched: true, HasNullCandidate: hasNullCandidate);
                        continue;
                    }

                    if (leftVal.EqualsSql(cand, Dialect))
                        return new InMembershipState(Matched: true, HasNullCandidate: hasNullCandidate);
                }

                continue; // não cai no EqualsSql(v) do enumerable inteiro
            }

            // Row IN Row (normal)
            if (leftVal is object?[] la && v is object?[] ra)
            {
                if (HasNullElement(la) || HasNullElement(ra))
                {
                    hasNullCandidate = true;
                    continue;
                }

                if (la.Length == ra.Length && !la.Where((t, idx) => !t.EqualsSql(ra[idx], Dialect)).Any())
                    return new InMembershipState(Matched: true, HasNullCandidate: hasNullCandidate);
                continue;
            }

            if (v is null || v is DBNull)
            {
                hasNullCandidate = true;
                continue;
            }

            if (leftVal.EqualsSql(v, Dialect))
                return new InMembershipState(Matched: true, HasNullCandidate: hasNullCandidate);
        }

        return new InMembershipState(Matched: false, HasNullCandidate: hasNullCandidate);
    }

    /// <summary>
    /// EN: Checks whether an object array contains at least one SQL NULL-like value.
    /// PT: Verifica se um array de objetos contém ao menos um valor SQL nulo.
    /// </summary>
    private static bool HasNullElement(object?[] values)
        => values.Any(static v => v is null || v is DBNull);

    private bool EvalExists(
        ExistsExpr ex,
        EvalRow row,
        IDictionary<string, Source> ctes)
    {
        var sq = ex.Subquery;
        var cacheKey = BuildCorrelatedSubqueryCacheKey("EXISTS", sq.Sql, row);

        return _existsSubqueryCache.GetOrAdd(
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
            quantified.Quantifier == SqlQuantifier.Any
                ? $"QANY_{quantified.Op}"
                : $"QALL_{quantified.Op}",
            row,
            ctes);

        if (quantified.Quantifier == SqlQuantifier.Any)
        {
            foreach (var candidate in candidates)
            {
                var truth = EvaluateScalarComparisonTruthValue(quantified.Op, leftVal, candidate);
                if (truth == SqlTruthValue.True)
                    return true;
            }

            // UNKNOWN in WHERE is filtered out (same observable result as false).
            return false;
        }

        // ALL on empty set is true (vacuous truth).
        if (candidates.Count == 0)
            return true;

        var hasUnknownAll = false;
        foreach (var candidate in candidates)
        {
            var truth = EvaluateScalarComparisonTruthValue(quantified.Op, leftVal, candidate);
            if (truth == SqlTruthValue.False)
                return false;

            if (truth == SqlTruthValue.Unknown)
                hasUnknownAll = true;
        }

        // If no FALSE but at least one UNKNOWN => UNKNOWN => filtered out in WHERE.
        return !hasUnknownAll;
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

        return _inSubqueryFirstColumnCache.GetOrAdd(
            cacheKey,
            _ =>
            {
                var sub = ExecuteSelect(GetSingleSubqueryOrThrow(sq, operation), ctes, row);
                var values = new List<object?>(sub.Count);
                foreach (var sr in sub)
                    values.Add(sr.TryGetValue(0, out var cell) ? cell : null);

                return values;
            });
    }

    /// <summary>
    /// EN: Builds a deterministic cache key for correlated subquery evaluation using operation kind, raw subquery text and normalized outer-row values.
    /// PT: Monta uma chave de cache determinística para avaliação de subquery correlacionada usando tipo de operação, texto bruto da subquery e valores normalizados da linha externa.
    /// </summary>
    private static string BuildCorrelatedSubqueryCacheKey(string operation, string? subquerySql, EvalRow row)
    {
        var normalizedSubquerySql = NormalizeSubquerySqlForCacheKey(subquerySql ?? string.Empty);
        normalizedSubquerySql = NormalizeOperationSpecificSubquerySqlForCacheKey(operation, normalizedSubquerySql);
        var sb = new StringBuilder();
        sb.Append(operation);
        sb.Append('\u001F');
        sb.Append(normalizedSubquerySql);
        sb.Append('\u001F');

        var cacheFields = GetCorrelatedSubqueryCacheFields(subquerySql ?? string.Empty, row);
        foreach (var kv in cacheFields)
        {
            sb.Append(kv.Key);
            sb.Append('=');
            sb.Append(NormalizeSubqueryCacheValue(kv.Value));
            sb.Append('\u001E');
        }

        return sb.ToString();
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
        var allFields = row.Fields
            .OrderBy(static kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (allFields.Count == 0 || string.IsNullOrWhiteSpace(subquerySql))
            return allFields;

        var normalizedSql = NormalizeSqlIdentifierSpacing(subquerySql);
        var qualifiedIdentifiers = ExtractQualifiedSqlIdentifiers(normalizedSql);
        var qualifiedMatches = allFields
            .Where(static kv => kv.Key.IndexOf('.') >= 0)
            .Where(kv => qualifiedIdentifiers.Contains(kv.Key))
            .ToList();

        if (qualifiedMatches.Count > 0)
            return qualifiedMatches;

        var unqualifiedMatches = allFields
            .Where(static kv => kv.Key.IndexOf('.') < 0)
            .Where(kv => ContainsSqlIdentifierToken(normalizedSql, kv.Key))
            .ToList();

        if (unqualifiedMatches.Count > 0)
            return unqualifiedMatches;

        // If we cannot match any outer identifier but SQL still appears to reference outer qualifiers,
        // keep conservative behavior and include all fields to avoid stale cross-row reuse.
        return ContainsPotentialOuterQualifierReference(normalizedSql, allFields)
            ? allFields
            : [];
    }

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

        var matches = Regex.Matches(
            sql,
            @"(?<![A-Za-z0-9_$])([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)(?![A-Za-z0-9_$])",
            RegexOptions.CultureInvariant);

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
            var ch = normalized[i];

            if (ch == '\'')
            {
                previousWasSpace = false;
                i = AppendQuotedSegment(normalized, i, '\'', sb);
                continue;
            }

            if (ch == '"')
            {
                previousWasSpace = false;
                i = AppendQuotedSegment(normalized, i, '"', sb);
                continue;
            }

            if (ch == '`')
            {
                previousWasSpace = false;
                i = AppendQuotedSegment(normalized, i, '`', sb);
                continue;
            }

            if (ch == '[')
            {
                previousWasSpace = false;
                i = AppendBracketIdentifierSegment(normalized, i, sb);
                continue;
            }

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
            sb.Append(ch);

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
            sb.Append(ch);
            if (ch == ']')
                return i;
            i++;
        }

        return sql.Length - 1;
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

        var matches = Regex.Matches(
            sql,
            @"\b(?:FROM|JOIN)\s+(?:[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?([A-Z_][A-Z0-9_$]*)\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

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
            var ch = sql[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = AppendQuotedSegment(sql, i, ch, sb);
                continue;
            }

            if (ch == '[')
            {
                i = AppendBracketIdentifierSegment(sql, i, sb);
                continue;
            }

            if (IsAliasQualifierReferenceAt(sql, i, alias))
            {
                sb.Append(replacementAlias);
                sb.Append('.');
                i += alias.Length;
                continue;
            }

            sb.Append(ch);
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
            var ch = sql[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = AppendQuotedSegment(sql, i, ch, sb);
                continue;
            }

            if (ch == '[')
            {
                i = AppendBracketIdentifierSegment(sql, i, sb);
                continue;
            }

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
            var ch = sql[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                previousWasSpace = false;
                i = AppendQuotedSegment(sql, i, ch, sb);
                continue;
            }

            if (ch == '[')
            {
                previousWasSpace = false;
                i = AppendBracketIdentifierSegment(sql, i, sb);
                continue;
            }

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
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        if (!TryFindTopLevelKeywordIndex(sql, "SELECT", 0, out var selectIndex))
            return sql;

        var afterSelect = selectIndex + "SELECT".Length;
        if (!TryFindTopLevelKeywordIndex(sql, "FROM", afterSelect, out var fromIndex))
            return sql;

        if (fromIndex <= afterSelect)
            return sql;

        return string.Concat(
            sql.Substring(0, afterSelect),
            " <EXISTS_PAYLOAD> ",
            sql.Substring(fromIndex));
    }

    /// <summary>
    /// EN: Canonicalizes top-level SELECT projection aliases by removing explicit AS aliases while preserving projection expressions and relational clauses.
    /// PT: Canoniza aliases da projeção SELECT no nível de topo removendo aliases explícitos AS e preservando expressões projetadas e cláusulas relacionais.
    /// </summary>
    private static string NormalizeSelectProjectionAliasesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        if (!TryFindTopLevelKeywordIndex(sql, "SELECT", 0, out var selectIndex))
            return sql;

        var afterSelect = selectIndex + "SELECT".Length;
        if (!TryFindTopLevelKeywordIndex(sql, "FROM", afterSelect, out var fromIndex))
            return sql;

        if (fromIndex <= afterSelect)
            return sql;

        var payload = sql[afterSelect..fromIndex];
        var normalizedPayload = NormalizeSelectListAliasesForCacheKey(payload);
        return string.Concat(
            sql.Substring(0, afterSelect),
            " ",
            normalizedPayload,
            " ",
            sql.Substring(fromIndex));
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
            var ch = text[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(text, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(text, i);
                continue;
            }

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
        if (!TryFindTopLevelKeywordIndex(trimmed, "AS", 0, out var asIndex))
            return trimmed;

        var beforeAs = trimmed[..asIndex].TrimEnd();
        var aliasPart = trimmed[(asIndex + 2)..].Trim();
        if (!IsValidExplicitAliasToken(aliasPart))
            return trimmed;

        return beforeAs;
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
        if (Regex.IsMatch(trimmed, @"^[A-Z_][A-Z0-9_$]*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
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
            var ch = sql[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(sql, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(sql, i);
                continue;
            }

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

    /// <summary>
    /// EN: Normalizes commutative top-level AND chains in WHERE/HAVING clauses so equivalent predicate orderings reuse the same cache-key SQL fragment.
    /// PT: Normaliza cadeias comutativas de AND no topo em cláusulas WHERE/HAVING para que ordenações equivalentes de predicados reutilizem o mesmo fragmento SQL da chave de cache.
    /// </summary>
    private static string NormalizeCommutativeAndClausesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var wherePattern = @"\bWHERE\s+(?<predicate>.+?)(?=(?:\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))";
        var havingPattern = @"\bHAVING\s+(?<predicate>.+?)(?=(?:\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))";

        var normalizedWhere = Regex.Replace(
            sql,
            wherePattern,
            m => "WHERE " + NormalizeTopLevelAndPredicateForCacheKey(m.Groups["predicate"].Value),
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);

        return Regex.Replace(
            normalizedWhere,
            havingPattern,
            m => "HAVING " + NormalizeTopLevelAndPredicateForCacheKey(m.Groups["predicate"].Value),
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    }

    /// <summary>
    /// EN: Canonicalizes a predicate text by sorting top-level AND segments when safe (no top-level OR and no BETWEEN token).
    /// PT: Canoniza um texto de predicado ordenando segmentos AND de topo quando seguro (sem OR de topo e sem token BETWEEN).
    /// </summary>
    private static string NormalizeTopLevelAndPredicateForCacheKey(string predicate)
    {
        if (string.IsNullOrWhiteSpace(predicate))
            return string.Empty;

        var trimmedPredicate = TrimRedundantOuterParentheses(predicate);
        if (ContainsTokenOutsideQuotedSegments(trimmedPredicate, "OR"))
            return trimmedPredicate;

        if (ContainsTokenOutsideQuotedSegments(trimmedPredicate, "BETWEEN"))
            return trimmedPredicate;

        var segments = SplitTopLevelAndSegments(trimmedPredicate);
        if (segments.Count <= 1)
            return trimmedPredicate;

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
            var ch = predicate[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(predicate, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(predicate, i);
                continue;
            }

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
                var segment = NormalizePredicateSegmentForCacheKey(predicate[start..i]);
                if (segment.Length > 0)
                    segments.Add(segment);

                start = i + 3;
                i += 2;
            }
        }

        var lastSegment = NormalizePredicateSegmentForCacheKey(predicate[start..]);
        if (lastSegment.Length > 0)
            segments.Add(lastSegment);

        return segments;
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

        if (!TryFindStandaloneTopLevelEqualityOperator(segment, out var equalityIndex))
            return segment;

        var left = TrimRedundantOuterParentheses(segment[..equalityIndex]);
        var right = TrimRedundantOuterParentheses(segment[(equalityIndex + 1)..]);
        if (left.Length == 0 || right.Length == 0)
            return segment;

        return StringComparer.Ordinal.Compare(left, right) <= 0
            ? $"{left} = {right}"
            : $"{right} = {left}";
    }

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
            var ch = segment[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(segment, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(segment, i);
                continue;
            }

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
            var ch = expression[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(expression, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(expression, i);
                continue;
            }

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
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(token))
            return false;

        var depth = 0;
        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (ch == '\'' || ch == '"' || ch == '`')
            {
                i = FindQuotedSegmentEndIndex(sql, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = FindBracketSegmentEndIndex(sql, i);
                continue;
            }

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

            if (depth == 0 && MatchesKeywordTokenAt(sql, i, token))
                return true;
        }

        return false;
    }

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
    {
        _existsSubqueryCache.Clear();
        _inSubqueryFirstColumnCache.Clear();
        _scalarSubqueryCache.Clear();
    }


    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Searched CASE: CASE WHEN cond THEN x ...
        if (c.BaseExpr is null)
        {
            foreach (var wt in c.Whens)
            {
                if (Eval(wt.When, row, group, ctes).ToBool())
                    return Eval(wt.Then, row, group, ctes);
            }

            return c.ElseExpr is not null
                ? Eval(c.ElseExpr, row, group, ctes)
                : null;
        }

        // Simple CASE: CASE base WHEN val THEN x ...
        var baseVal = Eval(c.BaseExpr, row, group, ctes);

        foreach (var wt in c.Whens)
        {
            var whenVal = Eval(wt.When, row, group, ctes);

            // MySQL uses '=' comparison semantics here: NULL never matches (even NULL)
            if (baseVal is null || baseVal is DBNull || whenVal is null || whenVal is DBNull)
                continue;

            if (baseVal.Compare(whenVal, Dialect) == 0)
                return Eval(wt.Then, row, group, ctes);
        }

        return c.ElseExpr is not null
            ? Eval(c.ElseExpr, row, group, ctes)
            : null;
    }

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

        if (fn.Name.Equals("FIND_IN_SET", StringComparison.OrdinalIgnoreCase))
        {
            var needle = EvalArg(0)?.ToString() ?? "";
            var hay = EvalArg(1)?.ToString() ?? "";
            var parts = hay.Split(',').Select(_=>_.Trim()).ToArray();
            var idx = Array.FindIndex(parts, p => string.Equals(p, needle, StringComparison.OrdinalIgnoreCase));
            return idx >= 0 ? idx + 1 : 0;
        }

        if (IsFoundRowsEquivalentFunction(fn.Name, dialect))
        {
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            return _cnn.GetLastFoundRows();
        }

        var isIf = fn.Name.Equals("IF", StringComparison.OrdinalIgnoreCase);
        var isIif = fn.Name.Equals("IIF", StringComparison.OrdinalIgnoreCase);
        if ((isIf && dialect.SupportsIfFunction) || (isIif && dialect.SupportsIifFunction))
        {
            var cond = EvalArg(0).ToBool();
            return cond ? EvalArg(1) : EvalArg(2);
        }

        if (!fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase)
            && dialect.NullSubstituteFunctionNames.Any(n => n.Equals(fn.Name, StringComparison.OrdinalIgnoreCase)))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? EvalArg(1) : v;
        }

        if (fn.Name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase))
        {
            for (int i = 0; i < fn.Args.Count; i++)
            {
                var v = EvalArg(i);
                if (!IsNullish(v)) return v;
            }
            return null;
        }

        if (fn.Name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
        {
            var left = EvalArg(0);
            var right = EvalArg(1);

            if (IsNullish(left) || IsNullish(right))
                return left;

            return left!.Compare(right!, Dialect) == 0 ? null : left;
        }

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

        if (fn.Name.Equals("LOWER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("LCASE", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
#pragma warning disable CA1308 // Normalize strings to uppercase
            return IsNullish(v) ? null : v!.ToString()!.ToLowerInvariant();
#pragma warning restore CA1308 // Normalize strings to uppercase
        }

        if (fn.Name.Equals("UPPER", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UCASE", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : v!.ToString()!.ToUpperInvariant();
        }

        if (fn.Name.Equals("TRIM", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : v!.ToString()!.Trim();
        }

        if (fn.Name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("CHAR_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var v = EvalArg(0);
            return IsNullish(v) ? null : (long)(v!.ToString()!.Length);
        }

        if (fn.Name.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase))
        {
            var s = EvalArg(0);
            if (IsNullish(s)) return null;
            var str = s!.ToString() ?? "";

            var pos = EvalArg(1);
            if (IsNullish(pos)) return null;

            var start = Convert.ToInt32(pos.ToDec()) - 1; // MySQL is 1-based
            if (start < 0) start = 0;
            if (start >= str.Length) return "";

            var lenObj = EvalArg(2);
            if (IsNullish(lenObj))
                return str[start..];

            var len = Convert.ToInt32(lenObj.ToDec());
            if (len <= 0) return "";
            if (start + len > str.Length) len = str.Length - start;

            return str.Substring(start, len);
        }

        if (fn.Name.Equals("REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            var s = EvalArg(0);
            var from = EvalArg(1);
            var to = EvalArg(2);

            if (IsNullish(s) || IsNullish(from) || IsNullish(to))
                return null;

            return (s!.ToString() ?? "")
                .Replace(from!.ToString() ?? "", to!.ToString() ?? "");
        }


        var dateAddResult = TryEvalDateAddFunction(fn, row, group, ctes, EvalArg, out var handledDateAdd);
        if (handledDateAdd)
            return dateAddResult;

        if ((fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase))
            && fn.Args.Count >= 1)
        {
            var baseVal = EvalArg(0);
            if (IsNullish(baseVal)) return null;
            if (!TryCoerceDateTime(baseVal, out var dt))
                return null;

            // Minimal SQLite-like modifier support: '+N day|hour|minute|second|month|year'
            for (int i = 1; i < fn.Args.Count; i++)
            {
                var modifier = EvalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(modifier))
                    continue;

                if (!TryParseDateModifier(modifier!, out var unit, out var amount))
                    continue;

                dt = ApplyDateDelta(dt, unit, amount);
            }

            if (fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase))
                return dt.Date;

            return dt;
        }

        
        if (fn.Name.Equals("FIELD", StringComparison.OrdinalIgnoreCase))
        {
            // MySQL FIELD(str, str1, str2, ...) returns 1-based index of str in the list; 0 if not found
            var target = EvalArg(0);
            if (IsNullish(target)) return 0;

            for (int ai = 1; ai < fn.Args.Count; ai++)
            {
                var cand = EvalArg(ai);
                if (IsNullish(cand)) continue;
                if (target.EqualsSql(cand, Dialect))
                    return ai; // 1-based
            }
            return 0;
        }

// Unknown scalar => null (don't explode tests)
        return null;

        object? EvalArg(int i) => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null;
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
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{GetType().Name}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine(e);
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
        if (fn.Name.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase))
        {
            if (!(Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para operações de data.")).SupportsDateAddFunction("DATE_ADD"))
                return null;
            var baseVal = evalArg(0);
            if (IsNullish(baseVal) || !TryCoerceDateTime(baseVal, out var dt))
                return null;

            var itExpr = fn.Args.Count > 1 ? fn.Args[1] : null;
            if (itExpr is not CallExpr ce || !ce.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase) || ce.Args.Count < 2)
                return dt;

            var nObj = Eval(ce.Args[0], row, group, ctes);
            var unit = ce.Args[1] is RawSqlExpr rx ? rx.Sql : Eval(ce.Args[1], row, group, ctes)?.ToString() ?? "DAY";
            var n = Convert.ToInt32((nObj ?? 0m).ToDec());
            return ApplyDateDelta(dt, unit, n);
        }

        if (fn.Name.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase))
        {
            var featureName = fn.Name.ToUpperInvariant();
            if (!(Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para operações de data.")).SupportsDateAddFunction(featureName))
                return null;
            if (fn.Args.Count < 3)
                return null;

            var unit = GetDateAddUnit(fn.Args[0], row, group, ctes);
            var amountObj = evalArg(1);
            var baseVal = evalArg(2);
            if (IsNullish(baseVal) || !TryCoerceDateTime(baseVal, out var dt))
                return null;

            var n = Convert.ToInt32((amountObj ?? 0m).ToDec());
            return ApplyDateDelta(dt, unit, n);
        }

        handled = false;
        return null;
    }


    private object? TryEvalJsonAndNumberFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out bool handled)
    {
        handled = true;

        if (fn.Name.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Name.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
                && !dialect.SupportsJsonExtractFunction
                && !dialect.SupportsJsonArrowOperators)
                throw SqlUnsupported.ForDialect(dialect, "JSON_EXTRACT");

            if (fn.Name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase) && !dialect.SupportsJsonValueFunction)
                throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");

            object? json = evalArg(0);
            var path = evalArg(1)?.ToString();
            if (IsNullish(json) || string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return TryReadJsonPathValue(json!, path!);
            }
#pragma warning disable CA1031
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine(e);
                return null;
            }
#pragma warning restore CA1031
        }

        if (fn.Name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsOpenJsonFunction)
                throw SqlUnsupported.ForDialect(dialect, "OPENJSON");

            object? json = evalArg(0);
            return IsNullish(json) ? null : json?.ToString();
        }

        if (fn.Name.Equals("JSON_UNQUOTE", StringComparison.OrdinalIgnoreCase))
        {
            object? v = evalArg(0);
            if (IsNullish(v)) return null;
            var s = v!.ToString() ?? string.Empty;
            if (s.Length >= 2 && ((s[0] == '"' && s[^1] == '"') || (s[0] == '\'' && s[^1] == '\'')))
                return s[1..^1];
            return s;
        }

        if (fn.Name.Equals("TO_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            var v = evalArg(0);
            if (IsNullish(v)) return null;

            if (v is byte or sbyte or short or ushort or int or uint or long or ulong)
                return Convert.ToInt64(v, CultureInfo.InvariantCulture);
            if (v is decimal or double or float)
                return Convert.ToDecimal(v, CultureInfo.InvariantCulture);

            var text = v?.ToString();
            if (string.IsNullOrWhiteSpace(text))
                return null;
            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var li))
                return li;
            if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec))
                return dec;
            return null;
        }

        handled = false;
        return null;
    }

    private object? TryEvalConcatFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out bool handled)
    {
        handled = true;

        if (fn.Name.Equals("CONCAT", StringComparison.OrdinalIgnoreCase))
        {
            var parts = new string[fn.Args.Count];
            for (int i = 0; i < fn.Args.Count; i++)
            {
                var v = evalArg(i);
                if (IsNullish(v))
                {
                    if (Dialect!.ConcatReturnsNullOnNullInput)
                        return null;

                    parts[i] = string.Empty;
                    continue;
                }

#pragma warning disable CA1508 // Avoid dead conditional code
                parts[i] = v?.ToString() ?? string.Empty;
#pragma warning restore CA1508 // Avoid dead conditional code
            }
            return string.Concat(parts);
        }

        if (fn.Name.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase))
        {
            var sep = evalArg(0);
            if (IsNullish(sep)) return null;
#pragma warning disable CA1508 // Avoid dead conditional code
            var separator = sep?.ToString() ?? string.Empty;
#pragma warning restore CA1508 // Avoid dead conditional code

            var parts = new List<string>();
            for (int i = 1; i < fn.Args.Count; i++)
            {
                var v = evalArg(i);
                if (IsNullish(v)) continue;
#pragma warning disable CA1508 // Avoid dead conditional code
                parts.Add(v?.ToString() ?? string.Empty);
#pragma warning restore CA1508 // Avoid dead conditional code
            }

            return string.Join(separator, parts);
        }

        handled = false;
        return null;
    }

    private static object? TryReadJsonPathValue(object json, string path)
    {
        var jsonStr = json.ToString() ?? "";
        using var doc = System.Text.Json.JsonDocument.Parse(jsonStr);

        // suporta só "$.a.b" e "$.a" (suficiente pro corpus)
        if (!path.StartsWith("$.", StringComparison.Ordinal))
            return null;

        var cur = doc.RootElement;
        var segs = path[2..].Split('.').Select(_=>_.Trim()).ToArray();
        foreach (var s in segs)
        {
            if (cur.ValueKind != System.Text.Json.JsonValueKind.Object)
                return null;

            if (!cur.TryGetProperty(s.Trim('"'), out var next))
                return null;

            cur = next;
        }

        return cur.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => cur.GetString(),
            System.Text.Json.JsonValueKind.Number => cur.TryGetInt64(out var li) ? li : cur.GetDecimal(),
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Null => null,
            _ => cur.ToString()
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

        var m = Regex.Match(
            modifier.Trim(),
            @"^(?<amount>[+-]?\d+)\s*(?<unit>\w+)s?$",
            RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

        if (!m.Success)
            return false;

        if (!int.TryParse(m.Groups["amount"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        unit = m.Groups["unit"].Value;
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

        raw = raw!.Trim();
        if (raw.Contains('\\'))
            raw = raw.Replace("\\", string.Empty);

        var match = Regex.Match(raw, @"^(?<num>-?\d+(?:\.\d+)?)\s*(?<unit>[a-zA-Z]+)$");
        if (!match.Success)
            return null;

        if (!decimal.TryParse(match.Groups["num"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
            return null;

        var unit = match.Groups["unit"].Value.ToUpperInvariant();
        var span = unit switch
        {
            "DAY" or "DAYS" => TimeSpan.FromDays((double)value),
            "HOUR" or "HOURS" => TimeSpan.FromHours((double)value),
            "MINUTE" or "MINUTES" => TimeSpan.FromMinutes((double)value),
            "SECOND" or "SECONDS" => TimeSpan.FromSeconds((double)value),
            _ => (TimeSpan?)null
        };

        return span is null ? null : new IntervalValue(span.Value);
    }

    private object? EvalAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var name = fn.Name.ToUpperInvariant();

        // COUNT -> SEMPRE long
        if (!TryEvalAggrageteCount(fn, group, ctes, name, out var value))
            return value;

        if (fn.Args.Count == 0)
            return null;

        // coleta valores (não-nulos)
        var values = GetNotNullValues(fn, group, ctes);

        if (values.Count == 0)
        {
            // MySQL: SUM/AVG/MIN/MAX sobre conjunto vazio (ou tudo NULL) => NULL
            return null;
        }

        return name switch
        {
            "SUM" =>
                values.Sum(_ => _.ToDec()),                 // decimal
            "AVG" =>
                values.Sum(_ => _.ToDec()) / values.Count,  // decimal
            "MIN" =>
                values.Min(_ => _.ToDec()),                 // decimal (consistente pro teu teste)
            "MAX" =>
                values.Max(_ => _.ToDec()),                 // decimal
            "GROUP_CONCAT" =>
                EvalStringAggregate(values, fn.Args.Count > 1 && group.Rows.Count > 0 ? Eval(fn.Args[1], group.Rows[0], null, ctes) : null, ","),
            "STRING_AGG" =>
                EvalStringAggregate(values, fn.Args.Count > 1 && group.Rows.Count > 0 ? Eval(fn.Args[1], group.Rows[0], null, ctes) : null, ","),
            "LISTAGG" =>
                EvalStringAggregate(values, fn.Args.Count > 1 && group.Rows.Count > 0 ? Eval(fn.Args[1], group.Rows[0], null, ctes) : null, string.Empty),
            _ => null
        };
    }

    private object? EvalAggregate(
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        var name = fn.Name.ToUpperInvariant();

        // COUNT(DISTINCT ...)
        if (name == "COUNT" && fn.Distinct)
            return EvalCountDistinct(fn, group, ctes);

        if (name is "GROUP_CONCAT" or "STRING_AGG" or "LISTAGG")
        {
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

        // MySQL permite COUNT(DISTINCT expr) e COUNT(DISTINCT expr1, expr2)
        var set = new HashSet<string>(StringComparer.Ordinal);

        foreach (var r in group.Rows)
        {
            // avalia todos os args
            var vals = fn.Args.Select(a => Eval(a, r, null, ctes)).ToArray();

            // Regra prática compatível: se algum é NULL => ignora a linha
            // (na prática, COUNT(DISTINCT ...) ignora NULL; com múltiplas colunas,
            // qualquer NULL “mata” a tupla)
            if (vals.Any(IsNullish))
                continue;

            var key = string.Join("\u001F", vals.Select(v => NormalizeDistinctKey(v, Dialect)));
            set.Add(key);
        }

        return set.Count;
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

        object? separatorObj = null;
        if (fn.Args.Count > 1 && group.Rows.Count > 0)
            separatorObj = Eval(fn.Args[1], group.Rows[0], null, ctes);

        IEnumerable<EvalRow> rows = group.Rows;
        if (fn.WithinGroupOrderBy is { Count: > 0 })
        {
            rows = rows.OrderBy(
                row => fn.WithinGroupOrderBy
                    .Select(order => Eval(order.Expr, row, null, ctes))
                    .ToArray(),
                Comparer<object?[]>.Create((left, right) =>
                {
                    for (var i = 0; i < fn.WithinGroupOrderBy.Count; i++)
                    {
                        var cmp = CompareSql(left[i], right[i]);
                        if (cmp == 0)
                            continue;

                        if (fn.WithinGroupOrderBy[i].Desc)
                            cmp = -cmp;

                        return cmp;
                    }

                    return 0;
                }));
        }

        var values = new List<object?>();
        var distinct = new HashSet<string>(StringComparer.Ordinal);
        foreach (var r in rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (IsNullish(v))
                continue;

            if (!fn.Distinct)
            {
                values.Add(v);
                continue;
            }

            var normalized = NormalizeDistinctKey(v, Dialect);
            if (distinct.Add(normalized))
                values.Add(v);
        }

        var defaultSeparator = name == "LISTAGG" ? string.Empty : ",";
        return EvalStringAggregate(values, separatorObj, defaultSeparator);
    }

    private bool TryEvalAggrageteCount(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        out object? value)
    {
        if (name != "COUNT")
        {
            value = null;
            return true;
        }

        if (fn.Args.Count == 0)
        {
            value = (long)group.Rows.Count;
            return false;
        }

        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
        {
            value = (long)group.Rows.Count;
            return false;
        }

        long c = 0;
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v)) c++;
        }
        value = c;
        return false;
    }

    private List<object?> GetNotNullValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
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
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                var e = ParseExpr(exprRaw);
                if (WalkHasAggregate(e) || (e is RawSqlExpr && LooksLikeAggregateExpression(exprRaw)))
                    return true;
            }
            catch (Exception e)
            {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"{GetType().Name}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                Console.WriteLine(e);

                // fallback: preserve aggregate semantics even when expression parsing fails.
                if (LooksLikeAggregateExpression(exprRaw))
                    return true;
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }
        return q.Having is not null
            && WalkHasAggregate(q.Having);
    }

    private static bool LooksLikeAggregateExpression(string exprRaw)
        => Regex.IsMatch(
            exprRaw,
            @"\b(COUNT|SUM|MIN|MAX|AVG|GROUP_CONCAT|STRING_AGG|LISTAGG)\s*\(",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static bool WalkHasAggregate(SqlExpr e) => e switch
    {
        // ✅ NOVO: o parser usa CallExpr (COUNT, SUM, etc.)
        CallExpr c => _aggFnsStatic.Contains(c.Name) || c.Args.Any(WalkHasAggregate),

        // já existia
        FunctionCallExpr f => _aggFnsStatic.Contains(f.Name) || f.Args.Any(WalkHasAggregate),
        BinaryExpr b => WalkHasAggregate(b.Left) || WalkHasAggregate(b.Right),
        UnaryExpr u => WalkHasAggregate(u.Expr),
        LikeExpr l => WalkHasAggregate(l.Left) || WalkHasAggregate(l.Pattern),
        InExpr i => WalkHasAggregate(i.Left) || i.Items.Any(WalkHasAggregate),
        IsNullExpr isn => WalkHasAggregate(isn.Expr),
        QuantifiedComparisonExpr q => WalkHasAggregate(q.Left) || WalkHasAggregate(q.Subquery),

        // ⚠️ cuidado: isso aqui tá “agressivo”
        // EXISTS não é aggregate. Mas você provavelmente usa isso como “precisa de contexto especial”.
        // Vou deixar como está pra não quebrar outras coisas.
        ExistsExpr => true,

        RowExpr r => r.Items.Any(WalkHasAggregate),
        _ => false
    };

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
        {
            return _pars.Count > 0 ? ((IDataParameter)_pars[0]!).Value : null;
        }

        var norm = name.TrimStart('@', ':');

        foreach (IDataParameter p in _pars)
        {
            var pn = p.ParameterName?.TrimStart('@', ':');
            if (string.Equals(pn, norm, StringComparison.OrdinalIgnoreCase))
                return p.Value is DBNull ? null : p.Value;
        }

        return null;
    }

    private static object? ResolveIdentifier(
        string name,
        EvalRow row)
    {
        // "a.b"
        var dot = name.IndexOf('.');
        if (dot >= 0)
            return ResolveColumn(name[..dot], name[(dot + 1)..], row);

        // unqualified: try exact match in any source (first wins)
        foreach (var src in row.Sources.Values)
        {
            var key = $"{src.Alias}.{name}";
            if (row.Fields.TryGetValue(key, out var v))
                return v;
        }

        // maybe projected alias
        if (row.Fields.TryGetValue(name, out var v2))
            return v2;

        return null;
    }

    private static object? ResolveColumn(
        string? qualifier,
        string col,
        EvalRow row)
    {
        col = col.NormalizeName();

        Source? src = null;

        if (!string.IsNullOrWhiteSpace(qualifier))
            return ResolveQualifiedColumn(qualifier!, col, row, out src);

        // sem qualifier: tenta unqualified (você já expõe no AddFields)
        if (row.Fields.TryGetValue(col, out var v2))
            return v2;

        // fallback: tenta varrer sources
        foreach (var vlr in row.Sources.Values)
        {
            var key = $"{vlr.Alias}.{col}";
            if (row.Fields.TryGetValue(key, out var v))
                return v;

            var colHit = vlr.ColumnNames.FirstOrDefault(c => c.Equals(col, StringComparison.OrdinalIgnoreCase));
            if (colHit is not null && row.Fields.TryGetValue($"{vlr.Alias}.{colHit}", out v))
                return v;
        }

        return null;


    }
    private static object? ResolveQualifiedColumn(
        string qualifier,
        string col,
        EvalRow row,
        out Source? src)
    {
        qualifier = qualifier.NormalizeName();

        // se vier db.table, pega o "table"
        var lastQual = qualifier.Contains('.')
            ? qualifier.Split('.').Last()
            : qualifier;

        // 1) tenta por alias
        if (!row.Sources.TryGetValue(qualifier, out src) &&
            !row.Sources.TryGetValue(lastQual, out src))
        {
            // 2) tenta por nome físico da tabela
            src = row.Sources.Values.FirstOrDefault(s =>
                s.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase) ||
                s.Name.Equals(lastQual, StringComparison.OrdinalIgnoreCase));
        }

        if (src is null)
        {
            // Fallback: even if Sources doesn't contain the qualifier, fields may already have qualified keys
            var directKey = $"{lastQual}.{col}";
            if (row.Fields.TryGetValue(directKey, out var dv))
                return dv;
            directKey = $"{qualifier}.{col}";
            if (row.Fields.TryGetValue(directKey, out dv))
                return dv;
            return null;
        }

        // wildcard qualificado? (U.*) -> aqui não resolve valor, quem trata é o SELECT planner
        if (col == "*") return null;

        var key = $"{src.Alias}.{col}";
        if (row.Fields.TryGetValue(key, out var v))
            return v;

        var colHit = src.ColumnNames.FirstOrDefault(c => c.Equals(col, StringComparison.OrdinalIgnoreCase));
        if (colHit is not null && row.Fields.TryGetValue($"{src.Alias}.{colHit}", out v))
            return v;

        return null;
    }

    private static TableResultMock ApplyDistinct(
        TableResultMock res,
        ISqlDialect? dialect)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outRows = new List<Dictionary<int, object?>>();

        foreach (var row in res)
        {
            var key = new string[res.Columns.Count];
            for (int i = 0; i < res.Columns.Count; i++)
                key[i] = NormalizeDistinctKey(row.TryGetValue(i, out var v) ? v : null, dialect);

            if (seen.Add(string.Join("\u001F", key)))
                outRows.Add(row);
        }

        res.Clear();
        foreach (var r in outRows) res.Add(r);
        return res;
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

    private sealed record ScalarSubqueryCacheEntry(object? Value);
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
           && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
           && identifier.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);

    private static bool IsFoundRowsEquivalentFunction(string functionName, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        if (functionName.Equals("FOUND_ROWS", StringComparison.OrdinalIgnoreCase))
            return true;

        return dialect.Name.ToLowerInvariant() switch
        {
            "mysql" => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase),
            "sqlserver" => functionName.Equals("ROWCOUNT", StringComparison.OrdinalIgnoreCase),
            "postgresql" => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase),
            "oracle" => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase),
            "db2" => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase),
            "sqlite" => functionName.Equals("CHANGES", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    private static bool HasSqlCalcFoundRows(SqlSelectQuery query)
        => !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}
