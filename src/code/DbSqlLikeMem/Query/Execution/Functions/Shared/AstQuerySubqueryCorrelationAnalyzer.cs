using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySubqueryCorrelationAnalyzer
{
    private static readonly Regex _identifierRegex = new(
        "(?<![A-Za-z0-9_$])(?:\\[[^\\]]+\\]|\"[^\"]+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\\s*\\.\\s*(?:\\[[^\\]]+\\]|\"[^\"]+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*))?(?![A-Za-z0-9_$])",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    internal static bool CanReuseWithoutOuterRow(
        SubqueryExpr sq,
        EvalRow row,
        IDictionary<string, Source> ctes,
        Func<SqlTableSource, IDictionary<string, Source>, Source> resolveSource)
    {
        if (row.Fields.Count == 0 && row.Sources.Count == 0)
            return true;

        var outer = OuterReferenceScope.Create(row);
        if (outer.IsEmpty)
            return true;

        return AnalyzeQuery(sq.Parsed, outer) == CorrelationResult.Uncorrelated;
    }

    private static CorrelationResult AnalyzeQuery(
        SqlQueryBase query,
        OuterReferenceScope outer)
        => query switch
        {
            SqlSelectQuery select => AnalyzeSelect(select, outer),
            SqlUnionQuery union => AnalyzeUnion(union, outer),
            _ => CorrelationResult.Unknown
        };

    private static CorrelationResult AnalyzeUnion(
        SqlUnionQuery union,
        OuterReferenceScope outer)
    {
        foreach (var part in union.Parts)
        {
            var partResult = AnalyzeSelect(part, outer);
            if (partResult != CorrelationResult.Uncorrelated)
                return partResult;
        }

        foreach (var orderBy in union.OrderBy)
        {
            var orderByResult = AnalyzeRaw(orderBy.Raw, new LocalReferenceScope(outer));
            if (orderByResult != CorrelationResult.Uncorrelated)
                return orderByResult;
        }

        return AnalyzeRowLimit(union.RowLimit, new LocalReferenceScope(outer));
    }

    private static CorrelationResult AnalyzeSelect(
        SqlSelectQuery query,
        OuterReferenceScope outer)
    {
        if (query.Ctes.Count > 0)
            return CorrelationResult.Unknown;

        var local = new LocalReferenceScope(outer);
        var sourceResult = AddLocalSource(query.Table, local);
        if (sourceResult != CorrelationResult.Uncorrelated)
            return sourceResult;

        foreach (var join in query.Joins)
        {
            if (join.Type is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
                return CorrelationResult.Unknown;

            sourceResult = AddLocalSource(join.Table, local);
            if (sourceResult != CorrelationResult.Uncorrelated)
                return sourceResult;
        }

        var result = AnalyzeExpression(query.Where, local);
        if (result != CorrelationResult.Uncorrelated)
            return result;

        foreach (var join in query.Joins)
        {
            result = AnalyzeExpression(join.On, local);
            if (result != CorrelationResult.Uncorrelated)
                return result;
        }

        foreach (var item in query.SelectItems)
        {
            result = AnalyzeRaw(item.Raw, local);
            if (result != CorrelationResult.Uncorrelated)
                return result;
        }

        foreach (var groupBy in query.GroupBy)
        {
            result = AnalyzeRaw(groupBy, local);
            if (result != CorrelationResult.Uncorrelated)
                return result;
        }

        foreach (var orderBy in query.OrderBy)
        {
            result = AnalyzeRaw(orderBy.Raw, local);
            if (result != CorrelationResult.Uncorrelated)
                return result;
        }

        result = AnalyzeExpression(query.Having, local);
        return result != CorrelationResult.Uncorrelated
            ? result
            : AnalyzeRowLimit(query.RowLimit, local);
    }

    private static CorrelationResult AddLocalSource(
        SqlTableSource? table,
        LocalReferenceScope local)
    {
        if (table is null)
            return CorrelationResult.Uncorrelated;

        if (table.IsLateral
            || table.Derived is not null
            || table.DerivedUnion is not null
            || table.TableFunction is not null
            || table.OpenJsonWithClause is not null
            || table.JsonTableClause is not null
            || table.Pivot is not null
            || table.Unpivot is not null)
        {
            return CorrelationResult.Unknown;
        }

        local.AddQualifier(table.Alias);
        local.AddQualifier(table.Name);
        local.HasUnknownLocalColumns = true;
        return CorrelationResult.Uncorrelated;
    }

    private static CorrelationResult AnalyzeExpression(SqlExpr? expr, LocalReferenceScope local)
    {
        if (expr is null)
            return CorrelationResult.Uncorrelated;

        switch (expr)
        {
            case LiteralExpr:
            case ParameterExpr:
            case StarExpr:
                return CorrelationResult.Uncorrelated;
            case IdentifierExpr identifier:
                return AnalyzeIdentifier(identifier.Name, local);
            case ColumnExpr column:
                return AnalyzeQualifiedIdentifier(column.Qualifier, column.Name, local);
            case RawSqlExpr raw:
                return AnalyzeRaw(raw.Sql, local);
            case UnaryExpr unary:
                return AnalyzeExpression(unary.Expr, local);
            case BinaryExpr binary:
                return Combine(AnalyzeExpression(binary.Left, local), AnalyzeExpression(binary.Right, local));
            case InExpr inExpr:
                return AnalyzeExpressionList(inExpr.Items, AnalyzeExpression(inExpr.Left, local), local);
            case LikeExpr like:
                return Combine(
                    AnalyzeExpression(like.Left, local),
                    AnalyzeExpression(like.Pattern, local),
                    AnalyzeExpression(like.Escape, local));
            case IsNullExpr isNull:
                return AnalyzeExpression(isNull.Expr, local);
            case RowExpr row:
                return AnalyzeExpressionList(row.Items, CorrelationResult.Uncorrelated, local);
            case FunctionCallExpr function:
                return AnalyzeExpressionList(function.Args, CorrelationResult.Uncorrelated, local);
            case CallExpr call:
                return AnalyzeCall(call, local);
            case WindowFunctionExpr window:
                return AnalyzeWindowFunction(window, local);
            case JsonAccessExpr json:
                return Combine(AnalyzeExpression(json.Target, local), AnalyzeExpression(json.Path, local));
            case BetweenExpr between:
                return Combine(
                    AnalyzeExpression(between.Expr, local),
                    AnalyzeExpression(between.Low, local),
                    AnalyzeExpression(between.High, local));
            case CaseExpr caseExpr:
                return AnalyzeCase(caseExpr, local);
            case SubqueryExpr:
            case ExistsExpr:
            case QuantifiedComparisonExpr:
                return CorrelationResult.Unknown;
            default:
                return CorrelationResult.Unknown;
        }
    }

    private static CorrelationResult AnalyzeCall(CallExpr call, LocalReferenceScope local)
    {
        var result = AnalyzeExpressionList(call.Args, CorrelationResult.Uncorrelated, local);
        result = Combine(result, AnalyzeExpression(call.Filter, local));
        if (call.WithinGroupOrderBy is not null)
        {
            foreach (var item in call.WithinGroupOrderBy)
                result = Combine(result, AnalyzeExpression(item.Expr, local));
        }

        return result;
    }

    private static CorrelationResult AnalyzeWindowFunction(WindowFunctionExpr window, LocalReferenceScope local)
    {
        var result = AnalyzeExpressionList(window.Args, CorrelationResult.Uncorrelated, local);
        foreach (var partition in window.Spec.PartitionBy)
            result = Combine(result, AnalyzeExpression(partition, local));

        foreach (var orderBy in window.Spec.OrderBy)
            result = Combine(result, AnalyzeExpression(orderBy.Expr, local));

        return result;
    }

    private static CorrelationResult AnalyzeCase(CaseExpr caseExpr, LocalReferenceScope local)
    {
        var result = AnalyzeExpression(caseExpr.BaseExpr, local);
        foreach (var item in caseExpr.Whens)
        {
            result = Combine(
                result,
                AnalyzeExpression(item.When, local),
                AnalyzeExpression(item.Then, local));
        }

        return Combine(result, AnalyzeExpression(caseExpr.ElseExpr, local));
    }

    private static CorrelationResult AnalyzeExpressionList(
        IReadOnlyList<SqlExpr> expressions,
        CorrelationResult current,
        LocalReferenceScope local)
    {
        var result = current;
        foreach (var expression in expressions)
            result = Combine(result, AnalyzeExpression(expression, local));

        return result;
    }

    private static CorrelationResult AnalyzeRowLimit(SqlRowLimit? rowLimit, LocalReferenceScope local)
        => rowLimit switch
        {
            null => CorrelationResult.Uncorrelated,
            SqlLimitOffset limit => Combine(AnalyzeExpression(limit.Count, local), AnalyzeExpression(limit.Offset, local)),
            SqlTop top => AnalyzeExpression(top.Count, local),
            SqlFetch fetch => Combine(AnalyzeExpression(fetch.Count, local), AnalyzeExpression(fetch.Offset, local)),
            _ => CorrelationResult.Unknown
        };

    private static CorrelationResult AnalyzeIdentifier(string rawName, LocalReferenceScope local)
    {
        var normalized = NormalizeIdentifier(rawName);
        if (string.IsNullOrEmpty(normalized))
            return CorrelationResult.Uncorrelated;

        var dot = normalized.IndexOf('.');
        return dot > 0 && dot + 1 < normalized.Length
            ? AnalyzeQualifiedIdentifier(normalized[..dot], normalized[(dot + 1)..], local)
            : AnalyzeUnqualifiedIdentifier(normalized, local);
    }

    private static CorrelationResult AnalyzeQualifiedIdentifier(
        string rawQualifier,
        string rawName,
        LocalReferenceScope local)
    {
        var qualifier = NormalizeIdentifier(rawQualifier);
        if (local.IsLocalQualifier(qualifier))
            return CorrelationResult.Uncorrelated;

        return local.Outer.ContainsQualifier(qualifier)
            ? CorrelationResult.Correlated
            : CorrelationResult.Uncorrelated;
    }

    private static CorrelationResult AnalyzeUnqualifiedIdentifier(string rawName, LocalReferenceScope local)
    {
        var name = NormalizeIdentifier(rawName);
        if (!local.Outer.ContainsColumn(name))
            return CorrelationResult.Uncorrelated;

        if (local.ContainsColumn(name))
            return CorrelationResult.Uncorrelated;

        return local.HasUnknownLocalColumns
            ? CorrelationResult.Unknown
            : CorrelationResult.Correlated;
    }

    private static CorrelationResult AnalyzeRaw(string raw, LocalReferenceScope local)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return CorrelationResult.Uncorrelated;

        var result = CorrelationResult.Uncorrelated;
        foreach (Match match in _identifierRegex.Matches(raw))
        {
            var token = match.Value;
            var tokenResult = AnalyzeIdentifier(token, local);
            result = Combine(result, tokenResult);
            if (result != CorrelationResult.Uncorrelated)
                return result;
        }

        return result;
    }

    private static CorrelationResult Combine(params CorrelationResult[] values)
    {
        var sawUnknown = false;
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] == CorrelationResult.Correlated)
                return CorrelationResult.Correlated;

            sawUnknown |= values[i] == CorrelationResult.Unknown;
        }

        return sawUnknown ? CorrelationResult.Unknown : CorrelationResult.Uncorrelated;
    }

    private static string NormalizeIdentifier(string value)
        => value.NormalizeName();

    private enum CorrelationResult
    {
        Uncorrelated,
        Correlated,
        Unknown
    }

    private sealed class OuterReferenceScope
    {
        private readonly HashSet<string> _qualifiers = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _columns = new(StringComparer.OrdinalIgnoreCase);

        internal bool IsEmpty => _qualifiers.Count == 0 && _columns.Count == 0;

        internal static OuterReferenceScope Create(EvalRow row)
        {
            var scope = new OuterReferenceScope();
            foreach (var sourceName in row.Sources.Keys)
                scope.AddQualifier(sourceName);

            foreach (var fieldName in row.Fields.Keys)
            {
                var normalized = NormalizeIdentifier(fieldName);
                var dot = normalized.LastIndexOf('.');
                if (dot > 0 && dot + 1 < normalized.Length)
                {
                    scope.AddQualifier(normalized[..dot]);
                    scope.AddColumn(normalized[(dot + 1)..]);
                }
                else
                {
                    scope.AddColumn(normalized);
                }
            }

            return scope;
        }

        internal bool ContainsQualifier(string qualifier)
            => !string.IsNullOrWhiteSpace(qualifier) && _qualifiers.Contains(NormalizeIdentifier(qualifier));

        internal bool ContainsColumn(string column)
            => !string.IsNullOrWhiteSpace(column) && _columns.Contains(NormalizeIdentifier(column));

        private void AddQualifier(string? qualifier)
        {
            if (!string.IsNullOrWhiteSpace(qualifier))
                _qualifiers.Add(NormalizeIdentifier(qualifier!));
        }

        private void AddColumn(string? column)
        {
            if (!string.IsNullOrWhiteSpace(column))
                _columns.Add(NormalizeIdentifier(column!));
        }
    }

    private sealed class LocalReferenceScope
    {
        private readonly HashSet<string> _qualifiers = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _columns = new(StringComparer.OrdinalIgnoreCase);

        internal LocalReferenceScope(OuterReferenceScope outer)
        {
            Outer = outer;
        }

        internal OuterReferenceScope Outer { get; }

        internal bool HasUnknownLocalColumns { get; set; }

        internal void AddQualifier(string? qualifier)
        {
            if (!string.IsNullOrWhiteSpace(qualifier))
                _qualifiers.Add(NormalizeIdentifier(qualifier!));
        }

        internal void AddColumns(IReadOnlyList<string> columns)
        {
            foreach (var column in columns)
            {
                if (!string.IsNullOrWhiteSpace(column))
                    _columns.Add(NormalizeIdentifier(column));
            }
        }

        internal bool IsLocalQualifier(string qualifier)
            => !string.IsNullOrWhiteSpace(qualifier) && _qualifiers.Contains(NormalizeIdentifier(qualifier));

        internal bool ContainsColumn(string column)
            => !string.IsNullOrWhiteSpace(column) && _columns.Contains(NormalizeIdentifier(column));
    }
}
