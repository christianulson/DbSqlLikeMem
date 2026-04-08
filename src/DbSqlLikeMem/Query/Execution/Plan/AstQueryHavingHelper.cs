using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryHavingHelper(
    Func<ISqlDialect> getDialect,
    Func<string, SqlExpr> parseExpr,
    Func<string, string?, (string expr, string? alias)> splitTrailingAsAlias)
{
    private readonly Func<ISqlDialect> _getDialect = getDialect;
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr;
    private readonly Func<string, string?, (string expr, string? alias)> _splitTrailingAsAlias = splitTrailingAsAlias;

    internal SqlExpr NormalizeHavingExpression(SqlExpr expr, SqlSelectQuery q)
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

        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para HAVING.");
        if (HasAggregateIdentifierOrTemporalReference(rewritten, dialect))
            return rewritten;

        throw new InvalidOperationException(
            "invalid: HAVING must reference grouped columns, projected aliases, aggregates, or valid ordinals");
    }

    internal void EnsureHavingIdentifiersAreBound(SqlExpr expr, EvalRow row, ISqlDialect dialect)
    {
        ValidateHavingIdentifiersAreBound(expr, row, dialect);
    }

    private void ValidateHavingIdentifiersAreBound(SqlExpr expr, EvalRow row, ISqlDialect dialect)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                ValidateHavingIdentifierIsBound(id.Name, row, dialect);
                return;
            case ColumnExpr col:
                ValidateHavingIdentifierIsBound(col, row, dialect);
                return;
            case UnaryExpr unary:
                ValidateHavingIdentifiersAreBound(unary.Expr, row, dialect);
                return;
            case IsNullExpr isNull:
                ValidateHavingIdentifiersAreBound(isNull.Expr, row, dialect);
                return;
            case BinaryExpr binary:
                ValidateHavingIdentifiersAreBound(binary.Left, row, dialect);
                ValidateHavingIdentifiersAreBound(binary.Right, row, dialect);
                return;
            case LikeExpr like:
                ValidateHavingIdentifiersAreBound(like.Left, row, dialect);
                ValidateHavingIdentifiersAreBound(like.Pattern, row, dialect);
                if (like.Escape is not null)
                    ValidateHavingIdentifiersAreBound(like.Escape, row, dialect);
                return;
            case InExpr inExpr:
                ValidateHavingIdentifiersAreBound(inExpr.Left, row, dialect);
                for (var idx = 0; idx < inExpr.Items.Count; idx++)
                    ValidateHavingIdentifiersAreBound(inExpr.Items[idx], row, dialect);
                return;
            case RowExpr rowExpr:
                for (var idx = 0; idx < rowExpr.Items.Count; idx++)
                    ValidateHavingIdentifiersAreBound(rowExpr.Items[idx], row, dialect);
                return;
            case BetweenExpr between:
                ValidateHavingIdentifiersAreBound(between.Expr, row, dialect);
                ValidateHavingIdentifiersAreBound(between.Low, row, dialect);
                ValidateHavingIdentifiersAreBound(between.High, row, dialect);
                return;
            case CaseExpr @case:
                if (@case.BaseExpr is not null)
                    ValidateHavingIdentifiersAreBound(@case.BaseExpr, row, dialect);

                for (var idx = 0; idx < @case.Whens.Count; idx++)
                {
                    var when = @case.Whens[idx];
                    ValidateHavingIdentifiersAreBound(when.When, row, dialect);
                    ValidateHavingIdentifiersAreBound(when.Then, row, dialect);
                }

                if (@case.ElseExpr is not null)
                    ValidateHavingIdentifiersAreBound(@case.ElseExpr, row, dialect);
                return;
            case FunctionCallExpr function:
                for (var idx = 0; idx < function.Args.Count; idx++)
                    ValidateHavingIdentifiersAreBound(function.Args[idx], row, dialect);
                return;
            case CallExpr call:
                for (var idx = 0; idx < call.Args.Count; idx++)
                    ValidateHavingIdentifiersAreBound(call.Args[idx], row, dialect);
                return;
            case JsonAccessExpr jsonAccess:
                ValidateHavingIdentifiersAreBound(jsonAccess.Target, row, dialect);
                ValidateHavingIdentifiersAreBound(jsonAccess.Path, row, dialect);
                return;
            default:
                return;
        }
    }

    private static void ValidateHavingIdentifierIsBound(string name, EvalRow row, ISqlDialect dialect)
    {
        if (IsHavingTemporalIdentifier(name, dialect))
            return;

        if (IsIdentifierBound(row, name))
            return;

        throw new InvalidOperationException($"invalid: HAVING reference '{name}' was not found in grouped projection");
    }

    private static void ValidateHavingIdentifierIsBound(ColumnExpr column, EvalRow row, ISqlDialect dialect)
    {
        if (IsHavingTemporalIdentifier(column.Name, dialect))
            return;

        if (IsIdentifierBound(row, column))
            return;

        throw new InvalidOperationException(
            $"invalid: HAVING reference '{column.Qualifier}.{column.Name}' was not found in grouped projection");
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
                    var (exprRaw, _) = _splitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                    usedOrdinal = true;
                    return _parseExpr(exprRaw);
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
                var rewrittenInItems = new SqlExpr[i.Items.Count];
                for (var idx = 0; idx < i.Items.Count; idx++)
                {
                    rewrittenInItems[idx] = RewriteHavingOrdinals(
                        i.Items[idx],
                        q,
                        ref usedOrdinal,
                        false,
                        ref outOfRangeOrdinal,
                        ref nonPositiveOrdinal);
                }
                return i with
                {
                    Left = RewriteHavingOrdinals(i.Left, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Items = rewrittenInItems
                };
            case RowExpr r:
                var rewrittenRowItems = new SqlExpr[r.Items.Count];
                for (var idx = 0; idx < r.Items.Count; idx++)
                {
                    rewrittenRowItems[idx] = RewriteHavingOrdinals(
                        r.Items[idx],
                        q,
                        ref usedOrdinal,
                        false,
                        ref outOfRangeOrdinal,
                        ref nonPositiveOrdinal);
                }
                return r with { Items = rewrittenRowItems };
            case BetweenExpr bt:
                return bt with
                {
                    Expr = RewriteHavingOrdinals(bt.Expr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Low = RewriteHavingOrdinals(bt.Low, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    High = RewriteHavingOrdinals(bt.High, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            case FunctionCallExpr fn:
                var rewrittenFnArgs = new SqlExpr[fn.Args.Count];
                for (var idx = 0; idx < fn.Args.Count; idx++)
                {
                    rewrittenFnArgs[idx] = RewriteHavingOrdinals(
                        fn.Args[idx],
                        q,
                        ref usedOrdinal,
                        false,
                        ref outOfRangeOrdinal,
                        ref nonPositiveOrdinal);
                }
                return fn with { Args = rewrittenFnArgs };
            case CallExpr call:
                var rewrittenCallArgs = new SqlExpr[call.Args.Count];
                for (var idx = 0; idx < call.Args.Count; idx++)
                {
                    rewrittenCallArgs[idx] = RewriteHavingOrdinals(
                        call.Args[idx],
                        q,
                        ref usedOrdinal,
                        false,
                        ref outOfRangeOrdinal,
                        ref nonPositiveOrdinal);
                }
                return call with { Args = rewrittenCallArgs };
            case CaseExpr c:
                var rewrittenWhens = new CaseWhenThen[c.Whens.Count];
                for (var idx = 0; idx < c.Whens.Count; idx++)
                {
                    var when = c.Whens[idx];
                    rewrittenWhens[idx] = when with
                    {
                        When = RewriteHavingOrdinals(when.When, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                        Then = RewriteHavingOrdinals(when.Then, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                    };
                }
                return c with
                {
                    BaseExpr = c.BaseExpr is null ? null : RewriteHavingOrdinals(c.BaseExpr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal),
                    Whens = rewrittenWhens,
                    ElseExpr = c.ElseExpr is null ? null : RewriteHavingOrdinals(c.ElseExpr, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal)
                };
            default:
                return expr;
        }
    }

    private static bool IsOrdinalCandidateSide(SqlBinaryOp op, bool leftSide)
        => op switch
        {
            SqlBinaryOp.Eq => leftSide,
            SqlBinaryOp.Neq => leftSide,
            SqlBinaryOp.Greater => leftSide,
            SqlBinaryOp.GreaterOrEqual => leftSide,
            SqlBinaryOp.Less => leftSide,
            SqlBinaryOp.LessOrEqual => leftSide,
            SqlBinaryOp.NullSafeEq => leftSide,
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

    private static bool IsHavingTemporalIdentifier(string name, ISqlDialect dialect)
        => dialect.AllowsTemporalIdentifier(name);

    private static bool IsIdentifierBound(EvalRow row, string name)
    {
        var sources = row.Sources;
        var dot = name.IndexOf('.');
        if (dot >= 0)
        {
            var qualifier = name[..dot].Trim();
            var col = name[(dot + 1)..].Trim();
            if (qualifier.Length == 0 || col.Length == 0)
                return false;

            if (sources.TryGetValue(qualifier, out var source)
                && source.TryGetQualifiedColumnName(col, out var qualifiedColumnName))
            {
                var qualifiedName = qualifiedColumnName ?? string.Empty;
                if (qualifiedName.Length > 0
                    && row.TryGetValue(qualifiedName, out _))
                {
                    return true;
                }
            }

            if (TryHasQualifiedField(row, col))
                return true;

            return false;
        }

        return row.TryGetValue(name, out _);
    }

    private static bool IsIdentifierBound(EvalRow row, ColumnExpr column)
    {
        var sources = row.Sources;
        if (string.IsNullOrWhiteSpace(column.Qualifier))
            return row.TryGetValue(column.Name, out _);

        if (sources.TryGetValue(column.Qualifier, out var source)
            && source.TryGetQualifiedColumnName(column.Name, out var qualifiedColumnName))
        {
            var qualifiedName = qualifiedColumnName ?? string.Empty;
            if (qualifiedName.Length > 0
                && row.TryGetValue(qualifiedName, out _))
            {
                return true;
            }
        }

        return TryHasQualifiedField(row, sources, column.Name);
    }

    private static bool TryHasQualifiedField(EvalRow row, string columnName)
        => TryHasQualifiedField(row, row.Sources, columnName);

    private static bool TryHasQualifiedField(
        EvalRow row,
        Dictionary<string, Source> sources,
        string columnName)
    {
        if (row.TryGetSingleSource(out var singleSource)
            && TryHasQualifiedFieldFromSource(singleSource!, row, columnName))
        {
            return true;
        }

        foreach (var source in sources.Values)
        {
            if (TryHasQualifiedFieldFromSource(source, row, columnName))
                return true;
        }

        return false;
    }

    private static bool TryHasQualifiedFieldFromSource(
        Source source,
        EvalRow row,
        string columnName)
    {
        if (!source.TryGetQualifiedColumnName(columnName, out var qualifiedName)
            || string.IsNullOrWhiteSpace(qualifiedName))
            return false;

        return row.Fields.ContainsKey(qualifiedName!);
    }

    private static bool HasAggregateIdentifierOrTemporalReference(SqlExpr expr, ISqlDialect dialect)
    {
        switch (expr)
        {
            case IdentifierExpr:
                return true;

            case ColumnExpr:
                return true;

            case FunctionCallExpr function:
                return HasAggregateFunctionCall(function.Name, function.Args, dialect);

            case CallExpr call:
                return HasAggregateFunctionCall(call.Name, call.Args, dialect);

            case BinaryExpr binary:
                return HasAggregateIdentifierOrTemporalReference(binary.Left, dialect)
                    || HasAggregateIdentifierOrTemporalReference(binary.Right, dialect);

            case UnaryExpr unary:
                return HasAggregateIdentifierOrTemporalReference(unary.Expr, dialect);

            case LikeExpr like:
                return HasAggregateIdentifierOrTemporalReference(like.Left, dialect)
                    || HasAggregateIdentifierOrTemporalReference(like.Pattern, dialect)
                    || (like.Escape is not null && HasAggregateIdentifierOrTemporalReference(like.Escape, dialect));

            case InExpr inExpr:
                if (HasAggregateIdentifierOrTemporalReference(inExpr.Left, dialect))
                    return true;

                for (var idx = 0; idx < inExpr.Items.Count; idx++)
                {
                    if (HasAggregateIdentifierOrTemporalReference(inExpr.Items[idx], dialect))
                        return true;
                }

                return false;

            case IsNullExpr isNull:
                return HasAggregateIdentifierOrTemporalReference(isNull.Expr, dialect);

            case QuantifiedComparisonExpr quantified:
                return HasAggregateIdentifierOrTemporalReference(quantified.Left, dialect);

            case RowExpr row:
                for (var idx = 0; idx < row.Items.Count; idx++)
                {
                    if (HasAggregateIdentifierOrTemporalReference(row.Items[idx], dialect))
                        return true;
                }

                return false;

            case BetweenExpr between:
                return HasAggregateIdentifierOrTemporalReference(between.Expr, dialect)
                    || HasAggregateIdentifierOrTemporalReference(between.Low, dialect)
                    || HasAggregateIdentifierOrTemporalReference(between.High, dialect);

            case CaseExpr @case:
                if (@case.BaseExpr is not null && HasAggregateIdentifierOrTemporalReference(@case.BaseExpr, dialect))
                    return true;

                for (var idx = 0; idx < @case.Whens.Count; idx++)
                {
                    var whenThen = @case.Whens[idx];
                    if (HasAggregateIdentifierOrTemporalReference(whenThen.When, dialect)
                        || HasAggregateIdentifierOrTemporalReference(whenThen.Then, dialect))
                    {
                        return true;
                    }
                }

                return @case.ElseExpr is not null && HasAggregateIdentifierOrTemporalReference(@case.ElseExpr, dialect);

            case JsonAccessExpr jsonAccess:
                return HasAggregateIdentifierOrTemporalReference(jsonAccess.Target, dialect)
                    || HasAggregateIdentifierOrTemporalReference(jsonAccess.Path, dialect);

            default:
                return false;
        }
    }

    private static bool HasAggregateFunctionCall(
        string name,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect)
    {
        if (AggregateFunctionCatalog.Contains(name))
            return true;

        if (args.Count == 0)
            return dialect.AllowsTemporalCall(name);

        for (var idx = 0; idx < args.Count; idx++)
        {
            if (HasAggregateIdentifierOrTemporalReference(args[idx], dialect))
                return true;
        }

        return false;
    }
}
