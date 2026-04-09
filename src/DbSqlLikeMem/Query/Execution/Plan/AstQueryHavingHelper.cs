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
                var inItemCount = inExpr.Items.Count;
                for (var idx = 0; idx < inItemCount; idx++)
                    ValidateHavingIdentifiersAreBound(inExpr.Items[idx], row, dialect);
                return;
            case RowExpr rowExpr:
                var rowItemCount = rowExpr.Items.Count;
                for (var idx = 0; idx < rowItemCount; idx++)
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

                var whens = @case.Whens;
                var whenCount = whens.Count;
                for (var idx = 0; idx < whenCount; idx++)
                {
                    var when = whens[idx];
                    ValidateHavingIdentifiersAreBound(when.When, row, dialect);
                    ValidateHavingIdentifiersAreBound(when.Then, row, dialect);
                }

                if (@case.ElseExpr is not null)
                    ValidateHavingIdentifiersAreBound(@case.ElseExpr, row, dialect);
                return;
            case FunctionCallExpr function:
                var functionArgCount = function.Args.Count;
                for (var idx = 0; idx < functionArgCount; idx++)
                    ValidateHavingIdentifiersAreBound(function.Args[idx], row, dialect);
                return;
            case CallExpr call:
                var callArgCount = call.Args.Count;
                for (var idx = 0; idx < callArgCount; idx++)
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
        var selectItems = q.SelectItems;
        var selectItemsCount = selectItems.Count;

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
                    if (idx >= selectItemsCount)
                    {
                        outOfRangeOrdinal ??= ord;
                        return expr;
                    }

                    var selectItem = selectItems[idx];
                    var (exprRaw, _) = _splitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                    usedOrdinal = true;
                    return _parseExpr(exprRaw);
                }

            case BinaryExpr b:
            {
                var leftCanBeOrdinal = IsOrdinalCandidateSide(b.Op, leftSide: true);
                var rightCanBeOrdinal = IsOrdinalCandidateSide(b.Op, leftSide: false);
                var left = RewriteHavingOrdinals(b.Left, q, ref usedOrdinal, leftCanBeOrdinal, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var right = RewriteHavingOrdinals(b.Right, q, ref usedOrdinal, rightCanBeOrdinal, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(left, b.Left) && ReferenceEquals(right, b.Right)
                    ? b
                    : b with { Left = left, Right = right };
            }
            case UnaryExpr u:
            {
                var rewritten = RewriteHavingOrdinals(u.Expr, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(rewritten, u.Expr) ? u : u with { Expr = rewritten };
            }
            case IsNullExpr isn:
            {
                var rewritten = RewriteHavingOrdinals(isn.Expr, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(rewritten, isn.Expr) ? isn : isn with { Expr = rewritten };
            }
            case LikeExpr like:
            {
                var left = RewriteHavingOrdinals(like.Left, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var pattern = RewriteHavingOrdinals(like.Pattern, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var escape = like.Escape is null
                    ? null
                    : RewriteHavingOrdinals(like.Escape, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(left, like.Left)
                    && ReferenceEquals(pattern, like.Pattern)
                    && (like.Escape is null ? escape is null : ReferenceEquals(escape, like.Escape))
                    ? like
                    : like with
                    {
                        Left = left,
                        Pattern = pattern,
                        Escape = escape
                    };
            }
            case InExpr i:
            {
                var left = RewriteHavingOrdinals(i.Left, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var items = RewriteSqlExprList(i.Items, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(left, i.Left) && ReferenceEquals(items, i.Items)
                    ? i
                    : i with { Left = left, Items = items };
            }
            case RowExpr r:
            {
                var items = RewriteSqlExprList(r.Items, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(items, r.Items) ? r : r with { Items = items };
            }
            case BetweenExpr bt:
            {
                var exprPart = RewriteHavingOrdinals(bt.Expr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var low = RewriteHavingOrdinals(bt.Low, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var high = RewriteHavingOrdinals(bt.High, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(exprPart, bt.Expr)
                    && ReferenceEquals(low, bt.Low)
                    && ReferenceEquals(high, bt.High)
                    ? bt
                    : bt with
                    {
                        Expr = exprPart,
                        Low = low,
                        High = high
                    };
            }
            case FunctionCallExpr fn:
            {
                var args = RewriteSqlExprList(fn.Args, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(args, fn.Args) ? fn : fn with { Args = args };
            }
            case CallExpr call:
            {
                var args = RewriteSqlExprList(call.Args, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(args, call.Args) ? call : call with { Args = args };
            }
            case CaseExpr c:
            {
                var baseExpr = c.BaseExpr is null
                    ? null
                    : RewriteHavingOrdinals(c.BaseExpr, q, ref usedOrdinal, true, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var whens = RewriteCaseWhens(c.Whens, q, ref usedOrdinal, allowOrdinalLiteral, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                var elseExpr = c.ElseExpr is null
                    ? null
                    : RewriteHavingOrdinals(c.ElseExpr, q, ref usedOrdinal, false, ref outOfRangeOrdinal, ref nonPositiveOrdinal);
                return ReferenceEquals(baseExpr, c.BaseExpr)
                    && ReferenceEquals(whens, c.Whens)
                    && ReferenceEquals(elseExpr, c.ElseExpr)
                    ? c
                    : c with
                    {
                        BaseExpr = baseExpr,
                        Whens = whens,
                        ElseExpr = elseExpr
                    };
            }
            default:
                return expr;
        }
    }

    private IReadOnlyList<SqlExpr> RewriteSqlExprList(
        IReadOnlyList<SqlExpr> items,
        SqlSelectQuery q,
        ref bool usedOrdinal,
        bool allowOrdinalLiteral,
        ref int? outOfRangeOrdinal,
        ref int? nonPositiveOrdinal)
    {
        var itemCount = items.Count;
        SqlExpr[]? rewritten = null;
        for (var idx = 0; idx < itemCount; idx++)
        {
            var originalItem = items[idx];
            var rewrittenItem = RewriteHavingOrdinals(
                originalItem,
                q,
                ref usedOrdinal,
                allowOrdinalLiteral,
                ref outOfRangeOrdinal,
                ref nonPositiveOrdinal);

            if (rewritten is null)
            {
                if (ReferenceEquals(rewrittenItem, originalItem))
                    continue;

                rewritten = new SqlExpr[itemCount];
                for (var copyIndex = 0; copyIndex < idx; copyIndex++)
                    rewritten[copyIndex] = items[copyIndex];
            }

            rewritten[idx] = rewrittenItem;
        }

        return rewritten ?? items;
    }

    private IReadOnlyList<CaseWhenThen> RewriteCaseWhens(
        IReadOnlyList<CaseWhenThen> whens,
        SqlSelectQuery q,
        ref bool usedOrdinal,
        bool allowOrdinalLiteral,
        ref int? outOfRangeOrdinal,
        ref int? nonPositiveOrdinal)
    {
        var whenCount = whens.Count;
        CaseWhenThen[]? rewritten = null;
        for (var idx = 0; idx < whenCount; idx++)
        {
            var originalWhen = whens[idx];
            var rewrittenWhenExpr = RewriteHavingOrdinals(
                originalWhen.When,
                q,
                ref usedOrdinal,
                allowOrdinalLiteral,
                ref outOfRangeOrdinal,
                ref nonPositiveOrdinal);
            var rewrittenThenExpr = RewriteHavingOrdinals(
                originalWhen.Then,
                q,
                ref usedOrdinal,
                false,
                ref outOfRangeOrdinal,
                ref nonPositiveOrdinal);

            if (rewritten is null)
            {
                if (ReferenceEquals(rewrittenWhenExpr, originalWhen.When)
                    && ReferenceEquals(rewrittenThenExpr, originalWhen.Then))
                {
                    continue;
                }

                rewritten = new CaseWhenThen[whenCount];
                for (var copyIndex = 0; copyIndex < idx; copyIndex++)
                    rewritten[copyIndex] = whens[copyIndex];
            }

            rewritten[idx] = ReferenceEquals(rewrittenWhenExpr, originalWhen.When)
                && ReferenceEquals(rewrittenThenExpr, originalWhen.Then)
                ? originalWhen
                : originalWhen with
                {
                    When = rewrittenWhenExpr,
                    Then = rewrittenThenExpr
                };
        }

        return rewritten ?? whens;
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
        var nameSpan = name.AsSpan();
        var dot = nameSpan.IndexOf('.');
        if (dot >= 0)
        {
            var qualifier = nameSpan[..dot].Trim();
            var col = nameSpan[(dot + 1)..].Trim();
            if (qualifier.IsEmpty || col.IsEmpty)
                return false;

            var qualifierName = qualifier.ToString();
            var columnName = col.ToString();

            if (sources.TryGetValue(qualifierName, out var source)
                && source.TryGetQualifiedColumnName(columnName, out var qualifiedColumnName))
            {
                var qualifiedName = qualifiedColumnName ?? string.Empty;
                if (qualifiedName.Length > 0
                    && row.TryGetValue(qualifiedName, out _))
                {
                    return true;
                }
            }

            if (TryHasQualifiedField(row, sources, columnName))
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
        if (sources.Count == 0)
            return false;

        var fields = row.Fields;

        if (row.TryGetSingleSource(out var singleSource)
            && TryHasQualifiedFieldFromSource(singleSource!, fields, columnName))
        {
            return true;
        }

        foreach (var source in sources.Values)
        {
            if (TryHasQualifiedFieldFromSource(source, fields, columnName))
                return true;
        }

        return false;
    }

    private static bool TryHasQualifiedFieldFromSource(
        Source source,
        Dictionary<string, object?> fields,
        string columnName)
    {
        if (!source.TryGetQualifiedColumnName(columnName, out var qualifiedName)
            || string.IsNullOrWhiteSpace(qualifiedName))
            return false;

        return fields.ContainsKey(qualifiedName!);
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
                return HasAggregateIdentifierOrTemporalReferenceInBinary(binary, dialect);

            case UnaryExpr unary:
                return HasAggregateIdentifierOrTemporalReference(unary.Expr, dialect);

            case LikeExpr like:
                return HasAggregateIdentifierOrTemporalReferenceInLike(like, dialect);

            case InExpr inExpr:
                return HasAggregateIdentifierOrTemporalReferenceInIn(inExpr, dialect);

            case IsNullExpr isNull:
                return HasAggregateIdentifierOrTemporalReference(isNull.Expr, dialect);

            case QuantifiedComparisonExpr quantified:
                return HasAggregateIdentifierOrTemporalReference(quantified.Left, dialect);

            case RowExpr row:
                return HasAggregateIdentifierOrTemporalReferenceInRow(row, dialect);

            case BetweenExpr between:
                return HasAggregateIdentifierOrTemporalReferenceInBetween(between, dialect);

            case CaseExpr @case:
                return HasAggregateIdentifierOrTemporalReferenceInCase(@case, dialect);

            case JsonAccessExpr jsonAccess:
                return HasAggregateIdentifierOrTemporalReferenceInJsonAccess(jsonAccess, dialect);

            default:
                return false;
        }
    }

    private static bool HasAggregateIdentifierOrTemporalReferenceInBinary(BinaryExpr binary, ISqlDialect dialect)
        => HasAggregateIdentifierOrTemporalReference(binary.Left, dialect)
           || HasAggregateIdentifierOrTemporalReference(binary.Right, dialect);

    private static bool HasAggregateIdentifierOrTemporalReferenceInLike(LikeExpr like, ISqlDialect dialect)
        => HasAggregateIdentifierOrTemporalReference(like.Left, dialect)
           || HasAggregateIdentifierOrTemporalReference(like.Pattern, dialect)
           || (like.Escape is not null && HasAggregateIdentifierOrTemporalReference(like.Escape, dialect));

    private static bool HasAggregateIdentifierOrTemporalReferenceInIn(InExpr inExpr, ISqlDialect dialect)
    {
        if (HasAggregateIdentifierOrTemporalReference(inExpr.Left, dialect))
            return true;

        for (var idx = 0; idx < inExpr.Items.Count; idx++)
        {
            if (HasAggregateIdentifierOrTemporalReference(inExpr.Items[idx], dialect))
                return true;
        }

        return false;
    }

    private static bool HasAggregateIdentifierOrTemporalReferenceInRow(RowExpr row, ISqlDialect dialect)
    {
        for (var idx = 0; idx < row.Items.Count; idx++)
        {
            if (HasAggregateIdentifierOrTemporalReference(row.Items[idx], dialect))
                return true;
        }

        return false;
    }

    private static bool HasAggregateIdentifierOrTemporalReferenceInBetween(BetweenExpr between, ISqlDialect dialect)
        => HasAggregateIdentifierOrTemporalReference(between.Expr, dialect)
           || HasAggregateIdentifierOrTemporalReference(between.Low, dialect)
           || HasAggregateIdentifierOrTemporalReference(between.High, dialect);

    private static bool HasAggregateIdentifierOrTemporalReferenceInCase(CaseExpr @case, ISqlDialect dialect)
    {
        if (@case.BaseExpr is not null && HasAggregateIdentifierOrTemporalReference(@case.BaseExpr, dialect))
            return true;

        var whens = @case.Whens;
        var whenCount = whens.Count;
        for (var idx = 0; idx < whenCount; idx++)
        {
            var whenThen = whens[idx];
            if (HasAggregateIdentifierOrTemporalReference(whenThen.When, dialect)
                || HasAggregateIdentifierOrTemporalReference(whenThen.Then, dialect))
            {
                return true;
            }
        }

        return @case.ElseExpr is not null && HasAggregateIdentifierOrTemporalReference(@case.ElseExpr, dialect);
    }

    private static bool HasAggregateIdentifierOrTemporalReferenceInJsonAccess(JsonAccessExpr jsonAccess, ISqlDialect dialect)
        => HasAggregateIdentifierOrTemporalReference(jsonAccess.Target, dialect)
           || HasAggregateIdentifierOrTemporalReference(jsonAccess.Path, dialect);

    private static bool HasAggregateFunctionCall(
        string name,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect)
    {
        if (AggregateFunctionCatalog.Contains(name))
            return true;

        var argCount = args.Count;
        if (argCount == 0)
            return dialect.AllowsTemporalCall(name);

        for (var idx = 0; idx < argCount; idx++)
        {
            if (HasAggregateIdentifierOrTemporalReference(args[idx], dialect))
                return true;
        }

        return false;
    }
}
