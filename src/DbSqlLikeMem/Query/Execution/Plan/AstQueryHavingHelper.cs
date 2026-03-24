using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryHavingHelper(
    Func<ISqlDialect> getDialect,
    Func<string, SqlExpr> parseExpr,
    Func<SqlExpr, bool> walkHasAggregate,
    Func<string, string?, (string expr, string? alias)> splitTrailingAsAlias)
{
    private readonly Func<ISqlDialect> _getDialect = getDialect;
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr;
    private readonly Func<SqlExpr, bool> _walkHasAggregate = walkHasAggregate;
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
        var hasAggregate = _walkHasAggregate(rewritten);
        var hasIdentifier = EnumerateIdentifiers(rewritten).Any();
        var hasTemporalReference = WalkHasTemporalHavingReference(rewritten, dialect);
        if (hasAggregate || hasIdentifier || hasTemporalReference)
            return rewritten;

        throw new InvalidOperationException(
            "invalid: HAVING must reference grouped columns, projected aliases, aggregates, or valid ordinals");
    }

    internal void EnsureHavingIdentifiersAreBound(SqlExpr expr, EvalRow row, ISqlDialect dialect)
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

    private static bool WalkHasTemporalHavingReference(SqlExpr expr, ISqlDialect dialect)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return dialect.AllowsTemporalIdentifier(id.Name);

            case FunctionCallExpr fn:
                if (fn.Args.Count == 0)
                {
                    if (dialect.AllowsTemporalCall(fn.Name))
                        return true;
                }

                foreach (var arg in fn.Args)
                {
                    if (WalkHasTemporalHavingReference(arg, dialect))
                        return true;
                }

                return false;

            case CallExpr call:
                if (call.Args.Count == 0)
                {
                    if (dialect.AllowsTemporalCall(call.Name))
                        return true;
                }

                foreach (var arg in call.Args)
                {
                    if (WalkHasTemporalHavingReference(arg, dialect))
                        return true;
                }

                return false;

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

                foreach (var item in i.Items)
                {
                    if (WalkHasTemporalHavingReference(item, dialect))
                        return true;
                }

                return false;

            case RowExpr r:
                foreach (var item in r.Items)
                {
                    if (WalkHasTemporalHavingReference(item, dialect))
                        return true;
                }

                return false;

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

    private static bool IsHavingTemporalIdentifier(string name, ISqlDialect dialect)
        => dialect.AllowsTemporalIdentifier(name);

    private static bool IsIdentifierBound(EvalRow row, string name)
    {
        var dot = name.IndexOf('.');
        if (dot >= 0)
        {
            var qualifier = name[..dot].Trim();
            var col = name[(dot + 1)..].Trim();
            if (qualifier.Length == 0 || col.Length == 0)
                return false;

            if (row.Sources.TryGetValue(qualifier, out var source)
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

    private static bool TryHasQualifiedField(EvalRow row, string columnName)
    {
        if (row.TryGetSingleSource(out var singleSource)
            && TryHasQualifiedFieldFromSource(singleSource!, row, columnName))
        {
            return true;
        }

        foreach (var source in row.Sources.Values)
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

    private IEnumerable<string> EnumerateIdentifiers(SqlExpr expr)
    {
        var identifiers = new List<string>();
        AppendIdentifiers(expr, identifiers);
        return identifiers;
    }

    private void AppendIdentifiers(SqlExpr expr, List<string> identifiers)
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

    private void AppendBinaryIdentifiers(SqlExpr left, SqlExpr right, List<string> identifiers)
    {
        AppendIdentifiers(left, identifiers);
        AppendIdentifiers(right, identifiers);
    }

    private void AppendLikeIdentifiers(LikeExpr like, List<string> identifiers)
    {
        AppendIdentifiers(like.Left, identifiers);
        AppendIdentifiers(like.Pattern, identifiers);
        if (like.Escape is not null)
            AppendIdentifiers(like.Escape, identifiers);
    }

    private void AppendInIdentifiers(InExpr inExpr, List<string> identifiers)
    {
        AppendIdentifiers(inExpr.Left, identifiers);
        AppendIdentifierSequence(inExpr.Items, identifiers);
    }

    private void AppendCaseIdentifiers(CaseExpr @case, List<string> identifiers)
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

    private void AppendBetweenIdentifiers(BetweenExpr between, List<string> identifiers)
    {
        AppendIdentifiers(between.Expr, identifiers);
        AppendIdentifiers(between.Low, identifiers);
        AppendIdentifiers(between.High, identifiers);
    }

    private void AppendIdentifierSequence(IEnumerable<SqlExpr> expressions, List<string> identifiers)
    {
        foreach (var expression in expressions)
            AppendIdentifiers(expression, identifiers);
    }
}
