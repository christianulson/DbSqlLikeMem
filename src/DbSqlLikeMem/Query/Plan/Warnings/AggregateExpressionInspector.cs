namespace DbSqlLikeMem;

internal static class AggregateExpressionInspector
{
    private static readonly Regex AggregateExpressionRegex = new(
        $@"\b(?:{AggregateFunctionCatalog.GetRegexAlternation()})\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    internal static bool LooksLikeAggregateExpression(string exprRaw)
        => AggregateExpressionRegex.IsMatch(RemoveSubqueryBodies(exprRaw));

    internal static string RemoveSubqueryBodies(string exprRaw)
    {
        if (string.IsNullOrWhiteSpace(exprRaw))
            return string.Empty;

        var sb = new StringBuilder(exprRaw.Length);
        for (var i = 0; i < exprRaw.Length; i++)
        {
            var ch = exprRaw[i];
            if (ch is '\'' or '"')
            {
                AppendQuoted(exprRaw, sb, ref i, ch);
                continue;
            }

            if (ch == '(' && TrySkipSubquery(exprRaw, ref i))
            {
                sb.Append("(SUBQUERY)");
                continue;
            }

            sb.Append(ch);
        }

        return sb.ToString();
    }

    internal static bool WalkHasAggregate(SqlExpr expr) => expr switch
    {
        CallExpr call => HasAggregateFunctionCall(call.Name, call.Args),
        FunctionCallExpr function => HasAggregateFunctionCall(function.Name, function.Args),
        BinaryExpr binary => WalkHasAggregate(binary.Left) || WalkHasAggregate(binary.Right),
        UnaryExpr unary => WalkHasAggregate(unary.Expr),
        LikeExpr like => WalkHasAggregateLike(like),
        InExpr inExpression => WalkHasAggregateIn(inExpression),
        IsNullExpr isNull => WalkHasAggregate(isNull.Expr),
        QuantifiedComparisonExpr quantified => WalkHasAggregate(quantified.Left),
        ExistsExpr => false,
        SubqueryExpr => false,
        RowExpr row => WalkHasAggregateSequence(row.Items),
        _ => false
    };

    private static bool HasAggregateFunctionCall(string name, IReadOnlyList<SqlExpr> args)
        => AggregateFunctionCatalog.Contains(name) || WalkHasAggregateSequence(args);

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

    private static void AppendQuoted(string text, StringBuilder sb, ref int index, char quote)
    {
        sb.Append(text[index]);
        index++;

        while (index < text.Length)
        {
            sb.Append(text[index]);
            if (text[index] == quote)
                return;
            index++;
        }
    }

    private static bool TrySkipSubquery(string text, ref int index)
    {
        var cursor = index + 1;
        while (cursor < text.Length && char.IsWhiteSpace(text[cursor]))
            cursor++;

        if (!StartsWithKeyword(text, cursor, SqlConst.SELECT) && !StartsWithKeyword(text, cursor, SqlConst.WITH))
            return false;

        var depth = 1;
        index++;
        while (index < text.Length && depth > 0)
        {
            var ch = text[index];
            if (ch is '\'' or '"')
            {
                SkipQuoted(text, ref index, ch);
                continue;
            }

            if (ch == '(')
                depth++;
            else if (ch == ')')
                depth--;

            index++;
        }

        index--;
        return true;
    }

    private static void SkipQuoted(string text, ref int index, char quote)
    {
        index++;
        while (index < text.Length)
        {
            if (text[index] == quote)
                return;
            index++;
        }
    }

    private static bool StartsWithKeyword(string text, int start, string keyword)
    {
        if (start + keyword.Length > text.Length)
            return false;

        if (!text.AsSpan(start, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
            return false;

        var end = start + keyword.Length;
        return end >= text.Length || !(char.IsLetterOrDigit(text[end]) || text[end] == '_');
    }
}
