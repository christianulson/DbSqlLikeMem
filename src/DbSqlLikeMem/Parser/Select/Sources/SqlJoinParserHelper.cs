namespace DbSqlLikeMem;

internal static class SqlJoinParserHelper
{
    internal static SqlJoin ParseJoin(
        this SqlQueryParserContext ctx,
        Func<SqlTableSource> parseTableSource)
    {
        if (ctx.IsWord(SqlConst.CROSS) && ctx.IsWord(1, SqlConst.APPLY))
        {
            if (!ctx.Dialect.SupportsApplyClause)
                throw SqlApplyClauseHelper.CreateApplyUnsupportedException(ctx, "CROSS APPLY", 2);

            ctx.Consume(); // CROSS
            ctx.Consume(); // APPLY

            var tableCross = parseTableSource();
            SqlApplyClauseHelper.ValidateApplySource(tableCross, "CROSS APPLY");
            return new SqlJoin(SqlJoinType.CrossApply, tableCross, new LiteralExpr(true));
        }

        if (ctx.IsWord(SqlConst.OUTER) && ctx.IsWord(1, SqlConst.APPLY))
        {
            if (!ctx.Dialect.SupportsApplyClause)
                throw SqlApplyClauseHelper.CreateApplyUnsupportedException(ctx, "OUTER APPLY", 2);

            ctx.Consume(); // OUTER
            ctx.Consume(); // APPLY

            var tableOuter = parseTableSource();
            SqlApplyClauseHelper.ValidateApplySource(tableOuter, "OUTER APPLY");
            return new SqlJoin(SqlJoinType.OuterApply, tableOuter, new LiteralExpr(true));
        }

        var type = SqlJoinType.Inner;
        if (ctx.IsWord(SqlConst.LEFT)) { ctx.Consume(); type = SqlJoinType.Left; }
        else if (ctx.IsWord(SqlConst.RIGHT)) { ctx.Consume(); type = SqlJoinType.Right; }
        else if (ctx.IsWord(SqlConst.CROSS)) { ctx.Consume(); type = SqlJoinType.Cross; }
        else if (ctx.IsWord(SqlConst.INNER)) { ctx.Consume(); type = SqlJoinType.Inner; }
        if (ctx.IsWord(SqlConst.OUTER))
            ctx.Consume();
        ctx.ExpectWord(SqlConst.JOIN);

        var isLateral = false;
        if (ctx.IsWord("LATERAL"))
        {
            ctx.Consume();
            isLateral = true;
        }

        var table = parseTableSource();
        if (isLateral)
            table = table with { IsLateral = true };

        SqlExpr onExpr = new LiteralExpr(true);
        if (type != SqlJoinType.Cross)
        {
            ctx.ExpectWord(SqlConst.ON);
            var txt = ctx.ReadClauseTextUntilTopLevelStop(
                SqlConst.JOIN,
                SqlConst.LEFT,
                SqlConst.RIGHT,
                SqlConst.INNER,
                SqlConst.CROSS,
                SqlConst.OUTER,
                SqlConst.APPLY,
                SqlConst.WHERE,
                SqlConst.GROUP,
                SqlConst.ORDER,
                SqlConst.LIMIT,
                SqlConst.OFFSET,
                SqlConst.FETCH,
                SqlConst.UNION);
            onExpr = ctx.ParseWhere(txt);
        }

        return new SqlJoin(type, table, onExpr);
    }

    internal static SqlJoin ParseJoin(
        this SqlQueryParserContext ctx,
        Func<SqlTableSource> parseTableSource,
        Func<string, NotSupportedException> createApplyUnsupportedException,
        Action<SqlTableSource, string> validateApplySource)
    {
        if (ctx.IsWord(SqlConst.CROSS) && ctx.IsWord(1, SqlConst.APPLY))
        {
            if (!ctx.Dialect.SupportsApplyClause)
                throw ctx.CreateApplyUnsupportedException("CROSS APPLY", 2);

            ctx.Consume(); // CROSS
            ctx.Consume(); // APPLY

            var tableCross = parseTableSource();
            validateApplySource(tableCross, "CROSS APPLY");
            return new SqlJoin(SqlJoinType.CrossApply, tableCross, new LiteralExpr(true));
        }

        if (ctx.IsWord(SqlConst.OUTER) && ctx.IsWord(1, SqlConst.APPLY))
        {
            if (!ctx.Dialect.SupportsApplyClause)
                throw ctx.CreateApplyUnsupportedException("OUTER APPLY", 2);

            ctx.Consume(); // OUTER
            ctx.Consume(); // APPLY

            var tableOuter = parseTableSource();
            validateApplySource(tableOuter, "OUTER APPLY");
            return new SqlJoin(SqlJoinType.OuterApply, tableOuter, new LiteralExpr(true));
        }

        var type = SqlJoinType.Inner;
        if (ctx.IsWord(SqlConst.LEFT)) { ctx.Consume(); type = SqlJoinType.Left; }
        else if (ctx.IsWord(SqlConst.RIGHT)) { ctx.Consume(); type = SqlJoinType.Right; }
        else if (ctx.IsWord(SqlConst.CROSS)) { ctx.Consume(); type = SqlJoinType.Cross; }
        else if (ctx.IsWord(SqlConst.INNER)) { ctx.Consume(); type = SqlJoinType.Inner; }
        if (ctx.IsWord(SqlConst.OUTER)) ctx.Consume();
        ExpectWord(ctx, SqlConst.JOIN);

        var isLateral = false;
        if (ctx.IsWord("LATERAL"))
        {
            ctx.Consume();
            isLateral = true;
        }

        var table = parseTableSource();
        if (isLateral)
            table = table with { IsLateral = true };

        SqlExpr onExpr = new LiteralExpr(true);
        if (type != SqlJoinType.Cross)
        {
            ExpectWord(ctx, SqlConst.ON);
            var txt = ReadClauseTextUntilTopLevelStop(
                ctx,
                SqlConst.JOIN,
                SqlConst.LEFT,
                SqlConst.RIGHT,
                SqlConst.INNER,
                SqlConst.CROSS,
                SqlConst.OUTER,
                SqlConst.APPLY,
                SqlConst.WHERE,
                SqlConst.GROUP,
                SqlConst.ORDER,
                SqlConst.LIMIT,
                SqlConst.OFFSET,
                SqlConst.FETCH,
                SqlConst.UNION);
            onExpr = ctx.ParseWhere(txt);
        }

        return new SqlJoin(type, table, onExpr);
    }

    private static void ExpectWord(
        SqlQueryParserContext ctx,
        string word)
    {
        if (!ctx.IsWord(word))
            throw new InvalidOperationException($"Expected '{word}'.");

        ctx.Consume();
    }

    private static string ReadClauseTextUntilTopLevelStop(
        this SqlQueryParserContext ctx,
        params string[] stopWords)
    {
        var buf = new List<SqlToken>();
        var depth = 0;
        while (true)
        {
            var t = ctx.Peek(0);
            if (t.Kind == SqlTokenKind.EndOfFile)
                break;

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                depth++;
            else if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
                depth--;

            if (depth == 0 && t.Kind == SqlTokenKind.Symbol && t.Text == ";")
                break;

            if (depth == 0 && stopWords.Any(sw => SqlQueryParserContext.IsWord(t, sw)))
                break;

            buf.Add(ctx.Consume());
        }

        return TokensToSql(buf);
    }

    private static string TokensToSql(List<SqlToken> toks)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;

        foreach (var t in toks)
        {
            if (prev is not null && NeedsSpace(prev.Value, t))
                sb.Append(' ');

            sb.Append(t.Text);
            prev = t;
        }

        return sb.ToString();
    }

    private static bool NeedsSpace(SqlToken prev, SqlToken cur)
        => !(prev.Kind == SqlTokenKind.Symbol || cur.Kind == SqlTokenKind.Symbol);
}
