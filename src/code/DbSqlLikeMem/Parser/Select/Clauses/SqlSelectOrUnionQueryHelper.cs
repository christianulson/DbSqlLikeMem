namespace DbSqlLikeMem;

internal static class SqlSelectOrUnionQueryHelper
{
    internal static SqlQueryBase ParseSelectOrUnion(
        this SqlQueryParserContext ctx,
        Func<bool, bool, SqlSelectQuery> parseSelectQuery)
    {
        var first = parseSelectQuery(true, false);

        if (!ctx.IsWord(SqlConst.UNION))
        {
            var orderBy = SqlOrderByHelper.TryParseOrderBy(
                ctx,
                boundary => boundary
                    ? ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.ROWS, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON)
                    : ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.ROWS, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING));
            var rowLimit = ctx.TryParseRowLimitTail(orderBy.Count > 0);
            var forJson = ctx.TryParseForJsonClause();
            rowLimit ??= first.RowLimit;
            ctx.TryConsumeQueryHintOption();
            ctx.ExpectEndOrUnionBoundary();

            return first with
            {
                OrderBy = orderBy,
                RowLimit = rowLimit,
                ForJson = forJson
            };
        }

        var parts = new List<SqlSelectQuery> { first };
        var allFlags = new List<bool>();

        while (ctx.IsWord(SqlConst.UNION))
        {
            ctx.Consume();
            var isAll = false;
            if (ctx.IsWord(SqlConst.ALL))
            {
                ctx.Consume();
                isAll = true;
            }

            allFlags.Add(isAll);
            parts.Add(parseSelectQuery(false, false));
        }

        var unionOrderBy = SqlOrderByHelper.TryParseOrderBy(
            ctx,
            boundary => boundary
                ? ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.ROWS, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON)
                : ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.ROWS, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING));
        var unionRowLimit = ctx.TryParseRowLimitTail(unionOrderBy.Count > 0);
        ctx.TryConsumeQueryHintOption();
        ctx.ExpectEndOrUnionBoundary();

        return new SqlUnionQuery(parts, allFlags, unionOrderBy, unionRowLimit);
    }

    internal static SqlQueryBase Parse(
        this SqlQueryParserContext ctx,
        Func<bool, bool, SqlSelectQuery> parseSelectQuery,
        Func<List<SqlOrderByItem>> tryParseOrderBy,
        Func<bool, SqlRowLimit?> tryParseRowLimitTail,
        Func<SqlForJsonClause?> tryParseForJsonClause,
        Action tryConsumeQueryHintOption,
        Action expectEndOrUnionBoundary)
    {
        var first = parseSelectQuery(true, false);

        if (!ctx.IsWord(SqlConst.UNION))
        {
            var orderBy = tryParseOrderBy();
            var rowLimit = tryParseRowLimitTail(orderBy.Count > 0);
            var forJson = tryParseForJsonClause();
            rowLimit ??= first.RowLimit;
            tryConsumeQueryHintOption();
            expectEndOrUnionBoundary();

            return first with
            {
                OrderBy = orderBy,
                RowLimit = rowLimit,
                ForJson = forJson
            };
        }

        var parts = new List<SqlSelectQuery> { first };
        var allFlags = new List<bool>();

        while (ctx.IsWord(SqlConst.UNION))
        {
            ctx.Consume();
            var isAll = false;
            if (ctx.IsWord(SqlConst.ALL))
            {
                ctx.Consume();
                isAll = true;
            }

            allFlags.Add(isAll);
            parts.Add(parseSelectQuery(false, false));
        }

        var unionOrderBy = tryParseOrderBy();
        var unionRowLimit = tryParseRowLimitTail(unionOrderBy.Count > 0);
        tryConsumeQueryHintOption();
        expectEndOrUnionBoundary();

        return new SqlUnionQuery(parts, allFlags, unionOrderBy, unionRowLimit);
    }
}
