namespace DbSqlLikeMem;

internal static class SqlPaginationHelper
{
    internal static SqlRowLimit? TryParseRowLimitTail(
        this SqlQueryParserContext ctx,
        bool hasOrderBy)
    {
        var dialect = ctx.Dialect;

        // MySQL/Postgres: LIMIT ...
        if (ctx.IsWord(SqlConst.LIMIT))
        {
            if (!dialect.SupportsLimitOffset && !dialect.AllowsParserLimitOffsetCompatibility)
                throw SqlUnsupported.NotSupportedPagination(dialect, SqlConst.LIMIT);

            ctx.Consume();
            var a = ctx.ExpectRowLimitExpr();
            if (ctx.IsSymbol(","))
            {
                ctx.Consume();
                return new SqlLimitOffset(Count: ctx.ExpectRowLimitExpr(), Offset: a);
            }
            if (ctx.IsWord(SqlConst.OFFSET))
            {
                ctx.Consume();
                return new SqlLimitOffset(Count: a, Offset: ctx.ExpectRowLimitExpr());
            }
            return new SqlLimitOffset(Count: a, Offset: null);
        }

        // Oracle/SQL Server/Postgres: OFFSET ... FETCH ...
        if (ctx.IsWord(SqlConst.OFFSET))
        {
            if (!dialect.SupportsOffsetFetch)
                throw SqlUnsupported.NotSupportedPagination(dialect, SqlConst.OFFSET_FETCH);
            if (dialect.RequiresOrderByForOffsetFetch && !hasOrderBy)
                throw SqlUnsupported.NotSupportedOffsetFetchRequiresOrderBy(dialect);

            ctx.Consume();
            var offset = ctx.ExpectRowLimitExpr();
            if (ctx.IsWord(SqlConst.ROW) || ctx.IsWord(SqlConst.ROWS))
                ctx.Consume();

            if (ctx.IsWord(SqlConst.FETCH))
            {
                ctx.Consume();
                if (ctx.IsWord(SqlConst.NEXT) || ctx.IsWord(SqlConst.FIRST))
                    ctx.Consume();

                var count = ctx.ExpectRowLimitExpr();

                if (ctx.IsWord(SqlConst.ROW) || ctx.IsWord(SqlConst.ROWS))
                    ctx.Consume();

                if (ctx.IsWord(SqlConst.ONLY))
                    ctx.Consume();

                return new SqlLimitOffset(Count: count, Offset: offset);
            }

            return new SqlLimitOffset(Count: new LiteralExpr(int.MaxValue), Offset: offset);
        }

        // Oracle/Postgres: FETCH FIRST n ROWS ONLY
        if (ctx.IsWord(SqlConst.FETCH))
        {
            if (!dialect.SupportsFetchFirst)
                throw SqlUnsupported.NotSupportedPagination(dialect, SqlConst.FETCH_FIRST_NEXT);

            ctx.Consume();
            if (ctx.IsWord(SqlConst.NEXT) || ctx.IsWord(SqlConst.FIRST))
                ctx.Consume();

            var count = ctx.ExpectRowLimitExpr();

            if (ctx.IsWord(SqlConst.ROW) || ctx.IsWord(SqlConst.ROWS))
                ctx.Consume();

            if (ctx.IsWord(SqlConst.ONLY))
                ctx.Consume();

            return new SqlLimitOffset(Count: count, Offset: null);
        }

        return null;
    }

    internal static SqlExpr ExpectRowLimitExpr(
        this SqlQueryParserContext ctx)
    {
        var t = ctx.Peek();
        if (t.Kind == SqlTokenKind.Number)
        {
            ctx.Consume();
            return new LiteralExpr(int.Parse(t.Text, CultureInfo.InvariantCulture));
        }

        if (t.Kind == SqlTokenKind.Parameter)
        {
            ctx.Consume();
            return new ParameterExpr(t.Text);
        }

        throw new InvalidOperationException($"Esperava número inteiro ou parâmetro para limite de linhas, veio {t.Kind} '{t.Text}'.");
    }
}
