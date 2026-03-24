namespace DbSqlLikeMem;

internal static class SqlCteParserHelper
{
    internal static List<SqlCte> TryParseCtes(
        SqlQueryParserContext ctx,
        Func<string, SqlQueryBase> parseQuery)
    {
        var list = new List<SqlCte>();
        if (!ctx.IsWord( SqlConst.WITH))
            return list;

        ctx.Consume();
        if (!ctx.Dialect.SupportsWithCte)
            throw SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.WITH_CTE);

        if (ctx.IsWord( SqlConst.RECURSIVE))
        {
            if (!ctx.Dialect.SupportsWithRecursive)
                throw SqlUnsupported.ForWithRecursive(ctx.Dialect);
            ctx.Consume();
        }

        while (true)
        {
            var name = ctx.ExpectIdentifier();
            if (ctx.IsSymbol( "("))
            {
                ctx.Consume();
                while (!ctx.IsSymbol( ")"))
                    ctx.Consume();
                ctx.Consume();
            }

            if (!ctx.IsWord( SqlConst.AS))
                throw new InvalidOperationException("CTE requires AS.");

            ctx.Consume();
            if (ctx.IsWord( SqlConst.NOT) && ctx.IsWord(1, SqlConst.MATERIALIZED))
            {
                if (!ctx.Dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(ctx.Dialect, "WITH ... AS NOT MATERIALIZED");
                ctx.Consume();
                ctx.Consume();
            }
            else if (ctx.IsWord( SqlConst.MATERIALIZED))
            {
                if (!ctx.Dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(ctx.Dialect, "WITH ... AS MATERIALIZED");
                ctx.Consume();
            }

            var innerSql = ctx.ReadBalancedParenRawTokens();
            var q = parseQuery(innerSql);
            if (q is SqlSelectQuery or SqlUnionQuery)
                list.Add(new SqlCte(name, q));

            if (ctx.IsSymbol( ","))
            {
                ctx.Consume();
                continue;
            }

            break;
        }

        return list;
    }

    internal static List<SqlCte> TryParseCtes(
        ISqlDialect dialect,
        Func<int, SqlToken> peek,
        Action consume,
        Func<string> expectIdentifier,
        Func<SqlToken, string, bool> isWord,
        Func<SqlToken, string, bool> isSymbol,
        Func<string> readBalancedParenRawTokens,
        Func<string, SqlQueryBase> parseQuery)
    {
        var list = new List<SqlCte>();
        if (!isWord(peek(0), SqlConst.WITH))
            return list;

        consume();
        if (!dialect.SupportsWithCte)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.WITH_CTE);

        if (isWord(peek(0), SqlConst.RECURSIVE))
        {
            if (!dialect.SupportsWithRecursive)
                throw SqlUnsupported.ForWithRecursive(dialect);
            consume();
        }

        while (true)
        {
            var name = expectIdentifier();
            if (isSymbol(peek(0), "("))
            {
                consume();
                while (!isSymbol(peek(0), ")"))
                    consume();
                consume();
            }

            if (!isWord(peek(0), SqlConst.AS))
                throw new InvalidOperationException("CTE requires AS.");

            consume();
            if (isWord(peek(0), SqlConst.NOT) && isWord(peek(1), SqlConst.MATERIALIZED))
            {
                if (!dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(dialect, "WITH ... AS NOT MATERIALIZED");
                consume();
                consume();
            }
            else if (isWord(peek(0), SqlConst.MATERIALIZED))
            {
                if (!dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(dialect, "WITH ... AS MATERIALIZED");
                consume();
            }

            var innerSql = readBalancedParenRawTokens();
            var q = parseQuery(innerSql);
            if (q is SqlSelectQuery or SqlUnionQuery)
                list.Add(new SqlCte(name, q));

            if (isSymbol(peek(0), ","))
            {
                consume();
                continue;
            }

            break;
        }

        return list;
    }
}
